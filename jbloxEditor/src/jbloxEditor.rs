use clap::{Arg, Command};
use crossterm::event::KeyEventKind;
use crossterm::style::{Attribute, SetAttribute, SetForegroundColor};
use crossterm::style::{Color, Print, ResetColor, SetBackgroundColor};
use crossterm::terminal::window_size;
use crossterm::{
    cursor::{Hide, MoveTo, Show},
    event::{self, Event, KeyCode, KeyEvent, KeyModifiers},
    execute,
    terminal::{
        Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode,
        enable_raw_mode,
    },
};
use memmap2::MmapOptions;
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::{self, Write, stdout};
use std::path::PathBuf;
use std::io::Stdout;

use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::process::exit;

use memmap2::{MmapMut};

use chrono::Local; // <-- add this at top of file
use std::io::{ErrorKind};


// Represents the cursor's position within the visible viewport
use crossterm::{
    ExecutableCommand, cursor, queue,
    style::{self},
    terminal::{self},
};


struct Cursor {
    row: usize,
    col: usize,
    view_width: usize,
    char_size: usize, // New field
}


impl Cursor {
    fn new(view_width: usize, char_size: usize) -> Self {
        Cursor {
            row: 0,
            col: 0,
            view_width,
            char_size,
        }
    }

    fn move_up(&mut self, stdout: &mut Stdout) -> std::io::Result<()> {
        if self.row > 0 {
            self.row -= 1;
        }
        self.update_terminal_cursor(stdout)
    }

    fn move_down(&mut self, stdout: &mut Stdout, max_rows: usize) -> std::io::Result<()> {
        if self.row + 1 < max_rows {
            self.row += 1;
        }
        self.update_terminal_cursor(stdout)
    }

    fn move_left(&mut self, stdout: &mut Stdout) -> std::io::Result<()> {
        if self.col > 0 {
            self.col -= 1;
        }
        self.update_terminal_cursor(stdout)
    }

    fn move_right(&mut self, stdout: &mut Stdout, line_len: usize) -> std::io::Result<()> {
        if (self.col + 1) * self.char_size < line_len {
            self.col += 1;
        }
        self.update_terminal_cursor(stdout)
    }

    fn jump_start(&mut self, stdout: &mut Stdout) -> std::io::Result<()> {
        self.col = 0;
        self.update_terminal_cursor(stdout)
    }

    fn jump_end(&mut self, stdout: &mut Stdout, line_len: usize) -> std::io::Result<()> {
        let char_count = line_len / self.char_size;
        if char_count > self.view_width {
            self.col = self.view_width - 1;
        } else if char_count > 0 {
            self.col = char_count - 1;
        } else {
            self.col = 0;
        }
        self.update_terminal_cursor(stdout)
    }

    fn update_terminal_cursor(&self, stdout: &mut Stdout) -> std::io::Result<()> {
        execute!(stdout, MoveTo(self.col as u16, self.row as u16))
    }
}


/// Places the visible terminal cursor at the logical Cursor position
fn place_cursor(
    cursor: &mut Cursor,
    ui_line_map: &HashMap<usize, usize>,
    line_ptr_map: &HashMap<usize, usize>,
    line_len_map: &HashMap<usize, usize>,
) -> io::Result<()> {
    let mut stdout = stdout();

    // Get the actual file line number from the UI mapping
    let file_line_number = ui_line_map[&cursor.row];

    // Get start offset and length from hashmaps
    let line_start = line_ptr_map[&file_line_number];
    let line_len = line_len_map[&file_line_number];

    // Clamp column position if line is too short
    if cursor.col >= line_len {
        cursor.col = line_len.saturating_sub(1);
    }

    // Place cursor in terminal
    execute!(
        stdout,
        crossterm::cursor::MoveTo(
            (cursor.col + 10) as u16, // +10 for line number
            (cursor.row + 2) as u16   // +2 for header
        )
    )
}

fn detect_encoding(mmap: &[u8]) -> &'static str {
    if mmap.starts_with(&[0xEF, 0xBB, 0xBF]) {
        "utf-8"      // UTF-8 BOM
    } else if mmap.starts_with(&[0xFF, 0xFE]) {
        "utf-16le"   // UTF-16 Little Endian
    } else if mmap.starts_with(&[0xFE, 0xFF]) {
        "utf-16be"   // UTF-16 Big Endian
    } else if mmap.starts_with(&[0xFF, 0xFE, 0x00, 0x00]) {
        "utf-32le"   // UTF-32 Little Endian
    } else if mmap.starts_with(&[0x00, 0x00, 0xFE, 0xFF]) {
        "utf-32be"   // UTF-32 Big Endian
    } else {
        "utf-8"      // Fallback: assume UTF-8
    }
}
/// Checks if a character is in the protected_chars list
fn is_protected_char(c: char, protected_chars: &Vec<char>) -> bool {
    protected_chars.contains(&c)
}

/// Determines if a character is editable based on rules
fn is_editable(line: &str, col: usize, separator: char, is_jblox: bool) -> bool {
    if !is_jblox {
        return true; // Non-jblox files → fully editable
    }

    // Find separator position
    if let Some(sep_pos) = line.find(separator) {
        if col < sep_pos {
            // Before separator → only first 2 chars editable
            col < 2
        } else {
            // After separator → editable
            true
        }
    } else {
        // No separator → only first 2 chars editable
        col < 2
    }
}

// Fixed version: index_lines_batch now properly reads multiple lines for UCS-2
fn index_lines_batch(
    file: &File,
    batch_size: usize,
    start_offset: usize,
    line_ptr_map: &mut HashMap<usize, usize>,
    line_len_map: &mut HashMap<usize, usize>,
    starting_line_num: usize,
    char_size: usize,
) -> io::Result<usize> {
    // 1) mmap entire file read-only
    let mmap: memmap2::Mmap = unsafe { MmapOptions::new().map(file)? };
    let file_len = mmap.len();

    let mut offset = start_offset;
    let mut line_num = starting_line_num;

    // how many characters wide is your size field?
    let size_field_chars = 7;
    let size_field_bytes = size_field_chars * char_size;
    let mut counter : u32 = 0;
    for _ in 0..batch_size {
        if offset >= file_len {
            break; // EOF
        }

        // record starts here
        let line_start = offset;

        // compute slice indices for the size‐field
        let field_start = line_start + 22 * char_size;
        let field_end   = field_start + size_field_bytes;

        // make sure we don’t overrun
        if field_end > file_len {
            eprintln!(
                "⚠️ size header at {}…{} out of bounds (file len {})",
                field_start, field_end, file_len
            );
            break;
        }

        // grab exactly the size‐field bytes
        let header_bytes = &mmap[field_start .. field_end];

        // decode to a digit-string
        let digits: String = if char_size == 2 {
            // UCS-2: map low bytes to chars, stop at first non-digit
            header_bytes
                .chunks(2)
                .map(|pair| pair[0] as char)
                .take_while(|c| c.is_ascii_digit())
                .collect()
        } else {
            // ASCII: grab the UTF-8 & stop at first non-digit
            std::str::from_utf8(header_bytes)
                .expect("invalid UTF-8 in size header")
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect()
        };

        // if we didn’t get exactly size_field_chars digits, it wasn’t all numeric:
        if digits.len() != size_field_chars {
            eprintln!(
                "⚠️  size header contains non-numeric at offset {}…{}: {:?}",
                field_start, field_end, digits,
            );
            break ;
        }

        counter +=1;
        // parse the total byte‐length of this record
        let record_len: usize = digits
            .parse()
            .expect("size header not numeric");

        // compute the next offset (just past this record)
        let next_offset = line_start + record_len;


        // insert into your maps
        line_ptr_map.insert(line_num, line_start);
        line_len_map.insert(line_num, record_len);

        // advance to the next record
        offset = next_offset;
        line_num += 1;

    }

    // return the offset you stopped at
    Ok(offset)
}


fn print_header(h_offset: usize, v_offset: usize, view_width: usize, view_height: usize, stdout: &mut Stdout) {
    execute!(stdout, MoveTo(0, 0));
    // Top status line
    queue!(stdout, MoveTo(0, 0), Print("JBlox Editor"));

    // Second help line
    let controlsstr: String 
        = "Arrows: ↑↓↔ |Ctrl+p/Alt+p |Ctrl+f/Ctrl+l|Alt+f/Alt+l |Ctrl+S |Ctrl+z |Ctrl+g |Ctrl+q"
        .to_string()
        .chars()
        .take(view_width-2)
        .collect();
    queue!(stdout, MoveTo(0, 1), Print(controlsstr));
}



fn main() -> io::Result<()> {
    let mut modified_positions: HashSet<(usize, usize)> = HashSet::new();
    let mut modified_positions_rollback: HashSet<(usize, usize)> = HashSet::new();
    let mut modified_positions_saved: HashSet<(usize, usize)> = HashSet::new();

    
    let mut orig_line_map: HashMap<usize, String> = HashMap::new();
    let mut modified_line_map: HashMap<usize, String> = HashMap::new();


    // Create a history file with timestamp
    let timestamp = Local::now().format("%Y%m%d_%H%M%S");


    let matches = Command::new("JBlox Editor")
        .version("0.6.0")
        .about("Mmap JBlox editor with arrow-key scrolling and cursor")
        .arg(
            Arg::new("file")
                .short('f')
                .long("file")
                .value_name("FILE")
                .required(true)
                .num_args(1)
                .help("Path to .jblox file"),
        )
        .arg(
            Arg::new("batch-size")
                .short('b')
                .long("batch-size")
                .value_name("BATCH")
                .required(false)
                .num_args(1)
                .help("Batch size for indexing lines (default: 40000)"),
        )
        .arg(
            Arg::new("char-size")
                .short('c')
                .long("char-size")
                .value_name("SIZE")
                .required(false)
                .num_args(1)
                .help("Character size: 1 for single byte, 2 for UCS-2 (default: 2)"),
        )
        .arg(
            Arg::new("rollback-lines")
                .short('r')
                .long("rollback-lines")
                .value_name("rollback-lines")
                .required(false)
                .num_args(1)
                .help("rollback-lines for indexing lines (default: 1000)"),
        )
        .arg(
            Arg::new("protected-chars")
                .short('p')
                .long("protected-chars")
                .value_name("CHARS")
                .required(false)
                .help("Comma-separated list of protected characters (default: ':,`,-')")
        )
        .arg(
            Arg::new("separator-char")
                .short('s')
                .long("separator-char")
                .value_name("CHAR")
                .required(false)
                .help("Single character used as separator (default: ':')")
        ).get_matches();

    let file_path = matches.get_one::<String>("file").unwrap();
    let batch_size = matches
                            .get_one::<String>("batch-size")
                            .and_then(|s| s.parse::<usize>().ok())
                            .unwrap_or(10000); // default
    let rollback_lines = matches
                            .get_one::<String>("rollback-lines")
                            .and_then(|s| s.parse::<usize>().ok())
                            .unwrap_or(1000); // default       

    let char_size = matches
        .get_one::<String>("char-size")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(2);

    let protected_chars_arg: Vec<char> = matches
        .get_one::<String>("protected-chars")
        .map(|s| s.chars().filter(|c| *c != ',').collect())
        .unwrap_or_else(|| vec![':', '`', '-']);

    let recsepratorforlen_arg: char = matches
        .get_one::<String>("separator-char")
        .and_then(|s| s.chars().next())
        .unwrap_or(':');

    //max size for below decided by rollback_lines
    let mut rollback_orig_line_map: HashMap<usize, String> = HashMap::new();
    let mut rollback_insert_tracker: Vec<usize> = Vec::with_capacity(rollback_lines);
    
    let path = PathBuf::from(file_path);

    let file = OpenOptions::new().read(true).write(true).open(&path)?;

    let metadata = file.metadata()?;
    let file_size = metadata.len();
    println!("Opened file: {:?} ({} bytes)", path, file_size);

    // Determine protected_chars based on filename
    let file_name = match path.file_name() {
        Some(name) => name.to_string_lossy().to_string(),
        None => {
            eprintln!("❌ Failed to extract filename from path.");
            std::process::exit(1);
        }
    };
    let protected_chars: Vec<char> = if file_name.contains(".jblox") {
        protected_chars_arg
    } else {
        vec![] // No restrictions
    };

    // Get parent directory of input file
    let parent_dir = path.parent().unwrap_or_else(|| Path::new("."));

    // Build history file path in same directory
    let history_file_name = format!("{}.mod.{}", file_name, timestamp);
    let history_file_path = parent_dir.join(&history_file_name);

    let mut history_file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(&history_file_path)?;


    // Offsets for scrolling
    let mut offset: u64 = 0;
    let mut v_offset: usize = 0; // Vertical scroll offset
    let mut h_offset: usize = 0; // Horizontal scroll offset

    let (mut cols, mut rows) = terminal::size()?;


    let mut stdout = stdout();
    // Clear screen ONCE before first draw
    execute!(stdout, Clear(ClearType::All), MoveTo(0, 0))?;

    let mut mmap_file 
                = unsafe { MmapOptions::new().len(file_size as usize).map_mut(&file)? };


    //let mut mmap = unsafe { MmapOptions::new().len(file_size as usize).map_anon()? };

    // 2) Create a private (MAP_PRIVATE) staging map
    //    writes here are never visible to the file on disk
    let mut mmap: MmapMut = unsafe {MmapOptions::new().map_copy(&file)? };    

    //mmap.copy_from_slice(&mmap_file);

    let mut offset = 0;
    let mut line_num = 0;

    let mut line_ptr_map: HashMap<usize, usize> = HashMap::new();
    let mut line_len_map: HashMap<usize, usize> = HashMap::new();
    let mut ui_line_map: HashMap<usize, usize> = HashMap::new();


    let mut file_pos = 0;


    //------------- debug -----------------
    let print_header_debug 
            = |           ui_line_map: &HashMap<usize, usize>,
                          window_lines: &[String],
                          view_width: usize,
                          v_offset: usize,
                          h_offset: usize,
                          cursor: &Cursor,
                          modified_positions: &HashSet<(usize, usize)>,
                          stdout: &mut Stdout,
                          message: String| -> std::io::Result<()>{

            //for (key, value) in ui_line_map {
            //    println!("Key: {}, Value: {}", key, value);
           // }
               execute!(stdout, MoveTo(0, 0));
           println!("message: {}",message);

            Ok(())
    };


    //------------- update screen data --------------
let draw_visible_lines_v3 = |
    ui_line_map: &HashMap<usize, usize>,
    window_lines: &[String],
    view_width: usize,
    v_offset: usize,
    h_offset: usize,
    cursor: &Cursor,
    modified_positions: &HashSet<(usize, usize)>,
    modified_positions_rollback: &HashSet<(usize, usize)>,
    modified_positions_saved: &HashSet<(usize, usize)>,
    modified_line_map_ip: &HashMap<usize, String>,
    orig_line_map: &HashMap<usize, String>,
    stdout: &mut Stdout,
    view_height: usize
| -> std::io::Result<()> {

    print_header(h_offset, v_offset,view_width , view_height, stdout);

    // Draw visible lines
    for (i, mut line) in window_lines.iter().enumerate()
    {
        let file_line_temp = ui_line_map[&i];

        // If line was modified, use it
        if let Some(modified_line) = modified_line_map_ip.get(&file_line_temp) {
            line = modified_line;
        }

        let visible = if h_offset < line.len()-1 {
            let end = (h_offset + view_width).min(line.len()-1);
            &line[h_offset..end]
        } else {
            ""
        };

        // Pad if visible part is shorter than view_width, 
        //not working, need to investigate
        //let mut padded_visible = format!("{:<width$}", visible, width = view_width);
        
        let mut padded_visible = visible.to_string();

        //+2 due to 2 lines to be reserved for header
        queue!(stdout, MoveTo(0, (i+2) as u16), Print(format!("{:08}: ", file_line_temp)));

        for (j, ch) in padded_visible.chars().enumerate() {
            let logical_row = v_offset + i;
            let logical_col = (h_offset + j) * cursor.char_size;

            if i == cursor.row && j == cursor.col {
                execute!(
                    stdout,
                    SetForegroundColor(Color::Blue),
                    SetAttribute(Attribute::Underlined),
                    Print(ch),
                    SetAttribute(Attribute::Reset),
                    ResetColor
                )?;
            }
            else if modified_positions_saved.contains(&(ui_line_map[&i], logical_col)) {
                execute!(
                    stdout,
                    SetForegroundColor(Color::Yellow),
                    SetAttribute(Attribute::Underlined),
                    Print(ch),
                    SetAttribute(Attribute::Reset),
                    ResetColor
                )?;
            } else if modified_positions_rollback.contains(&(ui_line_map[&i], logical_col)) {
                execute!(
                    stdout,
                    SetForegroundColor(Color::Green),
                    SetAttribute(Attribute::Underlined),
                    Print(ch),
                    SetAttribute(Attribute::Reset),
                    ResetColor
                )?;
            } else if modified_positions.contains(&(ui_line_map[&i], logical_col)) {
                execute!(
                    stdout,
                    SetForegroundColor(Color::Red),
                    SetAttribute(Attribute::Underlined),
                    Print(ch),
                    SetAttribute(Attribute::Reset),
                    ResetColor
                )?;
            } else {
                print!("{}", ch);
            }
        }

        // Fill remaining space if line is shorter than view_width
        for _ in visible.chars().count()..view_width {
            print!(" ");
        }
        println!();
    }
    stdout.flush()?;
    Ok(())
};



    //set file encoding
    // Detect encoding
    let encoding = detect_encoding(&mmap);
    
    // Now you can handle file based on encoding
    //editor only handles UTF-8

    if encoding == "utf-16"  || encoding == "utf-8"{
        //utf-8 file, so we are good
    } else {
        panic!("Unsupported encoding: {}", encoding);
    }

    let mut initilizing : bool = true;
    
    // Show initial message
    execute!(stdout, Clear(ClearType::All), MoveTo(0, 0))?;
    println!("📂 Loading file: {} ({} bytes)", file_name, file_size);
    println!("⏳ Indexing lines, please wait...");
    stdout.flush()?;
        
    loop {
        let prev_offset = offset;

        offset = index_lines_batch(
            &file,
            batch_size,
            offset,
            &mut line_ptr_map,
            &mut line_len_map,
            line_num,
            char_size.clone()
        )?;

        // Update progress bar
        let progress = (offset as f64 / file_size as f64) * 100.0;
        print!("\r📊 Progress: {:.2}% ({} lines indexed)", progress, line_ptr_map.len());
        io::stdout().flush()?;
        // Update starting line number for next batch
        line_num = line_ptr_map.len();
        if offset == prev_offset || line_num % batch_size != 0{
            // No more lines, EOF reached
            break;
        }


    }

    let mut view_height = (rows as usize).saturating_sub(4); // Adjust for header/footer
    let view_width = (cols as usize).saturating_sub(10); // Adjust for line number prefix
    // Logical cursor (row/col in viewport)
    let mut cursor = Cursor::new(view_width,char_size);
    let mut window_lines: Vec<String> = Vec::with_capacity(view_height);

    if mmap_file.len() == 0 || line_ptr_map.is_empty() || line_len_map.is_empty() {
        println!("⚠️ Cannot open file: no data or empty mappings.");
        return Ok(());
    }


    execute!(
        stdout,
        MoveTo(0, 2),
        Clear(ClearType::CurrentLine),
        Print("✅ File indexing complete. Starting editor...")
    )?;
    stdout.flush()?;
    std::thread::sleep(std::time::Duration::from_millis(500)); // brief pause    
    // Enter raw mode and alternate screen for clean UI
    enable_raw_mode()?;
    execute!(stdout, EnterAlternateScreen, Hide)?;

    // Populate UI mapping: screen row → file line number

    //need to correct view_height if file is short
    if(view_height > line_ptr_map.len()){
        view_height = line_ptr_map.len();
    }

for row in 0..view_height {
    ui_line_map.insert(row, row); // screen row 0 maps to file line 0, etc.

    let line_start = *line_ptr_map.get(&ui_line_map[&row]).expect("");
    let line_len = *line_len_map.get(&ui_line_map[&row]).expect("");

    // Fetch new line
    let line_bytes = &mmap[line_start..line_start + line_len];

    let new_line = if cursor.char_size == 2 {
        // Decode as UTF-16LE (for UCS-2/UTF-16 files)
        use std::slice;
        let u16_slice = unsafe {
            slice::from_raw_parts(line_bytes.as_ptr() as *const u16, line_bytes.len() / 2)
        };
        String::from_utf16_lossy(u16_slice)
    } else {
        // Decode as UTF-8 for single byte
        String::from_utf8_lossy(line_bytes).to_string()
    };

    window_lines.insert(row, new_line);
}


    // Move terminal cursor to (0,0) before redraw
    execute!(stdout, MoveTo(0, 0))?;
    loop {
        //initilizing = false;
        if(initilizing){
            initilizing = false;
        // 👇 Start a scope block so text & lines drop early
        {
                        draw_visible_lines_v3( &ui_line_map,
                                        &window_lines,
                                        view_width,
                                        v_offset,
                                        h_offset,
                                        &cursor,
                                        &modified_positions,
                                        &modified_positions_rollback,
                                        &modified_positions_saved,
                                        &modified_line_map,
                                        &orig_line_map,
                                        &mut stdout,
                                        view_height.clone())?; 
            } // 👈 Scope ends: text & lines dropped here
            // Place visible terminal cursor at logical position
        }


        // Handle keys (ignore KeyUp events)
        if let Event::Key(KeyEvent {
            code,
            modifiers,
            kind,
            ..
        }) = event::read()?
        {
            if kind == KeyEventKind::Press {
                match (code, modifiers) {
                    (KeyCode::Char('f'), modifiers) if modifiers.contains(KeyModifiers::CONTROL) => {
                    cursor.col = 0;
                    cursor.row = 0;

                    ui_line_map.clear(); 
                    window_lines.clear();   
                    for row in 0..view_height {
                        ui_line_map.insert(row, row); // screen row 0 maps to file line 0, etc.

                        let line_start = *line_ptr_map.get(&ui_line_map[&row]).expect("");
                        let line_len = *line_len_map.get(&ui_line_map[&row]).expect("");

                        // Fetch new line
                        let line_bytes = &mmap[line_start..line_start + line_len];

                        let new_line = if cursor.char_size == 2 {
                            // Decode as UTF-16LE (for UCS-2/UTF-16 files)
                            use std::slice;
                            let u16_slice = unsafe {
                                slice::from_raw_parts(line_bytes.as_ptr() as *const u16, line_bytes.len() / 2)
                            };
                            String::from_utf16_lossy(u16_slice)
                        } else {
                            // Decode as UTF-8 for single byte
                            String::from_utf8_lossy(line_bytes).to_string()
                        };

                        window_lines.insert(row, new_line);
                    }

                        draw_visible_lines_v3( &ui_line_map,
                                        &window_lines,
                                        view_width,
                                        v_offset,
                                        h_offset,
                                        &cursor,
                                        &modified_positions,
                                        &modified_positions_rollback,
                                        &modified_positions_saved,
                                        &modified_line_map,
                                        &orig_line_map,
                                        &mut stdout,
                                        view_height.clone())?; 


                    }
                    (KeyCode::Up, _) => {
                        let uilinemapstart = 0;
                        let dummint = 0;
                        if ui_line_map[&dummint] == 0 {
                            // Move cursor up within window
                            cursor.move_up(&mut stdout);
                        } else if cursor.row > 0 {
                            // Move cursor up within window
                            cursor.move_up(&mut stdout);
                        } else {
                            // Cursor at top → slide window up
                            let uilength = ui_line_map.len();
                            let tempval = ui_line_map[&0];
                            for row in 0..uilength {
                                ui_line_map.insert(row, tempval + row - 1);
                            }

                            let uilinefirstindex = 0;
                            let line_start = *line_ptr_map.get(&ui_line_map[&uilinefirstindex]).expect("");
                            let line_len = *line_len_map.get(&ui_line_map[&uilinefirstindex]).expect("");

                            let line_bytes = &mmap[line_start..line_start + line_len];

                            let new_line = if cursor.char_size == 2 {
                                // Decode UCS-2/UTF-16LE
                                use std::slice;
                                let u16_slice = unsafe {
                                    slice::from_raw_parts(line_bytes.as_ptr() as *const u16, line_bytes.len() / 2)
                                };
                                String::from_utf16_lossy(u16_slice)
                            } else {
                                // Decode UTF-8
                                String::from_utf8_lossy(line_bytes).to_string()
                            };

                            // Update window_lines
                            window_lines.remove(uilength - 1);
                            window_lines.insert(0, new_line);
                        }
                        draw_visible_lines_v3( &ui_line_map,
                                        &window_lines,
                                        view_width,
                                        v_offset,
                                        h_offset,
                                        &cursor,
                                        &modified_positions,
                                        &modified_positions_rollback,
                                        &modified_positions_saved,
                                        &modified_line_map,
                                        &orig_line_map,
                                        &mut stdout,
                                        view_height.clone())?; 


                    }
                    (KeyCode::Char('l'), modifiers) if modifiers.contains(KeyModifiers::CONTROL) => {
                        let ptrmaplen = line_ptr_map.len();
                        for row in 0..view_height {
                            ui_line_map.insert(row, ptrmaplen + row - view_height); // screen row 0 maps to file line 0, etc.

                            let line_start = *line_ptr_map.get(&ui_line_map[&row]).expect("");
                            let line_len = *line_len_map.get(&ui_line_map[&row]).expect("");

                            let line_bytes = &mmap[line_start..line_start + line_len];

                            let new_line = if cursor.char_size == 2 {
                                // Decode UCS-2/UTF-16LE
                                use std::slice;
                                let u16_slice = unsafe {
                                    slice::from_raw_parts(line_bytes.as_ptr() as *const u16, line_bytes.len() / 2)
                                };
                                String::from_utf16_lossy(u16_slice)
                            } else {
                                // Decode UTF-8
                                String::from_utf8_lossy(line_bytes).to_string()
                            };

                            window_lines.push(new_line);
                            window_lines.remove(0);
                        }
                        draw_visible_lines_v3( &ui_line_map,
                                        &window_lines,
                                        view_width,
                                        v_offset,
                                        h_offset,
                                        &cursor,
                                        &modified_positions,
                                        &modified_positions_rollback,
                                        &modified_positions_saved,
                                        &modified_line_map,
                                        &orig_line_map,
                                        &mut stdout,
                                        view_height.clone())?; 
                                        
                    }
                    (KeyCode::Down, _) => {
                        // Updated block to handle char_size properly

                        // don't do anything if EOF reached
                        let uilength = ui_line_map.len();
                        let tempval = ui_line_map[&0];
                        let uilinelastindex = uilength - 1;

                        if cursor.row + 1 < view_height {
                            // Move cursor down within visible window
                            cursor.move_down(&mut stdout, view_height);
                        } else {
                            if let Some(_) = line_ptr_map.get(&(ui_line_map[&uilinelastindex] + 1)) {
                                cursor.move_down(&mut stdout, view_height);

                                // Slide window down
                                let nextexpvalue = ui_line_map[&uilinelastindex] + 1;
                                let line_start = *line_ptr_map.get(&nextexpvalue).expect("");
                                let line_len = *line_len_map.get(&nextexpvalue).expect("");

                                // Fetch new bottom line
                                let line_bytes = &mmap[line_start..line_start + line_len];
                                let new_line = if cursor.char_size == 2 {
                                    // Decode UCS-2/UTF-16LE
                                    use std::slice;
                                    let u16_slice = unsafe {
                                        slice::from_raw_parts(line_bytes.as_ptr() as *const u16, line_bytes.len() / 2)
                                    };
                                    String::from_utf16_lossy(u16_slice)
                                } else {
                                    // Decode UTF-8
                                    String::from_utf8_lossy(line_bytes).to_string()
                                };

                                for row in 0..uilength {
                                    ui_line_map.insert(row, tempval + row + 1);
                                }

                                // Update window_lines
                                window_lines.remove(0);
                                window_lines.insert(uilinelastindex, new_line);
                            }
                        }
           
                        draw_visible_lines_v3( &ui_line_map,
                                        &window_lines,
                                        view_width,
                                        v_offset,
                                        h_offset,
                                        &cursor,
                                        &modified_positions,
                                        &modified_positions_rollback,
                                        &modified_positions_saved,
                                        &modified_line_map,
                                        &orig_line_map,
                                        &mut stdout,
                                        view_height.clone())?; 
                    }
                    (KeyCode::Char('f'), modifiers) if modifiers.contains(KeyModifiers::ALT) => {
                        h_offset = 0;
                        cursor.col = 0;
                        draw_visible_lines_v3( &ui_line_map,
                                        &window_lines,
                                        view_width,
                                        v_offset,
                                        h_offset,
                                        &cursor,
                                        &modified_positions,
                                        &modified_positions_rollback,
                                        &modified_positions_saved,
                                        &modified_line_map,
                                        &orig_line_map,
                                        &mut stdout,
                                        view_height.clone())?;                      
                    }
                    (KeyCode::Left, _) => {
                        if cursor.col > 0 {
                            cursor.move_left(&mut stdout);
                        } else if h_offset > 0 {
                            // Adjust horizontal offset by one character (in char_size units)
                            h_offset = h_offset.saturating_sub(1);
                        }
                        draw_visible_lines_v3( &ui_line_map,
                                        &window_lines,
                                        view_width,
                                        v_offset,
                                        h_offset,
                                        &cursor,
                                        &modified_positions,
                                        &modified_positions_rollback,
                                        &modified_positions_saved,
                                        &modified_line_map,
                                        &orig_line_map,
                                        &mut stdout,
                                        view_height.clone())?;                   
                    }
                    (KeyCode::Backspace, _) => {
                        if cursor.col > 0 {
                            cursor.move_left(&mut stdout);
                        } else if h_offset > 0 {
                            // Adjust horizontal offset by one character (in char_size units)
                            h_offset = h_offset.saturating_sub(1);
                        }
                        draw_visible_lines_v3( &ui_line_map,
                                        &window_lines,
                                        view_width,
                                        v_offset,
                                        h_offset,
                                        &cursor,
                                        &modified_positions,
                                        &modified_positions_rollback,
                                        &modified_positions_saved,
                                        &modified_line_map,
                                        &orig_line_map,
                                        &mut stdout,
                                        view_height.clone())?;                        
                    }
                    (KeyCode::Char('l'), modifiers) if modifiers.contains(KeyModifiers::ALT) => {
                    let current_line = &window_lines[cursor.row];

                    // 🔥 Use length of current_line directly as displayed char count
                    let line_len = current_line.len();

                    if line_len > view_width {
                        h_offset = line_len.saturating_sub(view_width);
                        cursor.col = view_width.saturating_sub(2);
                    } else {
                        h_offset = 0;
                        cursor.col = line_len.saturating_sub(2);
                    }
                        draw_visible_lines_v3( &ui_line_map,
                                        &window_lines,
                                        view_width,
                                        v_offset,
                                        h_offset,
                                        &cursor,
                                        &modified_positions,
                                        &modified_positions_rollback,
                                        &modified_positions_saved,
                                        &modified_line_map,
                                        &orig_line_map,
                                        &mut stdout,
                                        view_height.clone())?;                  
                    }


                    (KeyCode::Right, _) => {
                        let current_line = &window_lines[cursor.row];

                        // 🔥 Use length of current_line directly as displayed char count
                        let line_len = current_line.len();

                        let visible_len = line_len.saturating_sub(h_offset);
                        if cursor.col + 2 < std::cmp::min(visible_len, view_width) {
                             cursor.move_right(&mut stdout,line_len*char_size);
                             //cursor.col += 1;
                        } else if h_offset + view_width < line_len - 1 {
                            h_offset += 1;
                        }
                        draw_visible_lines_v3( &ui_line_map,
                                        &window_lines,
                                        view_width,
                                        v_offset,
                                        h_offset,
                                        &cursor,
                                        &modified_positions,
                                        &modified_positions_rollback,
                                        &modified_positions_saved,
                                        &modified_line_map,
                                        &orig_line_map,
                                        &mut stdout,
                                        view_height.clone())?; 
                    }



                    (KeyCode::Char('p'), modifiers) if modifiers.contains(KeyModifiers::CONTROL) =>{
                    let mut firstrec = ui_line_map[&0];
                    let mut tobeshifted = view_height;
                    if firstrec < view_height {
                        tobeshifted = firstrec;
                    }
                    let uilength = ui_line_map.len();
                    let tempval = ui_line_map[&0];
                    for row in 0..uilength {
                        ui_line_map.insert(row, tempval + row - tobeshifted);

                        let line_start = *line_ptr_map.get(&ui_line_map[&row]).expect("");
                        let line_len = *line_len_map.get(&ui_line_map[&row]).expect("");

                        let line_bytes = &mmap[line_start..line_start + line_len];
                        let new_line = if cursor.char_size == 2 {
                            // Decode UCS-2/UTF-16LE
                            use std::slice;
                            let u16_slice = unsafe {
                                slice::from_raw_parts(line_bytes.as_ptr() as *const u16, line_bytes.len() / 2)
                            };
                            String::from_utf16_lossy(u16_slice)
                        } else {
                            // Decode UTF-8
                            String::from_utf8_lossy(line_bytes).to_string()
                        };
                        window_lines.push(new_line);
                        window_lines.remove(0);
                    }

                        draw_visible_lines_v3( &ui_line_map,
                                        &window_lines,
                                        view_width,
                                        v_offset,
                                        h_offset,
                                        &cursor,
                                        &modified_positions,
                                        &modified_positions_rollback,
                                        &modified_positions_saved,
                                        &modified_line_map,
                                        &orig_line_map,
                                        &mut stdout,
                                        view_height.clone())?;   
                    }

                    (KeyCode::Char('p'), modifiers) if modifiers.contains(KeyModifiers::ALT) => {

                    let uilength: usize = ui_line_map.len();
                    let lastrec = ui_line_map[&(uilength - 1)];
                    let mut tobeshifted = view_height;

                    if line_ptr_map.len() < lastrec + view_height {
                        tobeshifted = line_ptr_map.len() - lastrec - 1;
                    }

                    let tempval = ui_line_map[&0];
                    for row in 0..uilength {
                        ui_line_map.insert(row, tempval + row + tobeshifted);
                        if let Some(&val) = line_ptr_map.get(&ui_line_map.get(&row).unwrap_or(&0)) {
                            let line_start = *line_ptr_map.get(&ui_line_map[&row]).expect("");
                            let line_len = *line_len_map.get(&ui_line_map[&row]).expect("");

                            let line_bytes = &mmap[line_start..line_start + line_len];
                            let new_line = if cursor.char_size == 2 {
                                // Decode UCS-2/UTF-16LE
                                use std::slice;
                                let u16_slice = unsafe {
                                    slice::from_raw_parts(line_bytes.as_ptr() as *const u16, line_bytes.len() / 2)
                                };
                                String::from_utf16_lossy(u16_slice)
                            } else {
                                // Decode UTF-8
                                String::from_utf8_lossy(line_bytes).to_string()
                            };
                            window_lines.push(new_line);
                            window_lines.remove(0);
                        } else {
                            //do nothing
                        }
                    }
                        draw_visible_lines_v3( &ui_line_map,
                                        &window_lines,
                                        view_width,
                                        v_offset,
                                        h_offset,
                                        &cursor,
                                        &modified_positions,
                                        &modified_positions_rollback,
                                        &modified_positions_saved,
                                        &modified_line_map,
                                        &orig_line_map,
                                        &mut stdout,
                                        view_height.clone())?;                  
                    }

                    (KeyCode::Char('z'), modifiers) if modifiers.contains(KeyModifiers::CONTROL) => {
                    // get current row
                    // Multiply by cursor.char_size for UCS-2 support
                    let file_col_formmap = h_offset* cursor.char_size + cursor.col ;                    

                    let file_line_number = ui_line_map[&cursor.row];
                    let currentline = window_lines[cursor.row].clone();
                    let mut origrec: String = String::new();

                    // get original line from rollback and replace window_lines
                    if let Some(origlinefrmrollback) = rollback_orig_line_map.get(&file_line_number).cloned() {
                        window_lines[cursor.row] = origlinefrmrollback.clone();
                        origrec = origlinefrmrollback.clone();

                        // save current line to orig_line_map if not already saved
                        if !orig_line_map.contains_key(&file_line_number) {
                            orig_line_map.insert(file_line_number, currentline.clone());
                        }

                        modified_line_map.insert(file_line_number, origrec.clone());

                        let file_line_ptr: usize = line_ptr_map[&file_line_number];
                        let file_line_len: usize = line_len_map[&file_line_number];

                        // mark all columns in the line as rollback-modified
                        let char_count = file_line_len;
                        for col in 0..char_count {
                            modified_positions_rollback.insert((file_line_number, col));
                        }

                        // remove any saved modifications for this line
                        modified_positions_saved.retain(|(first, _)| *first != file_line_number);

                        // update mmap with rollback content
                        let new_bytes = if cursor.char_size == 2 {
                            // encode to UTF-16LE
                            let utf16: Vec<u16> = origrec.encode_utf16().collect();
                            unsafe {
                                std::slice::from_raw_parts(
                                    utf16.as_ptr() as *const u8,
                                    utf16.len() * 2
                                ).to_vec()
                            }
                        } else {
                            // UTF-8 bytes
                            origrec.as_bytes().to_vec()
                        };

                        let slice = &mut mmap[file_line_ptr..file_line_ptr + file_line_len];
                        if new_bytes.len() <= slice.len() {
                            slice[..new_bytes.len()].copy_from_slice(&new_bytes);
                            for byte in &mut slice[new_bytes.len()..] {
                                *byte = if cursor.char_size == 2 { 0 } else { b' ' };
                            }
                        } else {
                            slice.copy_from_slice(&new_bytes[..slice.len()]);
                        }
                                            

                        //redraw
                        draw_visible_lines_v3( &ui_line_map,
                                        &window_lines,
                                        view_width,
                                        v_offset,
                                        h_offset,
                                        &cursor,
                                        &modified_positions,
                                        &modified_positions_rollback,
                                        &modified_positions_saved,
                                        &modified_line_map,
                                        &orig_line_map,
                                        &mut stdout,
                                        view_height.clone())?;        
                        }               
                    }
                    (KeyCode::Char('g'), modifiers) if modifiers.contains(KeyModifiers::CONTROL) => {
                    // Leave raw mode temporarily
                    disable_raw_mode()?;
                    execute!(stdout, LeaveAlternateScreen, Show)?;
                    execute!(
                        stdout,
                        Clear(ClearType::All),
                        MoveTo(0, 0)
                    ).unwrap();
                    print!("Enter line number to go to: ");
                    io::stdout().flush()?; // flush input prompt

                    let mut input = String::new();
                    io::stdin().read_line(&mut input)?;
                    let mut target_line = input.trim().parse::<usize>().unwrap_or(0);

                    
                    enable_raw_mode()?;
                    execute!(stdout, EnterAlternateScreen, Hide, Clear(ClearType::All), MoveTo(0, 0))?;

                    if target_line > (line_ptr_map.len() - window_lines.len()) {
                        target_line = line_ptr_map.len() - window_lines.len();
                    }

                    if line_ptr_map.contains_key(&target_line) {
                        // Update ui_line_map so target_line is at the top
                        ui_line_map.clear(); 
                        window_lines.clear();
                        for row in 0..view_height {
                            ui_line_map.insert(row, target_line + row);
                            if let Some(&line_start) = line_ptr_map.get(&(target_line + row)) {
                                let line_len = line_len_map[&(target_line + row)];
                                let line_bytes = &mmap[line_start..line_start + line_len];
                                let new_line = if cursor.char_size == 2 {
                                    // Decode UCS-2/UTF-16LE
                                    use std::slice;
                                    let u16_slice = unsafe {
                                        slice::from_raw_parts(line_bytes.as_ptr() as *const u16, line_bytes.len() / 2)
                                    };
                                    String::from_utf16_lossy(u16_slice)
                                } else {
                                    // Decode UTF-8
                                    String::from_utf8_lossy(line_bytes).to_string()
                                };

                                window_lines.push(new_line);
                            }
                        }

                        cursor.row = 0;
                        cursor.col = 0;
                        h_offset = 0;


                            
                        draw_visible_lines_v3( &ui_line_map,
                                        &window_lines,
                                        view_width,
                                        v_offset,
                                        h_offset,
                                        &cursor,
                                        &modified_positions,
                                        &modified_positions_rollback,
                                        &modified_positions_saved,
                                        &modified_line_map,
                                        &orig_line_map,
                                        &mut stdout,
                                        view_height.clone())?; 
                        } else {
                        draw_visible_lines_v3( &ui_line_map,
                                        &window_lines,
                                        view_width,
                                        v_offset,
                                        h_offset,
                                        &cursor,
                                        &modified_positions,
                                        &modified_positions_rollback,
                                        &modified_positions_saved,
                                        &modified_line_map,
                                        &orig_line_map,
                                        &mut stdout,
                                        view_height.clone())?; 
                        }
                    }
                    (KeyCode::Char('s'), modifiers)
                    if modifiers.contains(KeyModifiers::CONTROL) =>
                    {
                        //use modified_line_map to write to mmap_file
                        for (line_num, new_text) in &modified_line_map {
                            //use line_ptr_map to get offset
                            let start_byte = line_ptr_map[&line_num];
                            let size_bytes = line_len_map[&line_num];

                            for (i, c) in new_text.chars().enumerate() {
                                let code = c as u32;
                                if code > 0xFFFF {
                                    return Err(io::Error::new(
                                        ErrorKind::InvalidInput,
                                        format!("char {} (U+{:X}) is out of UCS-2 range", i, code),
                                    ));
                                }                                
                                let u = code as u16;
                                let [lo, hi] = u.to_le_bytes();
                                let pos = start_byte + i * 2;
                                mmap_file[pos]     = lo;
                                mmap_file[pos + 1] = hi;
                            }
                            mmap_file.flush()?;
                            // you can read `line_num: &usize` and `new_text: &String` here
                        }

                        // --- Write to history file ---
                        writeln!(history_file, "\n=== Saved at {} ===\n", Local::now().format("%Y-%m-%d %H:%M:%S"))?;

                        for (&line_num, orig_line) in &orig_line_map {
                            writeln!(history_file, "ORIGINAL [{}]: {}", line_num, orig_line)?;

                            if let Some(mod_line) = modified_line_map.get(&line_num) {
                                writeln!(history_file, "MODIFIED [{}]: {}", line_num, mod_line)?;
                            }
                            writeln!(history_file, "-----")?;

                        }

                        //populate modified_positions_saved from modified_positions_rollback
                        modified_positions_saved.extend(modified_positions.iter().cloned());


                        history_file.flush()?; // ensure everything written
                        //println!("📝 History file updated: {}", history_file_name);
                        modified_positions.clear();
                        modified_positions_rollback.clear();
                        orig_line_map.clear();
                        modified_line_map.clear();


                        draw_visible_lines_v3( &ui_line_map,
                                        &window_lines,
                                        view_width,
                                        v_offset,
                                        h_offset,
                                        &cursor,
                                        &modified_positions,
                                        &modified_positions_rollback,
                                        &modified_positions_saved,
                                        &modified_line_map,
                                        &orig_line_map,
                                        &mut stdout,
                                        view_height.clone())?;  

                    }

                    //handle edit
                    (KeyCode::Char(mut c), modifiers)
                        if modifiers.is_empty() && c.is_ascii_graphic() => {
                        //save any modified row


                        // Multiply by cursor.char_size for UCS-2 support
                        let file_col_formmap = (h_offset + cursor.col) * cursor.char_size;

                        let  orig_line_text = window_lines[cursor.row].clone();
                        let  mut tobemodified_line_text = window_lines[cursor.row].clone();

                        let file_line_number = ui_line_map[&cursor.row];
                        let file_line_ptr = line_ptr_map[&file_line_number];


                        let file_col = h_offset + cursor.col;
                        let file_row = v_offset + cursor.row;

                        // UCS-2 validation and explicit padding
                        let encoded: [u8; 2];
                        let c_u32 = c as u32;
                        if c.is_ascii() {
                            encoded = (c as u16).to_le_bytes();
                        } else if c_u32 <= 0xFFFF {
                            encoded = (c as u16).to_le_bytes();
                        } else {
                            c = '?';
                            encoded = ('?' as u16).to_le_bytes();
                        }

                        let editable = is_editable(
                            &orig_line_text,
                            file_col,
                            recsepratorforlen_arg,
                            file_name.contains(".jblox"),
                        );


                        let origchar =   orig_line_text
                                                .chars()
                                                .nth(file_col)
                                                .unwrap_or('?');
                        if !editable {
                            //println!(
                            //    "⚠️ Position ({}, {}) is protected.                            ",
                            //    file_row, file_col
                            //);
                        } else if protected_chars.contains(&origchar) {
                            //println!("⚠️ Char '{}' is protected.                             ", origchar);
                        } else {

                            tobemodified_line_text.replace_range(file_col..file_col+1, &c.to_string());


                            modified_line_map.insert(file_line_number, tobemodified_line_text.clone());


                            //save original 
                            if !orig_line_map.contains_key(&file_line_number) {
                                orig_line_map.insert(file_line_number, orig_line_text.clone());
                    
                            }
                            //save for rollbacks
                            //max size for below decided by rollback_lines
                            if !rollback_orig_line_map.contains_key(&file_line_number) {
                                if(rollback_insert_tracker.len() > rollback_lines){
                                    let mut linenumkeyval = 0;
                                    if let Some(linenumkeyvaltemp) 
                                                = rollback_insert_tracker.get(0){
                                        linenumkeyval = linenumkeyvaltemp.clone();
                                    }
                                    rollback_orig_line_map.remove(&linenumkeyval);
                                    rollback_insert_tracker.remove(0);       // Remove first element
                                }
                                rollback_insert_tracker.push(file_line_number.clone()); 
                                rollback_orig_line_map.insert(file_line_number, orig_line_text.clone());
                            }  



                            modified_positions.insert((file_line_number, file_col_formmap));

                            window_lines[cursor.row] = tobemodified_line_text.clone();
                            // 🔥 Write encoded bytes to mmap only
                            mmap[file_line_ptr + file_col_formmap..file_line_ptr + file_col_formmap + cursor.char_size]
                                .copy_from_slice(&encoded);
                        }

                        let current_line = &window_lines[cursor.row];

                        // 🔥 Use length of current_line directly as displayed char count
                        let line_len = current_line.len();

                        let visible_len = line_len.saturating_sub(h_offset);
                        if cursor.col + 2 < std::cmp::min(visible_len, view_width) {
                             cursor.move_right(&mut stdout,line_len*char_size);
                             //cursor.col += 1;
                        } else if h_offset + view_width < line_len - 1 {
                            h_offset += 1;
                        }
                        draw_visible_lines_v3( &ui_line_map,
                                        &window_lines,
                                        view_width,
                                        v_offset,
                                        h_offset,
                                        &cursor,
                                        &modified_positions,
                                        &modified_positions_rollback,
                                        &modified_positions_saved,
                                        &modified_line_map,
                                        &orig_line_map,
                                        &mut stdout,
                                        view_height.clone())?; 
                    }

                    (KeyCode::Char('q'), modifiers) if modifiers.contains(KeyModifiers::CONTROL)  => break,
                    _ => {}
                }
            }
        } 

    }


    execute!(stdout, Show, LeaveAlternateScreen)?;

    disable_raw_mode()?;
    // Clean up: restore terminal state
    execute!(
        stdout,
        MoveTo(0, 0),                  // Move to top-left
        Clear(ClearType::All)          // Clear entire screen
    )?;    
    println!("Exited JBlox Editor.");
    Ok(())
}
