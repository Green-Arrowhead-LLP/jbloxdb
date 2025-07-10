// jbloxDB
// © 2025 Green Arrowhead LLP
// Licensed under the jbloxDB License v1.0
// See LICENSE.txt for terms.
// Free for individuals and small companies.
// Commercial license required for production use by companies over USD 5M revenue or for SaaS/product distribution.

use memmap2::{MmapMut, MmapOptions};
use std::cell::UnsafeCell;
use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::{self, Seek, SeekFrom, Write, BufWriter};
use std::fs::File;
use std::path::PathBuf;
use std::time::Instant;
use serde_json::Value;
use serde::Deserialize;
use config::Config;
use chrono::Local;
use anyhow::Result;
 
use serde_json::{json};

use std::env;
use std::fs;
use std::path::{Path};
use std::process;

use std::time::{SystemTime, UNIX_EPOCH};
use chrono::{ Timelike};
use chrono::{DateTime};

use std::io::{BufRead, BufReader};

use std::io::{Read};

use std::sync::{Arc, Mutex};

/// Thread-safe wrapper around a raw pointer into a memory-mapped file.
#[derive(Debug)]
pub struct PtrWrapper(pub UnsafeCell<*mut u8>);

impl PtrWrapper {
    pub fn new(ptr: *mut u8) -> Self {
        PtrWrapper(UnsafeCell::new(ptr))
    }

    /// Advance pointer by offset
    pub fn add(&self, offset: usize) -> *mut u8 {
        unsafe { (*self.0.get()).add(offset) }
    }
    /// Raw pointer access
    pub fn as_ptr(&self) -> *mut u8 {
        unsafe { *self.0.get() }
    }
    /// Compute offset from another pointer
    pub fn offset_from(&self, other: &Self) -> isize {
        unsafe { (*self.0.get()).offset_from(*other.0.get()) }
    }
}

impl Clone for PtrWrapper {
    fn clone(&self) -> Self {
        PtrWrapper(UnsafeCell::new(self.as_ptr()))
    }
}
impl PartialEq for PtrWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.as_ptr() == other.as_ptr()
    }
}

impl Eq for PtrWrapper {}
impl std::hash::Hash for PtrWrapper {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (self.as_ptr() as usize).hash(state);
    }
}
unsafe impl Send for PtrWrapper {}
unsafe impl Sync for PtrWrapper {}

#[derive(Debug, Deserialize)]
pub struct Settings {
    initfilesize:f64, //in MB
    newfilesizemultiplier:usize, //new file size = this var times existing size
    datadir: String,
    logdir: String,
    logmaxlines: usize,
    indexnamevaluedelimiter: char,
    indexdelimiter: char,
    recorddelimiter: char,
    repindexnamevaluedelimiter: char,
    repindexdelimiter: char,
    reprecorddelimiter: char,
    maxgetrecords: usize,
    maxrecordlength: usize,
    maxlogtoconsolelength: usize,
    enableviewdelete: bool,

} 
pub struct jbothandler {
    settings: Settings,
    file_len_map: HashMap<String, usize>, //file->file length
    file_size_map: HashMap<String, usize>, //file->file length
    //file_mmap_map: HashMap<String, MmapMut>,//file->mmep of file
    file_line_map: HashMap<String, Vec<PtrWrapper>>,//file->pointer vector
    file_key_pointer_map: HashMap<String, HashMap<String, HashMap<String, Vec<PtrWrapper>>>>, //file->key name -> key -> pointer
    datadir: String,
    logdir: String,
    log_file: BufWriter<File>,
    log_line_count : usize,
    state: Arc<Mutex<SharedState>>,


}

struct SharedState {
    file_mmap_map: HashMap<String, MmapMut>,
}
/// Finds the byte offset of the start of the last line (after the last `\n`).
/// Returns `0` if file doesn't exist, is empty, or has no newlines.

pub fn find_last_data_offset(path: &str) -> std::io::Result<u64> {
    const BLOCK_SIZE: usize = 8192;

    let file_path = Path::new(path);
    if !file_path.exists() {
        eprintln!("⚠️ File not found: {}", path);
        return Ok(0);
    }

    let mut file = File::open(file_path)?;
    let filesize = file.metadata()?.len();
    if filesize == 0 {
        eprintln!("⚠️ File is empty: {}", path);
        return Ok(0);
    }

    let mut buf = vec![0u8; BLOCK_SIZE];
    let mut low = 0;
    let mut high = filesize;

    // 🚀 Binary search for end of text region
    while high - low > 1 {
        let mid = (low + high) / 2;

        file.seek(SeekFrom::Start(mid))?;
        let bytes_read = file.read(&mut buf)?;

        if bytes_read == 0 {
            break; // EOF
        }

        let mut has_printable_text = false;

        for offset in 0..=3 {
            if bytes_read > offset {
                let slice = &buf[offset..bytes_read];

                if let Ok(text) = std::str::from_utf8(slice) {
                    if text.chars().any(|c| !c.is_whitespace() && !c.is_control()) {
                        has_printable_text = true;
                        break;
                    }
                }
            }
        }

        if has_printable_text {
            low = mid; // ✅ Move cautiously
        } else {
            high = mid;
        }
    }

    // 🛡 Safer low with +4 bytes
    let low_safe = (low + 4).min(filesize);
    let start_pos = low_safe.saturating_sub(BLOCK_SIZE as u64 * 4);
    let mut pos = low_safe + BLOCK_SIZE as u64;
    let mut last_newline: Option<u64> = None;

    //eprintln!("🔍 Backward scan from offset: {}", pos);

    while pos > start_pos {
        let block_size = BLOCK_SIZE.min(pos as usize);
        pos -= block_size as u64;

        if pos > start_pos {
            pos -= 4; // overlap for UTF-8 safety
        }

        file.seek(SeekFrom::Start(pos))?;
        file.read_exact(&mut buf[..block_size])?;

        let mut has_printable_text = false;
        for offset in 0..=3 {
            if block_size > offset {
                let slice = &buf[offset..block_size];
                if let Ok(text) = std::str::from_utf8(slice) {
                    if text.chars().any(|c| !c.is_whitespace() && !c.is_control()) {
                        has_printable_text = true;
                        break;
                    }
                }
            }
        }

        if !has_printable_text {
            continue; // skip block
        }

        for i in (0..block_size).rev() {
            if buf[i] == b'\n' {
                last_newline = Some(pos + i as u64);
                break;
            }
        }

        if last_newline.is_some() {
            break;
        }
    }

    if let Some(nl_pos) = last_newline {
        println!("✅ Found last newline at byte offset: {}", nl_pos);
        Ok(nl_pos + 1)
    } else {
        println!("⚠️ No newline found in file");
        Ok(0)
    }
}







pub fn find_last_data_offset_non_binary_search(path: &str) -> io::Result<u64> {
    const BLOCK_SIZE: usize = 8192;

    let file_path = Path::new(path);

    if !file_path.exists() {
        eprintln!("⚠️ File not found: {}", path);
        return Ok(0);
    }

    let mut file = File::open(file_path)?;
    let filesize = file.metadata()?.len();

    if filesize == 0 {
        eprintln!("⚠️ File is empty: {}", path);
        return Ok(0);
    }

    let mut buf = vec![0u8; BLOCK_SIZE];
    let mut pos = filesize;
    let mut last_newline: Option<u64> = None;

    // Step backwards in blocks
    while pos > 0 {
        let block_size = BLOCK_SIZE.min(pos as usize);
        pos -= block_size as u64;

        //07-07-2025 : handle cases wherein \n sits right
        //between blocks
        if pos > 0 {
            // Overlap by 1 byte (so we don't miss a newline on a block boundary)
            pos -= 1;
        }
        // till here

        file.seek(SeekFrom::Start(pos))?;
        file.read_exact(&mut buf[..block_size])?;

        //improve perfomance by checking if block starts wih utf-8, if not
        //skip the block, as that would mean that block is empty anyways
        
        //07-07-2025 : Check if block starts with valid UTF-8 char
        let first_utf8 = std::str::from_utf8(&buf[..4]); // read up to 4 bytes for a UTF-8 char
        if first_utf8.is_err() {
            continue; // Not UTF-8 → skip block
        }
        // till here

        // Search from end of buffer to start
        for i in (0..block_size).rev() {
            if buf[i] == b'\n' {
                last_newline = Some(pos + i as u64);
                break;
            }
        }

        if last_newline.is_some() {
            break;
        }
    }

    if let Some(nl_pos) = last_newline {
        println!("✅ Found last newline at byte offset: {}", nl_pos);
        Ok(nl_pos + 1)
    } else {
        println!("⚠️ No newline found in file");
        Ok(0)
    }
}



    pub fn get_config_path() -> PathBuf {
        let mut current_dir = match env::current_exe()
            .ok()
            .and_then(|p| p.parent().map(Path::to_path_buf))
        {
            Some(dir) => dir,
            None => {
                eprintln!("Error: Cannot determine current executable path.");
                process::exit(1);
            }
        };

        loop {
            let config_path = current_dir.join("config/jbloxsettings.toml");
            if config_path.exists() {
                return config_path;
            }

            // Try accessing parent
            match fs::metadata(&current_dir) {
                Ok(_) => {
                    if !current_dir.pop() {
                        break; // Reached root
                    }
                }
                Err(_) => {
                    eprintln!("Error: Access denied or unreadable directory: {}", current_dir.display());
                    break;
                }
            }
        }

        eprintln!("Error: Could not find 'config/settings.toml' in current or any parent directory.");
        process::exit(1);
    }

    pub fn load_existing_file(
        initfilesize:f64,
        filesizemultiplier:usize,
        filepath: &str,
        recorddelimiter: &str,
        indexdelimiter: &str,
        indexnamevaluedelimiter: &str,
        enableviewdelete: bool,
        file_len_map: &mut HashMap<String, usize>,
        file_size_map: &mut HashMap<String, usize>,
        file_mmap_map: &mut HashMap<String, memmap2::MmapMut>,
        file_line_map: &mut HashMap<String, Vec<PtrWrapper>>,
        file_key_pointer_map: &mut HashMap<String, HashMap<String, HashMap<String, Vec<PtrWrapper>>>>,        
    ) -> std::io::Result<()> {
            let path = Path::new(filepath);
            let mut sizetobeused = (initfilesize*1000000.0); 

            // Get current file size (in bytes)
            let current_file_size = fs::metadata(path)
                .map(|meta| meta.len() as f64) // Convert u64 to f64 for comparison
                .unwrap_or(0.0); // If metadata fails, assume size 0

            // Calculate initfilesize * 1_000_000.0
            let init_size = initfilesize * 1_000_000.0;

            // Use the larger of the two
            let mut sizetobeused = current_file_size.max(init_size);

            if path.extension().and_then(|e| e.to_str()) == Some("jblox") {

                let fileactualsize = find_last_data_offset(filepath)?;
                //let fileactualsize = find_last_data_offset_non_binary_search(filepath)?;
                

                let fname = path.file_stem().unwrap().to_string_lossy().to_string();
                let file = OpenOptions::new().read(true).write(true).open(&path)?;
                let metadata = file.metadata()?;
                //get existing file size

                let ratio = fileactualsize as f64/sizetobeused as f64;

                let decimal_part = ratio.fract();
                if(decimal_part) > 0.5{
                    sizetobeused = fileactualsize as f64 * filesizemultiplier as f64;
                }

                file.set_len(sizetobeused as u64)?;
                let mut mmap = unsafe { MmapOptions::new().len(sizetobeused as usize).map_mut(&file)? };
                let len = mmap.len();
                let base = PtrWrapper(UnsafeCell::new(mmap.as_mut_ptr()));
                let mut lines = vec![base.clone()];
                let mut keymap: HashMap<String, HashMap<String, Vec<PtrWrapper>>> = HashMap::new();

                for i in 0..len {
                    let curr = PtrWrapper(UnsafeCell::new(base.add(i)));
                    if unsafe { *curr.as_ptr() } == b'\n' && i + 1 < len {
                        let next = PtrWrapper(UnsafeCell::new(base.add(i + 1)));
                        lines.push(next.clone());
                        let prev = lines[lines.len() - 2].clone();
                        let slice = unsafe {
                            std::slice::from_raw_parts(prev.as_ptr(), curr.offset_from(&prev) as usize)
                        };
                        if let Ok(text) = std::str::from_utf8(slice) {
                            let t = text.trim_end_matches('\n');
                            if enableviewdelete || t.starts_with("00") {
                                if let Some(a) = t.find(recorddelimiter) {
                                    if let Some(b) = t[a + 1..].find(recorddelimiter) {
                                        let field = t[a + 1..a + 1 + b].trim();
                                        for part in field.split(indexdelimiter) {
                                            if let Some(p) = part.find(indexnamevaluedelimiter) {
                                                let k = &part[..p];
                                                let v = &part[p + 1..];
                                                keymap.entry(k.to_string()).or_default()
                                                    .entry(v.to_string()).or_default()
                                                    .push(prev.clone());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Last line
                if let Some(last) = lines.last().cloned() {
                    let slice = unsafe {
                        std::slice::from_raw_parts(
                            last.as_ptr(),
                            len - (last.as_ptr() as usize - mmap.as_ptr() as usize),
                        )
                    };
                    if let Ok(text) = std::str::from_utf8(slice) {
                        let t = text.trim_end_matches('\n');
                        if enableviewdelete || t.starts_with("00") {
                            if let Some(a) = t.find(recorddelimiter) {
                                if let Some(b) = t[a + 1..].find(recorddelimiter) {
                                    let field = t[a + 1..a + 1 + b].trim();
                                    for part in field.split(indexdelimiter) {
                                        if let Some(p) = part.find(indexnamevaluedelimiter) {
                                            let k = &part[..p];
                                            let v = &part[p + 1..];
                                            keymap.entry(k.to_string()).or_default()
                                                .entry(v.to_string()).or_default()
                                                .push(last.clone());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                file_mmap_map.insert(fname.clone(), mmap);
                file_len_map.insert(fname.clone(), len);
                file_size_map.insert(fname.clone(), fileactualsize as usize);
                file_line_map.insert(fname.clone(), lines);
                file_key_pointer_map.insert(fname.clone(), keymap);


            }
            Ok(())
    }  

impl jbothandler {

    pub fn new() -> io::Result<Self> {
        //get config file path
        let config_path = get_config_path();
        println!("Config file path: {}",config_path.to_str().unwrap());
        let config = Config::builder()
            .add_source(config::File::with_name(config_path.to_str().unwrap()))
            .build()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Config build error: {}", e)))?;

        let settings: Settings = config
            .try_deserialize()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Config deserialize error: {}", e)))?;

        let initfilesize = settings.initfilesize;

        let newfilesizemultiplier = settings.newfilesizemultiplier;

        //directory to be used to store data
        let datadir = settings.datadir.clone();
        //log directory
        let logdir = settings.logdir.clone();
        //log frequency in hours
        let logmaxlines: usize = settings.logmaxlines;
        //indexnamevalue delimiter
        let indexnamevaluedelimiter: char = settings.indexnamevaluedelimiter.clone().to_string().chars().next().unwrap_or('-'); //'-'
        //index delimiter
        let indexdelimiter: char = settings.indexdelimiter.clone().to_string().chars().next().unwrap_or('`');//'`'
        //record delimiter
        let recorddelimiter: char = settings.recorddelimiter.clone().to_string().chars().next().unwrap_or(':');//':'

        let repindexnamevaluedelimiter: char = settings.recorddelimiter.clone().to_string().chars().next().unwrap_or('_');
        let repindexdelimiter: char = settings.recorddelimiter.clone().to_string().chars().next().unwrap_or('_');
        let reprecorddelimiter: char = settings.recorddelimiter.clone().to_string().chars().next().unwrap_or('_');
        
        let mut log_line_count: usize = 0;

        let log_filename = format!("{}/jblox.log", logdir);
        println!("log_filename : {}",log_filename);
        let log_file = BufWriter::new(
            OpenOptions::new().create(true).append(true).open(&log_filename)?
        );
        let log_path = std::path::Path::new(&log_filename);
        if log_path.exists() {
            if let Ok(logfileforlines) = File::open(log_path) {
                let reader = BufReader::new(logfileforlines);

                log_line_count = reader.lines().count();
            }
        }

        let mut file_len_map = HashMap::new();
        let mut file_size_map = HashMap::new();
        let mut file_mmap_map = HashMap::new();
        let mut file_line_map: HashMap<String, Vec<PtrWrapper>> = HashMap::new();
        let mut file_key_pointer_map = HashMap::new();

        for entry in std::fs::read_dir(datadir.clone())? {
            let path = entry?.path(); 
                load_existing_file(settings.initfilesize,
                settings.newfilesizemultiplier,
                          &path.to_string_lossy().to_string(), 
                                    &recorddelimiter.to_string(), 
                                    &indexdelimiter.to_string(), 
                                    &indexnamevaluedelimiter.to_string(),
                                    settings.enableviewdelete,
                                    &mut file_len_map,
                                    &mut file_size_map,
                                    &mut file_mmap_map,
                                    &mut file_line_map,
                                    &mut file_key_pointer_map,);

        }

        let state = Arc::new(Mutex::new(SharedState {
            file_mmap_map,
        }));
        print!("log dir: {}",logdir);
        print!("datadir dir: {}",datadir);
        Ok(Self {settings,
                file_len_map, 
                file_size_map, 
                file_line_map, 
                file_key_pointer_map, 
                datadir, 
                logdir, 
                log_file,
                log_line_count,
                state,})
    }

    pub fn append_line_and_track(&mut self, file_name: &str, new_line: &str, timestamp: &str) -> std::io::Result<()> {
        use std::fs::OpenOptions;
        use std::io::{Seek, SeekFrom, Write};
        use std::path::PathBuf;
        use memmap2::MmapMut;

        let file_path = PathBuf::from(&self.datadir).join(format!("{}.jblox", file_name));
        let updated_line = format!("00-{}:{}", timestamp, new_line);
        let line_bytes = updated_line.as_bytes();
        let line_len = line_bytes.len() + 1;

        let mut state = self.state.lock().unwrap();
        let mut file_mmap_map_t = &mut state.file_mmap_map;
        // Step 1: If mmap not initialized, initialize mapping and metadata
        if !file_mmap_map_t.contains_key(file_name) {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&file_path)?;

                load_existing_file(self.settings.initfilesize,
                self.settings.newfilesizemultiplier,
                          &file_path.to_string_lossy().to_string(), 
                                    &self.settings.recorddelimiter.to_string(), 
                                    &self.settings.indexdelimiter.to_string(), 
                                    &self.settings.indexnamevaluedelimiter.to_string(),
                                    self.settings.enableviewdelete,
                                    &mut self.file_len_map,
                                    &mut self.file_size_map,
                                    &mut file_mmap_map_t,
                                    &mut self.file_line_map,
                                    &mut self.file_key_pointer_map,);

        }
        //file actual size
        let fileactualsize = *self.file_size_map.get(file_name).unwrap();
        let allowedfilesize = *self.file_len_map.get(file_name).unwrap();
        
        let ratio = fileactualsize as f64/allowedfilesize as f64;

        let decimal_part = (ratio.fract() * 100.0).round() / 100.0;

        // Step 3: Write using mmap
        let mmap = file_mmap_map_t.get_mut(file_name).unwrap();
        let start = *self.file_size_map.get_mut(file_name).unwrap();
        let end = start + line_bytes.len();
        if end > mmap.len() {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Write exceeds mmap size"));
        }
        mmap[start..end].copy_from_slice(line_bytes);
        mmap[end] = b'\n';
        *self.file_size_map.get_mut(file_name).unwrap() += line_len;

        //increment the file size in 
        let new_ptr = PtrWrapper::new(unsafe { mmap.as_mut_ptr().add(start) });
        self.file_line_map.get_mut(file_name).unwrap().push(new_ptr.clone());


        // Step 4: Extract keys and update map
        if let Some(key_map) = self.file_key_pointer_map.get_mut(file_name) {
            let mut parts = updated_line.splitn(3, self.settings.recorddelimiter);
            parts.next(); // skip timestamp
            if let Some(key) = parts.next() {
                for part in key.trim().split(self.settings.indexdelimiter) {
                    if let Some(dash_pos) = part.find(self.settings.indexnamevaluedelimiter) {
                        let key_name = &part[..dash_pos];
                        let key_value = &part[dash_pos + 1..];

                        key_map
                            .entry(key_name.to_string())
                            .or_default()
                            .entry(key_value.to_string())
                            .or_default()
                            .push(new_ptr.clone());
                    }
                }
            }
        }
        if(decimal_part > 0.98){
            //end process for the integrity of the database
                eprintln!("⚠️  ERROR: data file size {}%, stopping jbloxbd.",decimal_part*100.00);
                process::exit(0); // Exit with status code 0 (success)
        }
        else if(decimal_part > 0.85){
            //send waring to the admin to restart for remapping
            eprintln!("⚠️  Warning data file size {}%, restart jbloxdb to increase allowed size.",decimal_part*100.00);
            eprintln!("⚠️  Warning data file size {}%, Process will stop automatically when file size reaches 98%.",decimal_part*100.00);
        }  
        Ok(())
    }

    pub fn print_line_by_pointer(&self, file_name: &str, ptrw: PtrWrapper) {
        let mut state = self.state.lock().unwrap();
        let mut file_mmap_map_t = &mut state.file_mmap_map;
        let ptr = ptrw.as_ptr();
        if let (Some(lines), Some(mmap), Some(&len)) = (
            self.file_line_map.get(file_name),
            file_mmap_map_t.get(file_name),
            self.file_len_map.get(file_name)
        ) {
            let idx = lines.binary_search_by(|w| unsafe {
                let p = w.as_ptr();
                if p <= ptr { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater }
            }).unwrap_or_else(|i| i);
            let start = ptr;
            let end = if idx+1 < lines.len() {
                lines[idx+1].as_ptr()
            } else {
                unsafe { mmap.as_ptr().add(len) as *mut u8 }
            };
            let slice = unsafe { std::slice::from_raw_parts(start, end.offset_from(start) as usize) };
            if let Ok(text) = std::str::from_utf8(slice) {
                println!("Line: {}", text.trim_end_matches('\n'));
            }
        }
    }

    pub fn print_between(&self, start_ptr: *mut u8, end_ptr: *mut u8) {
        let len = unsafe { end_ptr.offset_from(start_ptr) as usize };
        let slice = unsafe { std::slice::from_raw_parts(start_ptr as *const u8, len) };

        match std::str::from_utf8(slice) {
            Ok(text) => println!("Text between: {}", text),
            Err(_) => eprintln!("Invalid UTF-8 between pointers"),
        }
    }

    pub fn print_line_forpointer(&self, start_ptr: PtrWrapper, includedelete: bool) -> std::io::Result<String> {
        let max_len: usize = self.settings.maxrecordlength;
        let mut bytes = Vec::new();

        for i in 0..max_len {
            unsafe {
                let ch = *start_ptr.add(i);
                if ch == b'\n' {
                    // Convert collected bytes to UTF-8 string
                    let line = match std::str::from_utf8(&bytes) {
                        Ok(text) => text,
                        Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
                    };

                    //Only return if line starts with "00"
                    //if includedelete is not true
                    if includedelete && self.settings.enableviewdelete {
                        return Ok(line.to_string());
                    } 
                    else if line.starts_with("00") {
                        return Ok(line.to_string());
                    } else {
                        return Ok("".to_string());
                    }
                } else {
                    bytes.push(ch);
                }
            }
        }

        // EOF reached without newline — try to process what we have
        if !bytes.is_empty() {
            let line = std::str::from_utf8(&bytes).map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, e)
            })?;

            if line.starts_with("00") {
                return Ok(line.to_string());
            }
        }

        Ok("".to_string())
    }

pub fn insert_duplicate_frmObject(&mut self, json: &Value,timestamp: &str) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind};

    let file_name = json.get("keyobj")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "Missing or invalid 'keyobj'"))?;

    if let Some(obj) = json.as_object() {
        if let Some(Value::String(_key_name)) = obj.get("key") {
            // Step 2: Extract key value from JSON
            let key_str = {
                let pairs = self.extract_key_value_multiple(&json);
                let parts: Vec<String> = pairs
                    .into_iter()
                    .map(|(key, value)| {
                        let val_str = match value {
                            Value::String(s) => s.clone(),
                            _ => value.to_string().trim_matches('"').to_string(),
                        };
                        //replace indexnamevaluedelimiter with repindexnamevaluedelimiter
                        let mut key_t = key.clone().to_string();
                        let mut val_str_t = val_str.clone();
                        if(key_t.contains(self.settings.indexnamevaluedelimiter)){
                            key_t = key_t.replace(self.settings.indexnamevaluedelimiter,&self.settings.repindexnamevaluedelimiter.to_string());
                        }
                        if(key_t.contains(self.settings.indexdelimiter)){
                            key_t = key_t.replace(self.settings.indexdelimiter,&self.settings.repindexdelimiter.to_string());
                        }
                        if(key_t.contains(self.settings.recorddelimiter)){
                            key_t = key_t.replace(self.settings.recorddelimiter,&self.settings.reprecorddelimiter.to_string());
                        }
                        if(val_str_t.contains(self.settings.indexnamevaluedelimiter)){
                            val_str_t = val_str_t.replace(self.settings.indexnamevaluedelimiter,&self.settings.repindexnamevaluedelimiter.to_string());
                        }
                        if(val_str_t.contains(self.settings.indexdelimiter)){
                            val_str_t = val_str_t.replace(self.settings.indexdelimiter,&self.settings.repindexdelimiter.to_string());
                        }
                        if(val_str_t.contains(self.settings.recorddelimiter)){
                            val_str_t = val_str_t.replace(self.settings.recorddelimiter,&self.settings.reprecorddelimiter.to_string());
                        }
                        format!("{}{}{}", key_t,self.settings.indexnamevaluedelimiter, val_str_t)
                    })
                    .collect();
                parts.join(&self.settings.indexdelimiter.to_string())
            };

            // Step 4: Minify JSON and prepare line
            let new_line = format!("{}:{}", key_str, json);

            // Step 5: Append to file
            self.append_line_and_track(file_name, &new_line,timestamp)?;
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid or missing 'key' or key value in JSON",
            ));
        }
    } else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "JSON input is not an object",
        ));
    }

    Ok(())
}


    pub fn insertduplicate(&mut self, json_str: &str) ->std::io::Result<Vec<String>> {
        use std::io::{Error, ErrorKind};
        let currtstmp:String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-insertduplicate-{}",currtstmp, compact_jsonstr))?;

        // Do not check for duplicates
        return self.insert_main(json_str, false, &currtstmp);
    }

    pub fn insert(&mut self, json_str: &str) ->std::io::Result<Vec<String>> {
        use std::io::{Error, ErrorKind};
        
        let currtstmp:String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-insert-{}",currtstmp, compact_jsonstr))?;

        // To check for duplicates (default behavior)
        return self.insert_main(json_str, true, &currtstmp);
    }

    pub fn insert_main(&mut self, json_str: &str, check_duplicates: bool, timestamp: &str) -> std::io::Result<Vec<String>> {
        use std::io::{Error, ErrorKind};
        
        let currtstmp:String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;

        // Step 1: Parse the input JSON string into a Value.
        let json: Value = serde_json::from_str(&compact_jsonstr)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let file_name = json.get("keyobj")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "Missing or invalid 'keyobj'"))?;

        if let Some(obj) = json.as_object() {
            if let Some(Value::String(key_name)) = obj.get("key") {

                //check duplicates
                //get key->keyvalue map
                let key_map = self.extract_keyname_value_map(&json)?;    
                // Call the existing method

                if check_duplicates {
                    //let key_map = self.extract_keyname_value_map(&json)?;
                    if let Ok(ptrs) = self.getmain(json_str,true,false,false) {
                        if !ptrs.is_empty() {
                            print!("Duplicate Records");
                            return Err(Error::new(ErrorKind::Other, "Duplicate Record."));
                        }
                    }
                }
                // Step 2: Extract key value from JSON
                let key_str = {
                    let pairs = self.extract_key_value_multiple(&json);
                    let parts: Vec<String> = pairs
                        .into_iter()
                        .map(|(key, value)| {
                            let val_str = match value {
                                Value::String(s) => s.clone(),
                                _ => value.to_string().trim_matches('"').to_string(),
                            };
                            //replace indexnamevaluedelimiter with repindexnamevaluedelimiter
                            let mut key_t = key.clone().to_string();
                            let mut val_str_t = val_str.clone();
                            if(key_t.contains(self.settings.indexnamevaluedelimiter)){
                                key_t = key_t.replace(self.settings.indexnamevaluedelimiter,&self.settings.repindexnamevaluedelimiter.to_string());
                            }
                            if(key_t.contains(self.settings.indexdelimiter)){
                                key_t = key_t.replace(self.settings.indexdelimiter,&self.settings.repindexdelimiter.to_string());
                            }
                            if(key_t.contains(self.settings.recorddelimiter)){
                                key_t = key_t.replace(self.settings.recorddelimiter,&self.settings.reprecorddelimiter.to_string());
                            }
                            if(val_str_t.contains(self.settings.indexnamevaluedelimiter)){
                                val_str_t = val_str_t.replace(self.settings.indexnamevaluedelimiter,&self.settings.repindexnamevaluedelimiter.to_string());
                            }
                            if(val_str_t.contains(self.settings.indexdelimiter)){
                                val_str_t = val_str_t.replace(self.settings.indexdelimiter,&self.settings.repindexdelimiter.to_string());
                            }
                            if(val_str_t.contains(self.settings.recorddelimiter)){
                                val_str_t = val_str_t.replace(self.settings.recorddelimiter,&self.settings.reprecorddelimiter.to_string());
                            }                          
                            format!("{}{}{}", key_t,self.settings.indexnamevaluedelimiter.to_string(), val_str_t)
                        })
                        .collect();
                    parts.join(&self.settings.indexdelimiter.to_string())
                };
                // Step 4: Minify JSON and prepare line

                let new_line = format!("{}:{}", key_str, json);

                // Step 5: Append to file
                self.append_line_and_track(file_name, &new_line,timestamp)?;
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid or missing 'key' or key value in JSON",
                ));
            }
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "JSON input is not an object",
            ));
        }
                    
        Ok(vec!["done".to_string()])
    }


fn extract_specific_key_value<'a>(&self, json: &'a Value, keyname: &str) -> Option<&'a Value> {
    if let Some(obj) = json.as_object() {
        if let Some(value) = obj.get(keyname) {
            return Some(value);
        } else {
            return self.find_key_recursively(json, keyname);
        }
    }
    None
}

fn extract_keyobj_value<'a>(&self, json: &'a Value) -> Option<(&'a str, &'a Value)> {
    if let Some(obj) = json.as_object() {
        if let Some(value) = obj.get("keyobj") {
            return Some(("keyobj", value));
        }
    }
    None
}

    fn extract_key_value_multiple<'a>(&self, json: &'a Value) -> Vec<(&'a str, &'a Value)> {
        let mut results = Vec::new();

        if let Some(obj) = json.as_object() {
            if let Some(Value::String(key_names)) = obj.get("key") {
                for key_name_str in key_names.split(',').map(|s| s.trim()) {
                    if let Some(value) = obj.get(key_name_str) {
                        results.push((key_name_str, value));
                    } else if let Some(found_value) = self.find_key_recursively(json, key_name_str) {
                        results.push((key_name_str, found_value));
                    }
                }
            }
        }

        results
    }

    fn extract_key_value<'a>(&self, json: &'a Value) -> Option<(&'a str, &'a Value)> {
        if let Some(obj) = json.as_object() {
            if let Some(Value::String(key_name)) = obj.get("key") {
                let key_name_str = key_name.as_str();

                if let Some(value) = obj.get(key_name_str) {
                    return Some((key_name_str, value));
                } else if let Some(found_value) = self.find_key_recursively(json, key_name_str) {
                    return Some((key_name_str, found_value));
                }
            }
        }
        None
    }   

    fn find_key_recursively<'a>(&self, json: &'a Value, key: &str) -> Option<&'a Value> {
        match json {
            Value::Object(map) => {
                if let Some(val) = map.get(key) {
                    return Some(val);
                }
                for (_k, v) in map {
                    if let Some(found) = self.find_key_recursively(v, key) {
                        return Some(found);
                    }
                }
            }
            Value::Array(arr) => {
                for v in arr {
                    if let Some(found) = self.find_key_recursively(v, key) {
                        return Some(found);
                    }
                }
            }
            _ => {}
        }
        None
    }

    pub fn delete(&mut self, json_str: &str) ->std::io::Result<Vec<String>> {
        let currtstmp: String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-delete-{}", currtstmp, compact_jsonstr))?;

        return self.deletemain(json_str, false); // delete last
    }

    pub fn deleteall(&mut self, json_str: &str) ->std::io::Result<Vec<String>> {

        let currtstmp: String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-deleteall-{}", currtstmp, compact_jsonstr))?;
 
       return self.deletemain(json_str, true); // delete all
    }

    pub fn deletemain(&mut self, json_str: &str, delete_all: bool) ->std::io::Result<Vec<String>> {
        use std::io::{Error, ErrorKind};

        // Step 1: Parse the input JSON string
        let json: Value = serde_json::from_str(json_str)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;


        // Step 3: Validate JSON is an object
        if !json.is_object() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "JSON input is not an object",
            ));
        }

        let file_name = json.get("keyobj")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "Missing or invalid 'keyobj'"))?;

        // Step 4: Extract key-value map from JSON
        let key_map = self.extract_keyname_value_map(&json)?;

        // Step 5: Get matching pointers
        if let Some(ptrs) = self.get_fromkey_onlypointers(file_name, &key_map) {
            if ptrs.is_empty() {
                println!("Pointer list is empty.");
            } else if delete_all {
                for &ptr in &ptrs {
                    self.delete_using_pointer_fordelete(file_name, ptr);
                }
                println!("Deleted {} records.", ptrs.len());
            } else {
                if let Some(&farthest_ptr) = ptrs.iter().max_by_key(|&&p| p as usize) {
                    self.delete_using_pointer_fordelete(file_name, farthest_ptr);
                    println!("Deleted last record.");
                }
            }
        } else {
            println!("No Record found.");
        }

        let lines = vec![
            String::from("done")
        ];
        Ok(lines)
    }

    pub fn delete_using_pointer_fordelete(&mut self, file_name: &str, ptr: *mut u8) {
        // Get mutable mmap for the file
        let mut state = self.state.lock().unwrap();
        let file_mmap_map_t = &mut state.file_mmap_map;
        if let Some(mmap) = file_mmap_map_t.get_mut(file_name) {
            let mmap_start = mmap.as_mut_ptr();
            let mmap_len = mmap.len();

            unsafe {
                // Overwrite first byte at the pointer
                *ptr = b'2'; // Null byte if preferred
            }
        } else {
            eprintln!("No mmap found for file '{}'", file_name);
        }
    }

    pub fn delete_using_pointer_forupdate(&mut self, file_name: &str, ptr: *mut u8) {
        let mut state = self.state.lock().unwrap();
        let file_mmap_map_t = &mut state.file_mmap_map;
        // Get mutable mmap for the file
        if let Some(mmap) = file_mmap_map_t.get_mut(file_name) {
            let mmap_start = mmap.as_mut_ptr();
            let mmap_len = mmap.len();

            unsafe {
                // Overwrite first byte at the pointer
                *ptr = b'1'; // Null byte if preferred
            }
        } else {
            eprintln!("No mmap found for file '{}'", file_name);
        }
    }
    
    pub fn update(&mut self, json_str: &str) ->std::io::Result<Vec<String>>{
        let currtstmp:String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-update-{}",currtstmp, compact_jsonstr))?;

        return self.updatemain(json_str, false,&currtstmp); // delete last
    }

    pub fn updateall(&mut self,json_str: &str) ->std::io::Result<Vec<String>> {
        let currtstmp:String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-updateall-{}",currtstmp, compact_jsonstr))?;

       return self.updatemain(json_str, true,&currtstmp); // delete all
    }

    pub fn updatemain(&mut self, json_str: &str, update_all: bool, timestamp: &str) ->std::io::Result<Vec<String>> {
        use std::io::{Error, ErrorKind};


        // Step 1: Parse the input JSON string into a Value.
        let json: Value = serde_json::from_str(json_str)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        // Step 3: Validate JSON is an object
        if !json.is_object() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "JSON input is not an object",
            ));
        }

        let file_name = json.get("keyobj")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "Missing or invalid 'keyobj'"))?;

        // Step 4: Extract key-value map from JSON
        let key_map = self.extract_keyname_value_map(&json)?;

        // Step 5: Get matching pointers
        if let Some(ptrs) = self.get_fromkey_onlypointers(file_name, &key_map) {
            if ptrs.is_empty() {
                println!("Pointer list is empty.");
            } else if update_all {
                for &ptr in &ptrs {
                    self.delete_using_pointer_forupdate(file_name, ptr);
                }
                println!("Deleted {} records.", ptrs.len());
            } else {
                if let Some(&farthest_ptr) = ptrs.iter().max_by_key(|&&p| p as usize) {
                    self.delete_using_pointer_forupdate(file_name, farthest_ptr);
                    println!("Deleted last record.");
                }
            }
            self.insert_duplicate_frmObject(&json, timestamp);
        } else {
            println!("No Record found.");
        }

        let keyname = json.get("key")
            .and_then(|v| v.as_str())
            .and_then(|s| s.split(',').next())
            .map(str::trim)
            .ok_or_else(|| std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "'key' field missing or invalid in JSON",
            ))?;

        let lines = vec![
            String::from("done")
        ];
        Ok(lines)
    }



    pub fn gethistall(&mut self, json_str: &str) -> std::io::Result<Vec<String>> {
        if !self.settings.enableviewdelete {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied, // Use PermissionDenied or Other
                "EnableViewDelete feature not enabled.",
            ));
        }
        let currtstmp:String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-gethistall-{}",currtstmp, compact_jsonstr))?;

       return self.getmain(&json_str, false,true,false);  
    }

    pub fn gethist(&mut self, json_str: &str) -> std::io::Result<Vec<String>> {
        if !self.settings.enableviewdelete {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied, // Use PermissionDenied or Other
                "EnableViewDelete feature not enabled.",
            ));
        }
        
        let currtstmp:String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-gethist-{}",currtstmp, compact_jsonstr))?;
    

       return self.getmain(&json_str, true,true,false);  
    }


    pub fn gethistalldesc(&mut self, json_str: &str) -> std::io::Result<Vec<String>> {
        if !self.settings.enableviewdelete {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied, // Use PermissionDenied or Other
                "EnableViewDelete feature not enabled.",
            ));
        }
        let currtstmp:String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-gethistalldesc-{}",currtstmp, compact_jsonstr))?;

       return self.getmain(&json_str, false,true,true);  
    }


    pub fn gethistdesc(&mut self, json_str: &str) -> std::io::Result<Vec<String>> {
        if !self.settings.enableviewdelete {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied, // Use PermissionDenied or Other
                "EnableViewDelete feature not enabled.",
            ));
        }
        let currtstmp:String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-gethistdesc-{}",currtstmp, compact_jsonstr))?;

       return self.getmain(&json_str, true,true,true);  
    }  

    pub fn get(&mut self, json_str: &str) -> std::io::Result<Vec<String>> {
        let currtstmp:String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-get-{}",currtstmp, compact_jsonstr))?;

        return self.getmain(&json_str,  true,false,false);  
    }

    pub fn getall(&mut self, json_str: &str) -> std::io::Result<Vec<String>> {
        let currtstmp:String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-getall-{}",currtstmp, compact_jsonstr))?;

       return self.getmain(&json_str, false,false,false);  
    }

    pub fn getdesc(&mut self, json_str: &str) -> std::io::Result<Vec<String>> {
        let currtstmp:String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-getdesc-{}",currtstmp, compact_jsonstr))?;

        return self.getmain(&json_str,  true,false,true);  
    }
    
    pub fn getalldesc(&mut self, json_str: &str) -> std::io::Result<Vec<String>> {
        let currtstmp:String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-getalldesc-{}",currtstmp, compact_jsonstr))?;

       return self.getmain(&json_str, false,false,true);  
    }

  
    //apply AND between the keys
    pub fn getmain(&mut self, 
                    json_str: &str,
                    use_intersection: bool,
                    includedelete: bool,
                    reverse: bool,) -> std::io::Result<Vec<String>> {

        use std::io::{Error, ErrorKind};
        use serde_json::Value;

        // Parse the JSON string
        let json: Value = serde_json::from_str(json_str)
            .map_err(|e| Error::new(ErrorKind::InvalidInput, format!("Invalid JSON: {}", e)))?;

        // Ensure it's an object
        let obj = json.as_object().ok_or_else(|| {
            Error::new(ErrorKind::InvalidInput, "Expected JSON object at root")
        })?;

        let file_name = obj.get("keyobj")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "Missing or invalid 'keyobj'"))?;
        
        //paging implementation for get
        let recstart = obj.get("recstart").and_then(Value::as_str).unwrap_or("0");

        //get key->keyvalue map
        let key_map = self.extract_keyname_value_map(&json)?;    
        // Call the existing method
        let lines = self.get_fromkey(file_name, &key_map, &recstart, use_intersection,includedelete,reverse);
        Ok(lines)

    }

    pub fn get_fromkey(
        &mut self,
        file_name: &str,
        key_map: &HashMap<String, String>,
        recstart: &str, //if "0" send records from begining, else start from this record (including)
        use_intersection: bool,
        includedelete: bool,
        reverse: bool,   //send records in decending
    ) -> Vec<String> {

        let mut result: Vec<PtrWrapper> = Vec::new(); // ✅ Flat vector
        let mut found_keystart = recstart == "0";

        if !use_intersection{
            let mut seen = HashSet::new();

            for (keyname, keyvalue) in key_map {
                if let Some(ptrmap) = self.file_key_pointer_map.get(file_name) {
                    if let Some(val_map) = ptrmap.get(keyname) {
                        if let Some(ptrs) = val_map.get(keyvalue) {
                            for ptr in ptrs {
                                let key = ptr.as_ptr(); // Use raw pointer for uniqueness
                                if seen.insert(key) {
                                    result.push(ptr.clone());
                                }
                            }
                        }
                    }
                }
            }

        }
        else{
            let mut is_first = true;

            for (keyname, keyvalue) in key_map {
                if let Some(ptrmap) = self.file_key_pointer_map.get(file_name) {
                    if let Some(val_map) = ptrmap.get(keyname) {
                        if let Some(ptrs) = val_map.get(keyvalue) {
                            if ptrs.is_empty() {
                                result.clear(); // Found empty ptrs -> clear result
                                break;
                            }
                            if is_first {
                                result = ptrs.iter().cloned().collect();
                                is_first = false;
                            } else {
                                result.retain(|ptr| {
                                    ptrs.iter().any(|p| p.as_ptr() == ptr.as_ptr())
                                });
                                if result.is_empty() {
                                    break; // Intersection became empty -> stop
                                }
                            }
                        } else {
                            result.clear(); // keyvalue missing -> clear result
                            break;
                        }
                    } else {
                        result.clear(); // keyname missing -> clear result
                        break;
                    }
                } else {
                    result.clear(); // ptrmap missing -> clear result
                    break;
                }
            }
        }

        if(reverse){
            result.sort_by_key(|ptr| std::cmp::Reverse(ptr.as_ptr() as usize));
        }
        else{
            result.sort_by_key(|ptr| ptr.as_ptr() as usize);
        }

        let mut result_lines: Vec<String> = Vec::with_capacity(self.settings.maxgetrecords);

        for ptr in result {
            if result_lines.len() >= self.settings.maxgetrecords {
                break;
            }
            if let Ok(line) = self.print_line_forpointer(ptr,includedelete) {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                if !found_keystart {
                    if line.starts_with(&format!("{}", recstart)) {
                        found_keystart = true;
                        result_lines.push(line.to_string());
                    }
                } else {
                    result_lines.push(line.to_string());
                }
            }
        }

        if result_lines.is_empty() {
            //eprintln!("No matching records found.");
            return Vec::new();
        }

        //println!("Got the matching pointers");

        result_lines
    }


    pub fn get_fromkey_onlypointers(
        &self,
        file_name: &str,
        key_map: &HashMap<String, String>,
    ) -> Option<Vec<*mut u8>> {
        // Step 1: Gather all pointer lists for each keyname-value pair
        let mut sets: Vec<HashSet<&PtrWrapper>> = Vec::new();

        for (keyname, keyvalue) in key_map {
            let ptrs = self.file_key_pointer_map
                .get(file_name)?
                .get(keyname)?
                .get(keyvalue)?
                .iter()
                .clone()
                .collect::<HashSet<_>>();

            sets.push(ptrs);
        }

        // Step 2: Intersect all sets
        let common_ptrs = match sets.split_first() {
            Some((first, rest)) => {
                rest.iter().fold(first.clone(), |acc, s| acc.intersection(s).copied().collect())
            }
            None => return None,
        };

        if common_ptrs.is_empty() {
            None
        } else {
            Some(common_ptrs.into_iter().map(|pw| pw.as_ptr()).collect())
        }
    }

    pub fn extract_keyname_value_map(&self, json: &Value) -> std::io::Result<HashMap<String, String>> {
        let mut map = HashMap::new();

        let key_field = json.get("key")
            .and_then(|v| v.as_str())
            .ok_or_else(|| std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Missing or invalid 'key' field",
            ))?;

        for keyname in key_field.split(',').map(str::trim) {
            if keyname.is_empty() {
                continue;
            }

            let value_opt = json.get(keyname).or_else(|| self.find_key_recursively(json, keyname));

            if let Some(value) = value_opt {
                let value_str = match value {
                    Value::String(s) => s.clone(),
                    _ => value.to_string().trim_matches('"').to_string(),
                };
                //do reverse replacement
                let mut key_t = keyname.to_string();
                if(key_t.contains(self.settings.indexnamevaluedelimiter)){
                    key_t = key_t.replace(self.settings.indexnamevaluedelimiter,&self.settings.repindexnamevaluedelimiter.to_string());
                }
                if(key_t.contains(self.settings.indexdelimiter)){
                    key_t = key_t.replace(self.settings.indexdelimiter,&self.settings.repindexdelimiter.to_string());
                }
                if(key_t.contains(self.settings.recorddelimiter)){
                    key_t = key_t.replace(self.settings.recorddelimiter,&self.settings.reprecorddelimiter.to_string());
                }
                let mut value_str_t = value_str.to_string();
                if(value_str_t.contains(self.settings.indexnamevaluedelimiter)){
                    value_str_t = value_str_t.replace(self.settings.indexnamevaluedelimiter,&self.settings.repindexnamevaluedelimiter.to_string());
                }
                if(value_str_t.contains(self.settings.indexdelimiter)){
                    value_str_t = value_str_t.replace(self.settings.indexdelimiter,&self.settings.repindexdelimiter.to_string());
                }
                if(value_str_t.contains(self.settings.recorddelimiter)){
                    value_str_t = value_str_t.replace(self.settings.recorddelimiter,&self.settings.reprecorddelimiter.to_string());
                }
                map.insert(key_t, value_str_t);
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Key '{}' not found in JSON", keyname),
                ));
            }
        }

        Ok(map)
    }

    pub fn log_message(&mut self, message: &str) -> std::io::Result<()> {
        writeln!(self.log_file, "{}", message)?;
        println!("message: {}", &message.chars().take(self.settings.maxlogtoconsolelength).collect::<String>());
        self.log_file.flush()?;
        self.log_line_count += 1;

        // Check if rollover needed
        // Check if rollover is needed
        if self.log_line_count >= self.settings.logmaxlines {
            // Create timestamped backup file name
            let timestamp = Self::current_timestamp_forlog();
            let base_log_path = format!("{}/jblox.log", self.logdir);
            let backup_log_path = format!("{}/jblox_{}.log", self.logdir, timestamp);

            // Close the current file and rename it
            drop(&self.log_file); // Flush & close
            std::fs::rename(&base_log_path, &backup_log_path)?;

            // Open a new file with original name (overwrites if already exists)
            let new_log_file = BufWriter::new(
                OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&base_log_path)?
            );
            self.log_file = new_log_file;
            self.log_line_count = 0;
        }
        Ok(())
    }
        
    pub fn to_compact_json(&self, json: &Value) -> io::Result<String> {
        serde_json::to_string(json).map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("JSON serialization failed: {}", e))
        })
    }

    pub fn compact_json_str(&self, json_str: &str) -> io::Result<String> {
        let value: Value = serde_json::from_str(json_str).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("Invalid JSON: {}", e))
        })?;
        
        serde_json::to_string(&value).map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("Serialization failed: {}", e))
        })
    }
    fn current_timestamp(&self) -> String {
        let now = chrono::Local::now();
        now.format("%d%m%y%H%M%S%6f").to_string()
    }
    
    fn current_timestamp_forlog() -> String {
        Local::now().format("%d-%m-%y-%H-%M-%S-%6f").to_string()
    } 

    pub fn handle_request(&mut self, input: &str) -> std::io::Result<Vec<String>> {
        let parsed: Value = serde_json::from_str(input).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("Invalid JSON: {e}"))
        })?;

        let op = parsed.get("op").and_then(Value::as_str)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "Missing or invalid 'op'"))?;

        let data = parsed.get("data")
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "Missing 'data'"))?;

        let data_str = serde_json::to_string(data).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("Failed to serialize 'data': {e}"))
        })?;

        let ok_response = || Ok(vec!["ok".to_string()]);

        match op {
            "insertduplicate" => {
                self.insertduplicate(&data_str)?;
                ok_response()
            }
            "insert" => {
                self.insert(&data_str)?;
                ok_response()
            }
            "updateall" => {
                self.updateall(&data_str)?;
                ok_response()
            }
            "update" => {
                self.update(&data_str)?;
                ok_response()
            }
            "deleteall" => {
                self.deleteall(&data_str)?;
                ok_response()
            }
            "delete" => {
                self.delete(&data_str)?;
                ok_response()
            }
            "gethist" => self.gethist(&data_str),
            "gethistdesc" => self.gethistdesc(&data_str),
            "gethistall" => self.gethistall(&data_str),
            "gethistalldesc" => self.gethistalldesc(&data_str),
            "getall" => self.getall(&data_str),
            "get" => self.get(&data_str),
            "getalldesc" => self.getalldesc(&data_str),
            "getdesc" => self.getdesc(&data_str),
            _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("Unknown op: {op}"))),
        }
    }



}


fn run() -> io::Result<()> {
    let jsonpath: &'static str = "C:/dev/jbloxdb/jbolxWorkspace/data/json.txt";

    let mut handler = jbothandler::new()?;

    let json_input = r#"
    {
        "key": "id,name",
        "keyobj": "user",
        "id": "user12345",
        "name": "Alice james",
        "email": "alice@example.com"
    }
    "#;

    let json_input_get1 = r#"
    {
        "key": "id,name",
        "keyobj": "user",
        "id": "user12345",
        "name": "Alice james",
        "email": "alice@example.com"
    }
    "#;

    // Call the method or block you want to time
    let start = Instant::now();

    handler.insertduplicate(json_input);
    handler.insertduplicate(json_input);
    handler.insertduplicate(json_input);
    handler.insertduplicate(json_input);
    handler.insert(json_input);
    handler.updateall(json_input);
    handler.update(json_input);
    handler.deleteall(json_input);
    handler.delete(json_input);
    handler.getall(json_input_get1);
    handler.get(json_input_get1);

    let duration = start.elapsed();
    println!("Time taken: {:.2?}", duration);

    Ok(())
}