// jbloxDB
// © 2025 Green Arrowhead LLP
// Licensed under the jbloxDB License v1.0
// See LICENSE.txt for terms.
// Free for individuals and small companies.
// Commercial license required for production use by companies over USD 5M revenue or for SaaS/product distribution.
// Declare the internal module that contains the jbothandler logic
mod jbotm;

use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use serde_json::Value;
use std::fs;
use std::sync::Arc;
use jbotm::jbothandler;
use serde_json::json;

use tokio::sync::Mutex; 

use std::path::{Path};
use std::process;
use std::path::PathBuf;
use std::env;
use config::{Config, File, Environment};
use serde::Deserialize;

use std::io::{self, Error, ErrorKind};

use std::sync::{atomic::{AtomicBool, Ordering}};
use std::thread;
use std::time::Duration;

use crossterm::terminal;

#[derive(Clone,Debug, Deserialize)]
struct Settings {
    ip: String,
    port: String,
    htmldir: String,
    defaultpage:String,
    maxbuffer:usize,
}

use std::str;

fn find_headers_end(buf: &[u8]) -> Option<usize> {
    // index of the first byte of "\r\n\r\n"
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn split_header(line: &str) -> (&str, &str) {
    if let Some(i) = line.find(':') { (&line[..i], &line[i+1..]) } else { (line, "") }
}

async fn write_400(socket: &mut tokio::net::TcpStream) -> std::io::Result<()> {
    socket.write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await
}

async fn write_411(socket: &mut tokio::net::TcpStream) -> std::io::Result<()> {
    socket.write_all(b"HTTP/1.1 411 Length Required\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await
}

// Minimal chunked decoder good enough for JSON.
// If you don't expect chunked, you can skip this and just return 411.
enum ChunkParse { NeedMore, Done(Vec<u8>), Err }

fn find_line_end(s: &[u8]) -> Option<usize> {
    s.windows(2).position(|w| w == b"\r\n")
}

fn decode_chunked_try(src: &[u8]) -> ChunkParse {
    let mut i = 0usize;
    let mut out = Vec::<u8>::new();

    loop {
        let Some(end) = find_line_end(&src[i..]) else { return ChunkParse::NeedMore };
        let line = &src[i..i+end];
        let Ok(hex) = str::from_utf8(line) else { return ChunkParse::Err };
        let Ok(size) = usize::from_str_radix(hex.trim(), 16) else { return ChunkParse::Err };
        i += end + 2; // skip \r\n

        if size == 0 {
            // expect trailing CRLF
            if src.len() < i + 2 { return ChunkParse::NeedMore; }
            if &src[i..i+2] != b"\r\n" { return ChunkParse::Err; }
            return ChunkParse::Done(out);
        }

        if src.len() < i + size + 2 { return ChunkParse::NeedMore; }
        out.extend_from_slice(&src[i..i+size]);
        i += size;
        if &src[i..i+2] != b"\r\n" { return ChunkParse::Err; }
        i += 2;
    }
}

#[tokio::main]


async fn main() -> Result<(), Box<dyn std::error::Error>> {
    print_jbloxDB();
    println!("Starting jbloxDB: zero configuration; super fast JSON database.");

    //check .lck file so at to make sure that only on instance is running
    // Check for jblox.lck file
    let lock_file_path = Path::new(".").join("jblox.lck");
    if lock_file_path.exists() {
        eprintln!("jblox.lck found in current directory. Another instance might be running.");
        process::exit(1);
    }
    // Create lock file
    fs::write(&lock_file_path, "locked").map_err(|e| {
        io::Error::new(io::ErrorKind::Other, format!("Failed to create lock file: {}", e))
    })?;    

    let shutting_down = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutting_down.clone();

    let config_path = get_config_path();

    let config = Config::builder()
        .add_source(config::File::with_name(config_path.to_str().unwrap()))
        .build()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Config build error: {}", e)))?;

    let settings: Settings = config
        .try_deserialize()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Config deserialize error: {}", e)))?;


    println!("htmldir dir: {}",settings.htmldir);
    // Create a thread-safe shared instance of jbothandler wrapped in Arc and Mutex
    // 1) create the handler
    let mut handler = jbothandler::new().unwrap();
    //check data integrity
    handler.sanitycheck();
    println!("Ready to accept requests.");


    let handler = Arc::new(Mutex::new(handler));


    // Bind the TCP listener to localhost at port 3000
    let listener = TcpListener::bind(format!("{}:{}", settings.ip, settings.port)).await?;
    
    let shutdown_checker = shutdown_clone.clone();

    thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_secs(60)); // Check every 1 min

            let stop_file = std::path::Path::new(".").join("jblox.stop");
            if stop_file.exists() {
                println!("Detected jblox.stop file. Initiating shutdown...");

                shutdown_checker.store(true, Ordering::SeqCst);

                println!("Waiting 10 seconds for active requests to finish...");
                thread::sleep(Duration::from_secs(10));

                println!("Cleaning up lock and stop files...");
                let lock_file = std::path::Path::new(".").join("jblox.lck");

                if lock_file.exists() {
                    if let Err(e) = fs::remove_file(&lock_file) {
                        eprintln!("Failed to delete jblox.lck: {}", e);
                    } else {
                        println!("Deleted jblox.lck");
                    }
                }

                if let Err(e) = fs::remove_file(&stop_file) {
                    eprintln!("Failed to delete jblox.stop: {}", e);
                } else {
                    println!("Deleted jblox.stop");
                }

                println!("Shutdown complete. Exiting.");
                std::process::exit(0);
            }
        }
    });

    // Accept incoming connections in an infinite loop
    loop {

        if shutting_down.load(Ordering::SeqCst) {
            println!("Shutting down. Not accepting new connections.");
            return Ok(());
        }

        let handler = Arc::clone(&handler);

        // Accept a new TCP connection
        let (mut socket, _) = listener.accept().await?;

        // Spawn a new asynchronous task to handle the connection independently
        let MAXBUFFSIZE:usize  = settings.clone().maxbuffer;
        let htmldir = settings.clone().htmldir;
        let defaultpage = settings.clone().defaultpage;
        tokio::spawn(async move {
            // --- Read until headers complete ---
            let mut buf = Vec::<u8>::with_capacity(MAXBUFFSIZE.max(8192));
            let headers_end = loop {
                let mut tmp = [0u8; 4096];
                let Ok(n) = socket.read(&mut tmp).await else { return };
                if n == 0 { return; } // client closed
                buf.extend_from_slice(&tmp[..n]);
                if let Some(idx) = find_headers_end(&buf) {
                    break idx; // index of first byte of "\r\n\r\n"
                }
                if buf.len() > MAXBUFFSIZE {
                    // headers too large
                    let _ = write_400(&mut socket).await;
                    return;
                }
            };

            // headers as text
            let headers_bytes = &buf[..headers_end];
            let Ok(headers_str) = str::from_utf8(headers_bytes) else {
                let _ = write_400(&mut socket).await;
                return;
            };

            // Decide method from headers
            let is_post = headers_str.starts_with("POST ");
            if !is_post {
                // ---- GET (or others): serve static file just like your code ----
                let req_line = headers_str.lines().next().unwrap_or("");
                let path = req_line.split_whitespace().nth(1).unwrap_or("/");
                let page: &str = if path == "/" { &defaultpage } else { path.trim_start_matches('/') };
                let requested = Path::new(&htmldir).join(page);

                let target = if requested.exists() { requested }
                            else { Path::new(&htmldir).join(&defaultpage) };

                let html = match std::fs::read_to_string(&target) {
                    Ok(s) => s,
                    Err(e) => { eprintln!("Failed to read {}: {}", target.display(), e); return; }
                };

                let response = format!(
                    "HTTP/1.1 200 OK\r\n\
                    Content-Length: {}\r\n\
                    Content-Type: text/html; charset=utf-8\r\n\
                    Connection: close\r\n\r\n{}",
                    html.as_bytes().len(),
                    html
                );
                let _ = socket.write_all(response.as_bytes()).await;
                return;
            }

            // ---- POST: determine framing ----
            let mut content_length: Option<usize> = None;
            let mut chunked = false;

            for line in headers_str.lines().skip(1) {
                let (name, val) = split_header(line);
                if name.eq_ignore_ascii_case("content-length") {
                    if let Ok(n) = val.trim().parse::<usize>() { content_length = Some(n); }
                } else if name.eq_ignore_ascii_case("transfer-encoding") &&
                        val.to_ascii_lowercase().contains("chunked") {
                    chunked = true;
                }
            }

            let body_start = headers_end + 4; // skip CRLFCRLF
            let mut body = Vec::<u8>::new();

            if let Some(cl) = content_length {
                // Ensure we have the full body
                let already = buf.len().saturating_sub(body_start);
                if already >= cl {
                    body.extend_from_slice(&buf[body_start..body_start + cl]);
                } else {
                    body.extend_from_slice(&buf[body_start..]);
                    let mut remaining = cl - already;
                    while remaining > 0 {
                        let mut tmp = vec![0u8; remaining.min(4096)];
                        let Ok(n) = socket.read(&mut tmp).await else { return };
                        if n == 0 {
                            let _ = write_400(&mut socket).await;
                            return;
                        }
                        body.extend_from_slice(&tmp[..n]);
                        remaining -= n;
                        if body.len() > MAXBUFFSIZE {
                            let _ = write_400(&mut socket).await;
                            return;
                        }
                    }
                }
            } else if chunked {
                // Minimal chunked decode
                let mut stash = buf[body_start..].to_vec();
                loop {
                    match decode_chunked_try(&stash) {
                        ChunkParse::NeedMore => {
                            let mut tmp = [0u8; 4096];
                            let Ok(n) = socket.read(&mut tmp).await else { return };
                            if n == 0 {
                                let _ = write_400(&mut socket).await;
                                return;
                            }
                            stash.extend_from_slice(&tmp[..n]);
                            if stash.len() > MAXBUFFSIZE {
                                let _ = write_400(&mut socket).await;
                                return;
                            }
                        }
                        ChunkParse::Done(full) => { body = full; break; }
                        ChunkParse::Err => {
                            let _ = write_400(&mut socket).await;
                            return;
                        }
                    }
                }
            } else {
                // Neither Content-Length nor chunked
                let _ = write_411(&mut socket).await;
                return;
            }

            // Now parse JSON (UTF-8)
            let Ok(text) = String::from_utf8(body) else {
                let _ = write_400(&mut socket).await;
                return;
            };

            match serde_json::from_str::<Value>(&text) {
                Ok(json) => {
                    let mut response_body = String::new();
                    {
                        let mut h = handler.lock().await;

                        if json.get("data").is_some() {
                            let result: Result<Vec<String>, std::io::Error> = h.handle_request(&json.to_string());
                            response_body = match result {
                                Ok(lines) => {
                                    let data_obj: serde_json::Map<String, Value> = lines
                                        .iter()
                                        .enumerate()
                                        .map(|(i, line)| (format!("Rec{}", i + 1), json!(line)))
                                        .collect();
                                    json!({"response": "ok", "data": Value::Object(data_obj)}).to_string()
                                }
                                Err(e) => json!({"response": "error", "message": e.to_string()}).to_string(),
                            };
                        } else {
                            response_body = json!({"response": "error", "message": "'data' not found in input JSON"}).to_string();
                        }
                    }

                    let resp_bytes = response_body.as_bytes();
                    let response = format!(
                        "HTTP/1.1 200 OK\r\n\
                        Content-Type: application/json; charset=utf-8\r\n\
                        Content-Length: {}\r\n\
                        Connection: close\r\n\r\n{}",
                        resp_bytes.len(),
                        response_body
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                }
                Err(e) => {
                    let msg = format!("Invalid JSON: {}", e);
                    let response = format!(
                        "HTTP/1.1 400 Bad Request\r\n\
                        Content-Type: text/plain; charset=utf-8\r\n\
                        Content-Length: {}\r\n\
                        Connection: close\r\n\r\n{}",
                        msg.as_bytes().len(),
                        msg
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                }
            }
        });

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
            let config_path = current_dir.join("config/jbloxhttpsettings.toml");
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

pub fn print_jbloxDB() {
    // 7 rows × 8 cols per glyph; rows use the letter itself (no '#').
    // Lowercase 'o' and 'x' are intentionally shorter (x-height),
    // so they don't look like capitals next to D/B.
    fn glyph(c: char) -> [&'static str; 9] {
        match c {
            // ----- lowercase -----
            'j' => [
                "    j   ",
                "        ",
                "    j   ",
                "    j   ",
                "    j   ",
                "    j   ",
                "j   j   ",
                "j   j   ",
                " jjj    ",
            ],
            'b' => [
                "b       ",
                "b       ",
                "b bbb   ",
                "bb   b  ",
                "b    b  ",
                "bb   b  ",
                "b bbb   ",
                "        ",
                "        ",
            ],
            'l' => [ // simple lowercase 'l' (no baseline bar)
                "l ",
                "l ",
                "l ",
                "l ",
                "l ",
                "l ",
                "l ",
                "  ",
                "  ",
            ],
            // lowercase 'o' at x-height (shorter than capitals)
            'o' => [
                "        ",
                "        ",
                "  ooo   ",
                " o   o  ",
                " o   o  ",
                " o   o  ",
                "  ooo   ",
                "        ",
                "        ",
            ],
            // lowercase 'x' at x-height
            'x' => [
                "        ",
                "        ",
                " x   x  ",
                "  x x   ",
                "   x    ",
                "  x x   ",
                " x   x  ",
                "        ",
                "        ",
            ],

            // ----- uppercase -----
            'D' => [
                "DDDDD   ",
                "D    D  ",
                "D     D ",
                "D     D ",
                "D     D ",
                "D    D  ",
                "DDDDD   ",
                "        ",
                "        ",
            ],
            'B' => [
                "BBBBB   ",
                "B    B  ",
                "B    B  ",
                "BBBBB   ",
                "B    B  ",
                "B    B  ",
                "BBBBB   ",
                "        ",
                "        ",
            ],
            _ => ["        "; 9],
        }
    }

    // Exact text & case as requested.
    let text: [char; 7] = ['j','b','l','o','x','D','B'];

    // Build rows
    let mut rows: [String; 9] = Default::default();
    for r in 0..9 {
        let mut line = String::new();
        for &ch in &text {
            line.push_str(glyph(ch)[r]);
            line.push(' '); // spacing
        }
        while line.ends_with(' ') { line.pop(); }
        rows[r] = line;
    }

    // Border with padding
    let pad = 1usize;
    let content_w = rows.iter().map(|s| s.len()).max().unwrap_or(0);
    let inner_w = content_w + pad * 2;
    
    // --- ADDED: terminal fit check (skip printing if it won't fit) ---
    let total_width  = inner_w + 2;              // side borders
    let total_height = rows.len() + 2;           // top+bottom borders
    if let Ok((cols, rows_term)) = terminal::size() {
        let cols = cols as usize;
        let rows_term = rows_term as usize;
        if total_width > cols || total_height > rows_term {
            return; // do not display if it won't fit
        }
    } else {
        return; // fail closed if we can't read terminal size
    }
    // -----------------------------------------------------------------

    println!("+{}+", "-".repeat(inner_w));
    for row in rows {
        let mut padded = String::new();
        padded.push_str(&" ".repeat(pad));
        padded.push_str(&row);
        if padded.len() < inner_w {
            padded.push_str(&" ".repeat(inner_w - padded.len()));
        }
        println!("|{}|", padded);
    }
    println!("+{}+", "-".repeat(inner_w));
}



