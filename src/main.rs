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

            let mut buffer = vec![0u8; MAXBUFFSIZE]; // Read buffer for incoming request
            let Ok(n) = socket.read(&mut buffer).await else { return };

            // Convert raw bytes to UTF-8 string
            let request = String::from_utf8_lossy(&buffer[..n]);
            //println!("Request:\n{}", request);

            // Parse the request line, e.g. "GET /foo.html HTTP/1.1"


            // Check if the request is a POST
            if request.starts_with("POST") {
                // Find where the headers end and body begins
                if let Some(headers_end) = request.find("\r\n\r\n") {
                    let headers = &request[..headers_end];
                    let body = &request[headers_end + 4..];

                    // Try to extract Content-Length header
                    let content_length = headers
                        .lines()
                        .find(|line| line.to_lowercase().starts_with("content-length"))
                        .and_then(|line| line.split(':').nth(1))
                        .and_then(|val| val.trim().parse::<usize>().ok())
                        .unwrap_or(0);

                    // Serve static HTML if no content length is provided
                    if content_length == 0 {
                        match fs::read_to_string(format!("{}/{}", htmldir, defaultpage)) {
                            Ok(html) => {
                                let response = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: text/html\r\n\r\n{}",
                                    html.len(),
                                    html
                                );
                                let _ = socket.write_all(response.as_bytes()).await;
                            }
                            Err(e) => {
                                let response = format!(
                                    "HTTP/1.1 500 Internal Server Error\r\nContent-Length: {}\r\n\r\n{}",
                                    e.to_string().len(),
                                    e
                                );
                                let _ = socket.write_all(response.as_bytes()).await;
                            }
                        }
                    } else {
                        // Parse the body into JSON
                        let body = &body[..content_length.min(body.len())];

                        match serde_json::from_str::<Value>(body) {
                            Ok(json) => {
                                //println!("Parsed JSON: {:?}", json);

                                let mut response_body = String::new();
                                {
                                    // Lock the shared handler
                                    let mut h = handler.lock().await;

                                    // Look for the "data" field in the JSON
                                    if let Some(data_value) = json.get("data") {
                                        // Call handler logic and build a JSON response
                                        let result: Result<Vec<String>, std::io::Error> 
                                            = h.handle_request(&json.to_string());


                                        response_body = match result {
                                            Ok(lines) => {
                                                // Format the returned lines as Rec1, Rec2...
                                                let data_obj: serde_json::Map<String, Value> = lines
                                                    .iter()
                                                    .enumerate()
                                                    .map(|(i, line)| (format!("Rec{}", i + 1), json!(line)))
                                                    .collect();

                                                json!({
                                                    "response": "ok",
                                                    "data": Value::Object(data_obj)
                                                }).to_string()
                                            }
                                            Err(e) => {
                                                json!({
                                                    "response": "error",
                                                    "message": format!("{}", e)
                                                }).to_string()
                                            }
                                        };
                                    } else {
                                        // Missing data field error
                                        println!("'data' not found in JSON");
                                        response_body = json!({
                                            "response": "error",
                                            "message": "'data' not found in input JSON"
                                        }).to_string();
                                    }
                                }

                                // Send formatted JSON response
                                let response = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: text/plain\r\n\r\n{}",
                                    response_body.len(),
                                    response_body
                                );
                                let _ = socket.write_all(response.as_bytes()).await;
                            }
                            Err(e) => {
                                // Handle invalid JSON input
                                let response = format!(
                                    "HTTP/1.1 400 Bad Request\r\nContent-Length: {}\r\n\r\nInvalid JSON: {}",
                                    e.to_string().len(),
                                    e
                                );
                                let _ = socket.write_all(response.as_bytes()).await;
                            }
                        }
                    }
                }
            } else {
                // For GET or other methods, serve a static HTML file
                let req_line = request.lines().next().unwrap_or("");
                let path = req_line.split_whitespace().nth(1).unwrap_or("/");
                // Decide which file to serve: "/" → defaultpage, otherwise strip the leading '/'
                let page: &str  = if path == "/" {
                    &defaultpage
                } else {
                    &path[1..]  
                };
                // Build the filesystem path and try to read it
                let requested = Path::new(&htmldir).join(page);
                
                // 2) Decide which file to read based on existence
                let target = if requested.exists() {
                    requested
                } else {
                    Path::new(&htmldir).join(defaultpage)
                };

                let html = match std::fs::read_to_string(&target) {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("Failed to read {}: {}", target.display(), e);
                        return;       // this returns from your async block (which is `()`), no `?` needed
                    }
                };

                // now send the HTTP response
                let response = format!(
                    "HTTP/1.1 200 OK\r\n\
                    Content-Length: {}\r\n\
                    Content-Type: text/html\r\n\r\n\
                    {}",
                    html.len(),
                    html
                );
                let _ = socket.write_all(response.as_bytes()).await;

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



