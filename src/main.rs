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


    let config_path = get_config_path();
    println!("Config file path for jbloxdb http wrapper: {}",config_path.to_str().unwrap());
    let config = Config::builder()
        .add_source(config::File::with_name(config_path.to_str().unwrap()))
        .build()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Config build error: {}", e)))?;

    let settings: Settings = config
        .try_deserialize()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Config deserialize error: {}", e)))?;



    // Create a thread-safe shared instance of jbothandler wrapped in Arc and Mutex
    let handler = Arc::new(Mutex::new(jbothandler::new().unwrap()));

    // Bind the TCP listener to localhost at port 3000
    let listener = TcpListener::bind(format!("{}:{}", settings.ip, settings.port)).await?;

    // Accept incoming connections in an infinite loop
    loop {
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
                            "HTTP/1.1 404 Not Found\r\nContent-Length: {}\r\n\r\n{}",
                            e.to_string().len(),
                            e
                        );
                        let _ = socket.write_all(response.as_bytes()).await;
                    }
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

