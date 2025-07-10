use reqwest::blocking::Client;
use std::{fs, thread, time::Duration};
use chrono::Local;
use serde_json::json;
use base64;

// Path to the image you want to send
const IMAGE_PATH: &str = "C:/Users/prati/Downloads/GA.png";
// Interval between sends (seconds)
const SEND_INTERVAL_SECS: f64 = 200000.0;

fn main() {
    let interval = Duration::from_secs_f64(SEND_INTERVAL_SECS);
    let client = Client::new();
    let url = "http://localhost:3000/submit"; // Adjust if needed

    loop {
        // Read image file into bytes
        let img_bytes = match fs::read(IMAGE_PATH) {
            Ok(bytes) => bytes,
            Err(e) => {
                eprintln!("Failed to read image file: {}", e);
                break;
            }
        };

        // Encode image to Base64 with proper prefix
        let base64_image = format!("data:image/png;base64,{}", base64::encode(&img_bytes));

        // Create JSON payload
        let timestamp = Local::now().format("%Y%m%d%H%M%S%6f").to_string();
        let payload = json!({
            "op": "insertduplicate",
            "data": {
                "key": "id",
                "keyobj": "screenshot",
                "id": timestamp,
                "file": base64_image
            }
        });

        println!("Sending image at {}", timestamp);

        // POST request
        let res = client
            .post(url)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send();

        match res {
            Ok(r) => println!("Server responded: {}", r.status()),
            Err(e) => eprintln!("Failed to send image: {}", e),
        }

        // Wait before sending the next image
        thread::sleep(interval);
    }
}
