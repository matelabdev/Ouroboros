use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::collections::HashMap;
use std::time::Duration;

struct OuroStore {
    data: HashMap<String, String>,
}

impl OuroStore {
    fn new() -> Self {
        Self { data: HashMap::new() }
    }

    fn handle_line(&mut self, line: &str) -> String {
        let parts: Vec<&str> = line.trim().splitn(3, ' ').collect();
        if parts.is_empty() { return "-ERR Empty".to_string(); }

        match parts[0].to_uppercase().as_str() {
            "SET" => {
                if parts.len() < 3 { return "-ERR SET needs key and value".to_string(); }
                self.data.insert(parts[1].to_string(), parts[2].to_string());
                "+OK".to_string()
            }
            "GET" => {
                if parts.len() < 2 { return "-ERR GET needs key".to_string(); }
                if let Some(val) = self.data.get(parts[1]) {
                    format!("+{}", val)
                } else {
                    "$-1".to_string()
                }
            }
            "KEYS" => {
                let pattern = if parts.len() > 1 { parts[1] } else { "*" };
                let keys: Vec<&String> = self.data.keys()
                    .filter(|k| k.contains(pattern.replace("*", "").as_str()))
                    .collect();
                
                let mut resp = format!("*{}", keys.len());
                for key in keys {
                    resp.push_str(&format!("\n${}\n{}", key.len(), key));
                }
                resp
            }
            _ => "-ERR Unknown Command".to_string(),
        }
    }
}

fn main() {
    let listener = TcpListener::bind("0.0.0.0:8825").expect("Could not bind Ouroboros Core");
    let mut store = OuroStore::new();
    println!("Ouroboros Core Engine (RAM-Only) started on port 8825");

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                // CRITICAL: Set short timeouts to prevent hanging the whole ring
                let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
                let _ = stream.set_write_timeout(Some(Duration::from_millis(500)));
                
                let mut reader = BufReader::new(&stream);
                let mut line = String::new();
                if reader.read_line(&mut line).is_ok() && !line.is_empty() {
                    let response = store.handle_line(&line);
                    let _ = stream.write_all(format!("{}\n", response).as_bytes());
                }
            }
            Err(e) => eprintln!("Stream error: {}", e),
        }
    }
}
