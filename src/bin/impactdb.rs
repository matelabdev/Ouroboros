use std::env;
use std::io::{self, Write};
use std::time::Instant;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage:");
        eprintln!("  impactdb get <key>");
        eprintln!("  impactdb set <key> <value>");
        eprintln!("  impactdb keys [pattern]   (e.g. 'user_*', default: *)");
        eprintln!("");
        eprintln!("Connects to the local impactDB node gateway at http://localhost:3000");
        std::process::exit(1);
    }

    let command = &args[1];

    match command.as_str() {
        "get" => {
            if args.len() < 3 { eprintln!("Error: key missing."); std::process::exit(1); }
            let key = &args[2];
            let start = Instant::now();
            let url = format!(
                "http://localhost:3000/api/get?k={}",
                urlencoding::encode(key)
            );

            match ureq::get(&url).call() {
                Ok(response) => {
                    let body = response.into_string().unwrap_or_default();
                    let json: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
                    if let Some(value) = json.get("value").and_then(|v| v.as_str()) {
                        println!("\"{}\"", value);
                        println!("(Latency: {} µs)", start.elapsed().as_micros());
                    } else {
                        let err = json.get("error").and_then(|e| e.as_str()).unwrap_or("Not Found or Dropped");
                        eprintln!("(nil) - {}", err);
                        std::process::exit(1);
                    }
                }
                Err(e) => {
                    eprintln!("Connection refused. Is the impactDB server running?");
                    eprintln!("Details: {}", e);
                    std::process::exit(1);
                }
            }
        }
        "set" => {
            if args.len() < 4 { eprintln!("Error: Value is missing."); std::process::exit(1); }
            let key = &args[2];
            let value = &args[3];
            let start = Instant::now();
            let url = format!(
                "http://localhost:3000/api/set?k={}&v={}",
                urlencoding::encode(key),
                urlencoding::encode(value)
            );

            match ureq::get(&url).call() {
                Ok(_) => {
                    println!("OK ({} µs)", start.elapsed().as_micros());
                }
                Err(e) => {
                    eprintln!("Connection refused. Is the impactDB server running?");
                    eprintln!("Details: {}", e);
                    std::process::exit(1);
                }
            }
        }
        "keys" => {
            let pattern = args.get(2).map(|s| s.as_str()).unwrap_or("*");
            let url = format!("http://localhost:3000/api/keys?pattern={}", urlencoding::encode(pattern));

            match ureq::get(&url).call() {
                Ok(response) => {
                    let body = response.into_string().unwrap_or_default();
                    let json: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
                    if let Some(keys) = json.get("keys").and_then(|k| k.as_array()) {
                        if keys.is_empty() {
                            println!("(empty)");
                        } else {
                            for (i, k) in keys.iter().enumerate() {
                                println!("{}) {}", i + 1, k.as_str().unwrap_or(""));
                            }
                            println!("({} key{})", keys.len(), if keys.len() == 1 { "" } else { "s" });
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Connection refused. Is the impactDB server running?");
                    eprintln!("Details: {}", e);
                    std::process::exit(1);
                }
            }
        }
        _ => {
            eprintln!("Unknown command '{}'. Use 'get', 'set', or 'keys'.", command);
            std::process::exit(1);
        }
    }
}
