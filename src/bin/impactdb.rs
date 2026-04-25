use std::env;
use std::io::{self, BufRead, BufReader, Write};
use std::net::TcpStream;
use std::time::Instant;

fn execute_command(stream: &mut TcpStream, cmd: &str) {
    let start = Instant::now();
    if let Err(e) = stream.write_all(format!("{}\n", cmd).as_bytes()) {
        eprintln!("Error writing to socket: {}", e);
        return;
    }

    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut response = String::new();
    if let Ok(n) = reader.read_line(&mut response) {
        if n == 0 {
            eprintln!("Connection closed by server.");
            return;
        }
        let latency = start.elapsed().as_micros();
        let response = response.trim_end();
        if response.starts_with("+") {
            if response == "+OK" {
                println!("OK ({} µs)", latency);
            } else {
                println!("\"{}\"\n(Latency: {} µs)", &response[1..], latency);
            }
        } else if response.starts_with("-ERR") {
            eprintln!("(error) {}", &response[5..]);
        } else if response == "$-1" {
            println!("(nil)");
        } else if response.starts_with("*") {
            let count: usize = response[1..].parse().unwrap_or(0);
            if count == 0 {
                println!("(empty)");
            } else {
                for i in 0..count {
                    let mut len_str = String::new();
                    reader.read_line(&mut len_str).unwrap();
                    let mut val_str = String::new();
                    reader.read_line(&mut val_str).unwrap();
                    println!("{}) {}", i + 1, val_str.trim_end());
                }
                println!("({} key{})", count, if count == 1 { "" } else { "s" });
            }
        } else {
            println!("{}", response);
        }
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    
    let mut stream = match TcpStream::connect("127.0.0.1:8825") {
        Ok(s) => s,
        Err(_) => {
            eprintln!("Could not connect to impactDB on 127.0.0.1:8825.");
            eprintln!("Is the server running?");
            std::process::exit(1);
        }
    };

    if args.len() > 1 {
        let cmd = args[1..].join(" ");
        execute_command(&mut stream, &cmd);
    } else {
        println!("impactdb-cli (connected to 127.0.0.1:8825 via Raw TCP)");
        loop {
            print!("> ");
            io::stdout().flush().unwrap();
            let mut input = String::new();
            match io::stdin().read_line(&mut input) {
                Ok(n) if n == 0 => break, // EOF
                Ok(_) => {
                    let cmd = input.trim();
                    if cmd.is_empty() { continue; }
                    if cmd.eq_ignore_ascii_case("exit") || cmd.eq_ignore_ascii_case("quit") {
                        break;
                    }
                    execute_command(&mut stream, cmd);
                }
                Err(e) => {
                    eprintln!("Error reading input: {}", e);
                    break;
                }
            }
        }
    }
}
