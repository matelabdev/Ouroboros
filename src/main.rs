use std::collections::HashMap;
use std::env;
use std::io::{self, BufRead};
use std::net::UdpSocket;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
enum Packet {
    Data { key: u32, value: u32, epoch: u32 },
    Query { key: u32, origin_port: u16, ttl: u8 },
    Response { key: u32, value: u32, epoch: u32 },
}

impl Packet {
    fn to_bytes(&self) -> [u8; 16] {
        let mut buf = [0u8; 16];
        match self {
            Packet::Data { key, value, epoch } => {
                buf[0] = 0;
                buf[1..5].copy_from_slice(&key.to_be_bytes());
                buf[5..9].copy_from_slice(&value.to_be_bytes());
                buf[9..13].copy_from_slice(&epoch.to_be_bytes());
            }
            Packet::Query {
                key,
                origin_port,
                ttl,
            } => {
                buf[0] = 1;
                buf[1..5].copy_from_slice(&key.to_be_bytes());
                buf[13] = *ttl;
                buf[14..16].copy_from_slice(&origin_port.to_be_bytes());
            }
            Packet::Response { key, value, epoch } => {
                buf[0] = 2;
                buf[1..5].copy_from_slice(&key.to_be_bytes());
                buf[5..9].copy_from_slice(&value.to_be_bytes());
                buf[9..13].copy_from_slice(&epoch.to_be_bytes());
            }
        }
        buf
    }

    fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < 16 {
            return None;
        }
        let key = u32::from_be_bytes(buf[1..5].try_into().unwrap());
        match buf[0] {
            0 => {
                let value = u32::from_be_bytes(buf[5..9].try_into().unwrap());
                let epoch = u32::from_be_bytes(buf[9..13].try_into().unwrap());
                Some(Packet::Data { key, value, epoch })
            }
            1 => {
                let ttl = buf[13];
                let origin_port = u16::from_be_bytes(buf[14..16].try_into().unwrap());
                Some(Packet::Query {
                    key,
                    origin_port,
                    ttl,
                })
            }
            2 => {
                let value = u32::from_be_bytes(buf[5..9].try_into().unwrap());
                let epoch = u32::from_be_bytes(buf[9..13].try_into().unwrap());
                Some(Packet::Response { key, value, epoch })
            }
            _ => None,
        }
    }
}

const HISTORY_DURATION: Duration = Duration::from_millis(50);
const DEFAULT_TTL: u8 = 10;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 5 {
        eprintln!("Usage: cargo run -- <NODE_ID> <PORT> <DATA_NEXT_PORT> <QUERY_NEXT_PORT>");
        std::process::exit(1);
    }

    let node_id: u32 = args[1].parse().unwrap();
    let port: u16 = args[2].parse().unwrap();
    let data_next_port: u16 = args[3].parse().unwrap();
    let query_next_port: u16 = args[4].parse().unwrap();

    let socket = UdpSocket::bind(format!("127.0.0.1:{}", port)).expect("Failed to bind port");
    println!("Node {} started. Listening on port: {}", node_id, port);
    println!("Data routing port: {}", data_next_port);
    println!("Query routing port: {}", query_next_port);
    println!("Commands: 'SET <key> <value>' or 'GET <key>'");

    let seen_epochs: Arc<Mutex<HashMap<u32, u32>>> = Arc::new(Mutex::new(HashMap::new()));
    let recent_data: Arc<Mutex<HashMap<u32, (u32, u32, Instant)>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let recent_queries: Arc<Mutex<HashMap<u32, (u16, Instant)>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let socket_clone = socket.try_clone().unwrap();
    thread::spawn(move || {
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            let line = line.unwrap();
            let parts: Vec<&str> = line.trim().split_whitespace().collect();
            if parts.is_empty() {
                continue;
            }

            match parts[0].to_uppercase().as_str() {
                "SET" => {
                    if parts.len() == 3 {
                        if let (Ok(key), Ok(value)) =
                            (parts[1].parse::<u32>(), parts[2].parse::<u32>())
                        {
                            let epoch = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as u32;

                            let packet = Packet::Data { key, value, epoch };
                            socket_clone
                                .send_to(
                                    &packet.to_bytes(),
                                    format!("127.0.0.1:{}", data_next_port),
                                )
                                .unwrap();
                            println!(
                                "[Node {}] Data sent to network. SET {}={} (Epoch: {})",
                                node_id, key, value, epoch
                            );
                        }
                    }
                }
                "GET" => {
                    if parts.len() == 2 {
                        if let Ok(key) = parts[1].parse::<u32>() {
                            let packet = Packet::Query {
                                key,
                                origin_port: port,
                                ttl: DEFAULT_TTL,
                            };
                            socket_clone
                                .send_to(
                                    &packet.to_bytes(),
                                    format!("127.0.0.1:{}", query_next_port),
                                )
                                .unwrap();
                            println!(
                                "[Node {}] Query sent in reverse direction. GET {}",
                                node_id, key
                            );
                        }
                    }
                }
                _ => (),
            }
        }
    });

    let mut buf = [0u8; 32];
    loop {
        if let Ok((size, _)) = socket.recv_from(&mut buf) {
            if size >= 16 {
                if let Some(packet) = Packet::from_bytes(&buf[..16]) {
                    process_packet(
                        packet,
                        data_next_port,
                        query_next_port,
                        &socket,
                        &seen_epochs,
                        &recent_data,
                        &recent_queries,
                    );
                }
            }
        }

        let now = Instant::now();
        recent_data
            .lock()
            .unwrap()
            .retain(|_, (_, _, time)| now.duration_since(*time) < HISTORY_DURATION);
        recent_queries
            .lock()
            .unwrap()
            .retain(|_, (_, time)| now.duration_since(*time) < HISTORY_DURATION);
    }
}

fn process_packet(
    packet: Packet,
    data_next_port: u16,
    query_next_port: u16,
    socket: &UdpSocket,
    seen_epochs: &Arc<Mutex<HashMap<u32, u32>>>,
    recent_data: &Arc<Mutex<HashMap<u32, (u32, u32, Instant)>>>,
    recent_queries: &Arc<Mutex<HashMap<u32, (u16, Instant)>>>,
) {
    let now = Instant::now();

    match packet {
        Packet::Data { key, value, epoch } => {
            let mut epochs = seen_epochs.lock().unwrap();
            if let Some(&highest_epoch) = epochs.get(&key) {
                if epoch < highest_epoch {
                    // Muted: println!("[GC] Stale data encountered and dropped from network. Key: {}, Epoch: {}", key, epoch);
                    return;
                }
            }
            epochs.insert(key, epoch);
            drop(epochs);

            let mut queries = recent_queries.lock().unwrap();
            if let Some(&(origin_port, _)) = queries.get(&key) {
                // Muted: println!("[Collision] Data ({}) collided with query, sending response.", key);
                let response = Packet::Response { key, value, epoch };
                socket
                    .send_to(&response.to_bytes(), format!("127.0.0.1:{}", origin_port))
                    .unwrap();
                queries.remove(&key);
            }

            recent_data.lock().unwrap().insert(key, (value, epoch, now));

            let new_packet = Packet::Data { key, value, epoch };
            socket
                .send_to(
                    &new_packet.to_bytes(),
                    format!("127.0.0.1:{}", data_next_port),
                )
                .unwrap();
        }

        Packet::Query {
            key,
            origin_port,
            mut ttl,
        } => {
            let data = recent_data.lock().unwrap();
            if let Some(&(value, epoch, _)) = data.get(&key) {
                // Muted: println!("[Collision] Query ({}) collided with data, sending response.", key);
                let response = Packet::Response { key, value, epoch };
                socket
                    .send_to(&response.to_bytes(), format!("127.0.0.1:{}", origin_port))
                    .unwrap();
            } else {
                recent_queries
                    .lock()
                    .unwrap()
                    .insert(key, (origin_port, now));

                ttl -= 1;
                if ttl > 0 {
                    let new_packet = Packet::Query {
                        key,
                        origin_port,
                        ttl,
                    };
                    // Removed sleep to unlock full speed
                    socket
                        .send_to(
                            &new_packet.to_bytes(),
                            format!("127.0.0.1:{}", query_next_port),
                        )
                        .unwrap();
                } else {
                    // Muted: println!("[Error] Query packet TTL expired. Key: {}", key);
                }
            }
        }

        Packet::Response { .. } => {
            // Muted: println!("[Success] Response received...");
        }
    }
}
