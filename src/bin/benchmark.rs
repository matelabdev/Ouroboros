use std::collections::HashMap;
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
            Packet::Query { key, origin_port, ttl } => {
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
                Some(Packet::Query { key, origin_port, ttl })
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

const TARGET_PORT: u16 = 8001; // Targeting Node 1
const BENCHMARK_PORT: u16 = 9999;
const TOTAL_REQUESTS: u32 = 10_000;
const DEFAULT_TTL: u8 = 10;

fn main() {
    println!("Starting impactDB Benchmark Suite...");
    println!("Targeting Node on Port: {}", TARGET_PORT);
    
    let socket = UdpSocket::bind(format!("127.0.0.1:{}", BENCHMARK_PORT)).expect("Failed to bind benchmark port");
    let target_addr = format!("127.0.0.1:{}", TARGET_PORT);
    
    // PHASE 1: Seed Data
    println!("Phase 1: Seeding {} data packets into the network...", TOTAL_REQUESTS);
    let epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u32;
    
    let start_seed = Instant::now();
    for key in 1..=TOTAL_REQUESTS {
        let packet = Packet::Data { key, value: key * 2, epoch };
        socket.send_to(&packet.to_bytes(), &target_addr).unwrap();
        // 150 microsecond delay to avoid OS level UDP buffer overflow
        thread::sleep(Duration::from_micros(150)); 
    }
    println!("Seeding completed in {:?}", start_seed.elapsed());
    
    println!("Waiting 2 seconds for data to stabilize in the ring...");
    thread::sleep(Duration::from_secs(2));
    
    // PHASE 2: Querying
    println!("Phase 2: Blasting {} Queries...", TOTAL_REQUESTS);
    
    let received_count = Arc::new(Mutex::new(0));
    let total_latency_us = Arc::new(Mutex::new(0u128));
    let query_times: Arc<Mutex<HashMap<u32, Instant>>> = Arc::new(Mutex::new(HashMap::new()));
    
    let recv_socket = socket.try_clone().unwrap();
    let r_count = Arc::clone(&received_count);
    let t_latency = Arc::clone(&total_latency_us);
    let q_times = Arc::clone(&query_times);
    
    // Listener Thread
    thread::spawn(move || {
        let mut buf = [0u8; 32];
        recv_socket.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
        loop {
            if let Ok((size, _)) = recv_socket.recv_from(&mut buf) {
                if size >= 16 {
                    if let Some(Packet::Response { key, value: _, epoch: _ }) = Packet::from_bytes(&buf[..16]) {
                        let now = Instant::now();
                        let mut times = q_times.lock().unwrap();
                        if let Some(sent_time) = times.remove(&key) {
                            let latency = now.duration_since(sent_time).as_micros();
                            *t_latency.lock().unwrap() += latency as u128;
                            *r_count.lock().unwrap() += 1;
                        }
                    }
                }
            } else {
                // Timeout
                break;
            }
        }
    });
    
    let start_query = Instant::now();
    for key in 1..=TOTAL_REQUESTS {
        let packet = Packet::Query { key, origin_port: BENCHMARK_PORT, ttl: DEFAULT_TTL };
        query_times.lock().unwrap().insert(key, Instant::now());
        socket.send_to(&packet.to_bytes(), &target_addr).unwrap();
        
        // 150 microsecond delay to avoid UDP buffer drop
        thread::sleep(Duration::from_micros(150));
    }
    
    println!("All queries fired in {:?}. Waiting up to 3 seconds for straggler responses...", start_query.elapsed());
    thread::sleep(Duration::from_secs(3));
    
    // Calculate Results
    let final_count = *received_count.lock().unwrap();
    let final_latency = *total_latency_us.lock().unwrap();
    
    println!("\n==================================");
    println!("         BENCHMARK RESULTS        ");
    println!("==================================");
    println!("Total Requests Sent : {}", TOTAL_REQUESTS);
    println!("Responses Received  : {}", final_count);
    
    let success_rate = (final_count as f64 / TOTAL_REQUESTS as f64) * 100.0;
    println!("Success Rate        : {:.2}%", success_rate);
    
    if final_count > 0 {
        let avg_latency = final_latency / (final_count as u128);
        println!("Average Latency     : {} µs ({:.2} ms)", avg_latency, avg_latency as f64 / 1000.0);
        let ops_per_sec = (final_count as f64 / (start_query.elapsed().as_secs_f64() + 3.0)) as u32;
        println!("Approx Throughput   : {} ops/sec", ops_per_sec);
    } else {
        println!("Average Latency     : N/A (No responses)");
    }
    println!("==================================\n");
}
