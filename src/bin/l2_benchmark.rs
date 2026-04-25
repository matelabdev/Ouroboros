use pnet::datalink::{self, Channel, MacAddr};
use pnet::packet::ethernet::{EtherType, EthernetPacket, MutableEthernetPacket};
use pnet::packet::Packet as PnetPacket;
use std::collections::HashMap;
use std::env;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const IMPACT_ETHERTYPE: EtherType = EtherType(0x88B5);
const TOTAL_REQUESTS: u32 = 10;
const DEFAULT_TTL: u8 = 10;

// Benchmark node virtual MAC
const BENCHMARK_MAC: MacAddr = MacAddr(2, 0, 0, 0, 0, 99);
// Target Node 1 virtual MAC
const TARGET_MAC: MacAddr = MacAddr(2, 0, 0, 0, 0, 1);

#[derive(Debug, Clone)]
enum Packet {
    Data { key: u32, value: u32, epoch: u32 },
    Query { key: u32, origin_mac: u8, ttl: u8 },
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
            Packet::Query { key, origin_mac, ttl } => {
                buf[0] = 1;
                buf[1..5].copy_from_slice(&key.to_be_bytes());
                buf[13] = *ttl;
                buf[14] = *origin_mac;
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
                let origin_mac = buf[14];
                Some(Packet::Query { key, origin_mac, ttl })
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

fn send_l2_packet(
    tx: &mut Box<dyn datalink::DataLinkSender>,
    my_mac: MacAddr,
    dest_mac: MacAddr,
    packet: &Packet,
) {
    let mut ethernet_buffer = [0u8; 30]; // 14 byte Eth Header + 16 byte Payload
    let mut eth_packet = MutableEthernetPacket::new(&mut ethernet_buffer).unwrap();
    
    eth_packet.set_destination(dest_mac);
    eth_packet.set_source(my_mac);
    eth_packet.set_ethertype(IMPACT_ETHERTYPE);
    eth_packet.set_payload(&packet.to_bytes());

    tx.send_to(eth_packet.packet(), None);
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: sudo cargo run --bin l2_benchmark <INTERFACE_NAME>");
        eprintln!("Example: sudo cargo run --bin l2_benchmark en0");
        std::process::exit(1);
    }

    let interface_name = &args[1];

    let interfaces = datalink::interfaces();
    let interface = interfaces
        .into_iter()
        .find(|iface| iface.name == *interface_name)
        .expect("Error finding network interface");

    println!("========== impactDB L2 Raw Benchmark ==========");
    println!("Interface   : {}", interface.name);
    println!("My MAC      : {}", BENCHMARK_MAC);
    println!("Target MAC  : {}", TARGET_MAC);
    println!("=================================================");

    let (mut tx, mut rx) = match datalink::channel(&interface, Default::default()) {
        Ok(Channel::Ethernet(tx, rx)) => (tx, rx),
        Ok(_) => panic!("Unhandled channel type. Please use Ethernet."),
        Err(e) => panic!("An error occurred creating the datalink channel: {} (Did you forget sudo?)", e),
    };

    // PHASE 1: Seed Data
    println!("Phase 1: Seeding {} data frames directly to NIC...", TOTAL_REQUESTS);
    let epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u32;
    
    let start_seed = Instant::now();
    for key in 1..=TOTAL_REQUESTS {
        let packet = Packet::Data { key, value: key * 2, epoch };
        send_l2_packet(&mut tx, BENCHMARK_MAC, TARGET_MAC, &packet);
        
        // Slightly lower delay because L2 bypassing OS buffer is often faster,
        // but we still don't want to completely drown the NIC.
        thread::sleep(Duration::from_micros(50)); 
    }
    println!("Seeding completed in {:?}", start_seed.elapsed());
    
    // We remove the 2 second delay because software L2 dropping is too aggressive
    println!("Immediately blasting Query frames to catch data in flight...");
    
    // PHASE 2: Querying
    println!("Phase 2: Blasting {} Query frames...", TOTAL_REQUESTS);
    
    let received_count = Arc::new(Mutex::new(0));
    let total_latency_us = Arc::new(Mutex::new(0u128));
    let query_times: Arc<Mutex<HashMap<u32, Instant>>> = Arc::new(Mutex::new(HashMap::new()));
    
    let r_count = Arc::clone(&received_count);
    let t_latency = Arc::clone(&total_latency_us);
    let q_times = Arc::clone(&query_times);
    
    // Listener Thread
    thread::spawn(move || {
        loop {
            match rx.next() {
                Ok(frame) => {
                    if let Some(eth) = EthernetPacket::new(frame) {
                        // Look for our custom protocol destined to us
                        if eth.get_ethertype() == IMPACT_ETHERTYPE && eth.get_destination() == BENCHMARK_MAC {
                            if let Some(Packet::Response { key, value: _, epoch: _ }) = Packet::from_bytes(eth.payload()) {
                                let now = Instant::now();
                                let mut times = q_times.lock().unwrap();
                                if let Some(sent_time) = times.remove(&key) {
                                    let latency = now.duration_since(sent_time).as_micros();
                                    *t_latency.lock().unwrap() += latency as u128;
                                    *r_count.lock().unwrap() += 1;
                                }
                            }
                        }
                    }
                }
                Err(_) => {
                    break;
                }
            }
        }
    });
    
    let start_query = Instant::now();
    for key in 1..=TOTAL_REQUESTS {
        // origin_mac is 99 (from BENCHMARK_MAC)
        let packet = Packet::Query { key, origin_mac: 99, ttl: DEFAULT_TTL };
        query_times.lock().unwrap().insert(key, Instant::now());
        send_l2_packet(&mut tx, BENCHMARK_MAC, TARGET_MAC, &packet);
        
        thread::sleep(Duration::from_micros(50));
    }
    
    println!("All queries fired in {:?}. Waiting 3 seconds for straggler responses...", start_query.elapsed());
    thread::sleep(Duration::from_secs(3));
    
    let final_count = *received_count.lock().unwrap();
    let final_latency = *total_latency_us.lock().unwrap();
    
    println!("\n==================================");
    println!("        L2 BENCHMARK RESULTS      ");
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
