use pnet::datalink::{self, Channel, MacAddr};
use pnet::packet::ethernet::{EtherType, EthernetPacket, MutableEthernetPacket};
use pnet::packet::Packet as PnetPacket;
use std::collections::HashMap;
use std::env;
use std::io::{self, BufRead};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// Custom EtherType for impactDB (0x88B5 is IEEE Experimental)
const IMPACT_ETHERTYPE: EtherType = EtherType(0x88B5);
const TOTAL_NODES: u32 = 3;
const HISTORY_DURATION: Duration = Duration::from_millis(50);
const DEFAULT_TTL: u8 = 10;

#[derive(Debug, Clone)]
enum Packet {
    Data { key: u32, value: u32, epoch: u32 },
    Query { key: u32, origin_mac: u8, ttl: u8 }, // origin_mac holds the node_id
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

fn get_mac(node_id: u32) -> MacAddr {
    MacAddr::new(2, 0, 0, 0, 0, node_id as u8)
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
    if args.len() != 3 {
        eprintln!("Usage: sudo cargo run --bin l2_node <INTERFACE_NAME> <NODE_ID>");
        eprintln!("Example: sudo cargo run --bin l2_node en0 1");
        std::process::exit(1);
    }

    let interface_name = &args[1];
    let node_id: u32 = args[2].parse().expect("Invalid Node ID");

    // Discover the interface
    let interfaces = datalink::interfaces();
    let interface = interfaces
        .into_iter()
        .find(|iface| iface.name == *interface_name)
        .expect("Error finding network interface");

    let my_mac = get_mac(node_id);
    let data_next_id = if node_id == TOTAL_NODES { 1 } else { node_id + 1 };
    let query_next_id = if node_id == 1 { TOTAL_NODES } else { node_id - 1 };
    
    let data_next_mac = get_mac(data_next_id);
    let query_next_mac = get_mac(query_next_id);

    println!("========== impactDB L2 Raw Socket Node ==========");
    println!("Interface   : {}", interface.name);
    println!("My Node ID  : {} (Virtual MAC: {})", node_id, my_mac);
    println!("Data Route  : -> Node {} ({})", data_next_id, data_next_mac);
    println!("Query Route : -> Node {} ({})", query_next_id, query_next_mac);
    println!("Commands    : 'SET <key> <value>' or 'GET <key>'");
    println!("=================================================");

    // Create the raw ethernet channel
    let (tx, mut rx) = match datalink::channel(&interface, Default::default()) {
        Ok(Channel::Ethernet(tx, rx)) => (tx, rx),
        Ok(_) => panic!("Unhandled channel type. Please use Ethernet."),
        Err(e) => panic!("An error occurred creating the datalink channel: {} (Did you forget sudo?)", e),
    };

    let tx = Arc::new(Mutex::new(tx));

    let seen_epochs: Arc<Mutex<HashMap<u32, u32>>> = Arc::new(Mutex::new(HashMap::new()));
    let recent_data: Arc<Mutex<HashMap<u32, (u32, u32, Instant)>>> = Arc::new(Mutex::new(HashMap::new()));
    let recent_queries: Arc<Mutex<HashMap<u32, (u8, Instant)>>> = Arc::new(Mutex::new(HashMap::new()));

    // Terminal Input Thread
    let tx_clone = Arc::clone(&tx);
    thread::spawn(move || {
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            let line = line.unwrap();
            let parts: Vec<&str> = line.trim().split_whitespace().collect();
            if parts.is_empty() { continue; }

            match parts[0].to_uppercase().as_str() {
                "SET" => {
                    if parts.len() == 3 {
                        if let (Ok(key), Ok(value)) = (parts[1].parse::<u32>(), parts[2].parse::<u32>()) {
                            let epoch = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as u32;
                                
                            let packet = Packet::Data { key, value, epoch };
                            send_l2_packet(&mut *tx_clone.lock().unwrap(), my_mac, data_next_mac, &packet);
                            println!("[Node {}] L2 Frame sent directly to NIC. SET {}={} (Epoch: {})", node_id, key, value, epoch);
                        }
                    }
                }
                "GET" => {
                    if parts.len() == 2 {
                        if let Ok(key) = parts[1].parse::<u32>() {
                            let packet = Packet::Query { key, origin_mac: node_id as u8, ttl: DEFAULT_TTL };
                            send_l2_packet(&mut *tx_clone.lock().unwrap(), my_mac, query_next_mac, &packet);
                            println!("[Node {}] L2 Query Frame sent. GET {}", node_id, key);
                        }
                    }
                }
                _ => (),
            }
        }
    });

    // Main L2 Packet Sniffing Loop
    loop {
        match rx.next() {
            Ok(frame) => {
                if let Some(eth) = EthernetPacket::new(frame) {
                    // Filter only our custom impactDB protocol and destined to our Virtual MAC
                    if eth.get_ethertype() == IMPACT_ETHERTYPE && eth.get_destination() == my_mac {
                        if let Some(packet) = Packet::from_bytes(eth.payload()) {
                            process_packet(
                                packet,
                                my_mac,
                                data_next_mac,
                                query_next_mac,
                                &tx,
                                &seen_epochs,
                                &recent_data,
                                &recent_queries,
                            );
                        }
                    }
                }
            }
            Err(e) => {
                panic!("An error occurred while reading: {}", e);
            }
        }

        let now = Instant::now();
        // Memory cleanup threshold check can be expensive in hot loop, but keeping it simple
        recent_data.lock().unwrap().retain(|_, (_, _, time)| now.duration_since(*time) < HISTORY_DURATION);
        recent_queries.lock().unwrap().retain(|_, (_, time)| now.duration_since(*time) < HISTORY_DURATION);
    }
}

fn process_packet(
    packet: Packet,
    my_mac: MacAddr,
    data_next_mac: MacAddr,
    query_next_mac: MacAddr,
    tx: &Arc<Mutex<Box<dyn datalink::DataLinkSender>>>,
    seen_epochs: &Arc<Mutex<HashMap<u32, u32>>>,
    recent_data: &Arc<Mutex<HashMap<u32, (u32, u32, Instant)>>>,
    recent_queries: &Arc<Mutex<HashMap<u32, (u8, Instant)>>>,
) {
    let now = Instant::now();
    let mut tx_lock = tx.lock().unwrap();

    match packet {
        Packet::Data { key, value, epoch } => {
            let mut epochs = seen_epochs.lock().unwrap();
            if let Some(&highest_epoch) = epochs.get(&key) {
                if epoch < highest_epoch {
                    // Drop stale L2 frame
                    return;
                }
            }
            epochs.insert(key, epoch);
            drop(epochs);

            let mut queries = recent_queries.lock().unwrap();
            if let Some(&(origin_node, _)) = queries.get(&key) {
                // println!("[Collision] L2 Data ({}) collided with query, sending response.", key);
                let response = Packet::Response { key, value, epoch };
                send_l2_packet(&mut *tx_lock, my_mac, get_mac(origin_node as u32), &response);
                queries.remove(&key);
            }

            recent_data.lock().unwrap().insert(key, (value, epoch, now));

            let new_packet = Packet::Data { key, value, epoch };
            send_l2_packet(&mut *tx_lock, my_mac, data_next_mac, &new_packet);
        }

        Packet::Query { key, origin_mac, mut ttl } => {
            let data = recent_data.lock().unwrap();
            if let Some(&(value, epoch, _)) = data.get(&key) {
                // println!("[Collision] L2 Query ({}) collided with data, sending response.", key);
                let response = Packet::Response { key, value, epoch };
                send_l2_packet(&mut *tx_lock, my_mac, get_mac(origin_mac as u32), &response);
            } else {
                recent_queries.lock().unwrap().insert(key, (origin_mac, now));

                ttl -= 1;
                if ttl > 0 {
                    let new_packet = Packet::Query { key, origin_mac, ttl };
                    send_l2_packet(&mut *tx_lock, my_mac, query_next_mac, &new_packet);
                }
            }
        }

        Packet::Response { .. } => {
            // println!("[Success] L2 Response received...");
        }
    }
}
