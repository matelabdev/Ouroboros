use pnet::datalink::{self, Channel, MacAddr};
use pnet::packet::ethernet::{EtherType, EthernetPacket, MutableEthernetPacket};
use pnet::packet::Packet as PnetPacket;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::net::TcpListener;
use std::io::{Write, BufReader, BufRead};
use tiny_http::{Header, Response, Server};
use urlencoding::decode;

const OUROBOROS_ETHERTYPE: EtherType = EtherType(0x88B5);
const HISTORY_DURATION: Duration = Duration::from_millis(1000);
const DEFAULT_TTL: u8 = 10;
const MAX_CHUNK_SIZE: usize = 1000;

#[derive(Debug, Clone)]
enum Packet {
    Data { key: String, chunk_index: u16, total_chunks: u16, data: Vec<u8>, epoch: u32 },
    Query { key: String, origin_mac: u8, ttl: u8 },
    Response { key: String, chunk_index: u16, total_chunks: u16, data: Vec<u8>, epoch: u32 },
    Heartbeat { origin_mac: u8 },
}

impl Packet {
    fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            Packet::Data { key, chunk_index, total_chunks, data, epoch } => {
                buf.push(0);
                buf.push(key.len() as u8);
                buf.extend_from_slice(key.as_bytes());
                buf.extend_from_slice(&epoch.to_be_bytes());
                buf.extend_from_slice(&chunk_index.to_be_bytes());
                buf.extend_from_slice(&total_chunks.to_be_bytes());
                buf.extend_from_slice(&(data.len() as u16).to_be_bytes());
                buf.extend_from_slice(data);
            }
            Packet::Query { key, origin_mac, ttl } => {
                buf.push(1);
                buf.push(key.len() as u8);
                buf.extend_from_slice(key.as_bytes());
                buf.push(*origin_mac);
                buf.push(*ttl);
            }
            Packet::Response { key, chunk_index, total_chunks, data, epoch } => {
                buf.push(2);
                buf.push(key.len() as u8);
                buf.extend_from_slice(key.as_bytes());
                buf.extend_from_slice(&epoch.to_be_bytes());
                buf.extend_from_slice(&chunk_index.to_be_bytes());
                buf.extend_from_slice(&total_chunks.to_be_bytes());
                buf.extend_from_slice(&(data.len() as u16).to_be_bytes());
                buf.extend_from_slice(data);
            }
            Packet::Heartbeat { origin_mac } => {
                buf.push(3);
                buf.push(*origin_mac);
            }
        }
        buf
    }

    fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.is_empty() { return None; }
        let packet_type = buf[0];
        
        if packet_type == 3 {
            if buf.len() < 2 { return None; }
            return Some(Packet::Heartbeat { origin_mac: buf[1] });
        }

        let k_len = buf[1] as usize;
        if buf.len() < 2 + k_len { return None; }
        
        let key = String::from_utf8(buf[2..2+k_len].to_vec()).unwrap_or_default();
        let offset = 2 + k_len;

        match packet_type {
            0 | 2 => {
                if buf.len() < offset + 10 { return None; }
                let epoch = u32::from_be_bytes(buf[offset..offset+4].try_into().unwrap());
                let chunk_index = u16::from_be_bytes(buf[offset+4..offset+6].try_into().unwrap());
                let total_chunks = u16::from_be_bytes(buf[offset+6..offset+8].try_into().unwrap());
                let v_len = u16::from_be_bytes(buf[offset+8..offset+10].try_into().unwrap()) as usize;
                
                if buf.len() < offset + 10 + v_len { return None; }
                let data = buf[offset+10..offset+10+v_len].to_vec();
                
                if packet_type == 0 {
                    Some(Packet::Data { key, chunk_index, total_chunks, data, epoch })
                } else {
                    Some(Packet::Response { key, chunk_index, total_chunks, data, epoch })
                }
            }
            1 => {
                if buf.len() < offset + 2 { return None; }
                let origin_mac = buf[offset];
                let ttl = buf[offset+1];
                Some(Packet::Query { key, origin_mac, ttl })
            }
            _ => None,
        }
    }
}

fn get_mac(node_id: u32) -> MacAddr {
    MacAddr::new(2, 0, 0, 0, 0, node_id as u8)
}

fn get_next_alive(current: u32, live_nodes: &HashMap<u8, (MacAddr, Instant)>, forward: bool) -> MacAddr {
    let now = Instant::now();
    // Build sorted list of alive peers (excluding self)
    let mut alive_ids: Vec<u8> = live_nodes.iter()
        .filter(|&(&id, &(_, time))| id != current as u8 && now.duration_since(time) < Duration::from_millis(500))
        .map(|(&id, _)| id)
        .collect();
    alive_ids.sort();

    if alive_ids.is_empty() {
        return MacAddr::broadcast(); // Solo node — broadcast to self
    }

    let current_id = current as u8;
    let target_id = if forward {
        // Next higher node_id, wrapping to the smallest
        alive_ids.iter().find(|&&id| id > current_id)
            .or_else(|| alive_ids.first())
            .copied().unwrap()
    } else {
        // Next lower node_id, wrapping to the largest
        alive_ids.iter().rev().find(|&&id| id < current_id)
            .or_else(|| alive_ids.last())
            .copied().unwrap()
    };

    live_nodes.get(&target_id).map(|(mac, _)| *mac).unwrap_or(MacAddr::broadcast())
}

fn send_l2_packet(tx: &mut Box<dyn datalink::DataLinkSender>, my_mac: MacAddr, _dest_mac: MacAddr, packet: &Packet) {
    let payload = packet.to_bytes();
    let frame_size = std::cmp::max(60, 14 + payload.len());
    let mut ethernet_buffer = vec![0u8; frame_size];
    let mut eth_packet = MutableEthernetPacket::new(&mut ethernet_buffer).unwrap();
    // FORCE BROADCAST: Bypasses macOS Wi-Fi driver issues with raw Unicast frames
    eth_packet.set_destination(MacAddr::broadcast());
    eth_packet.set_source(my_mac);
    eth_packet.set_ethertype(OUROBOROS_ETHERTYPE);
    eth_packet.set_payload(&payload);
    tx.send_to(eth_packet.packet(), None);
}

#[derive(Serialize, Deserialize)]
struct SnapshotItem {
    key: String,
    value: String,
}

struct SharedState {
    http_responses: Mutex<HashMap<String, (HashMap<u16, Vec<u8>>, u16)>>,
    active_data_count: Mutex<usize>,
    global_live_nodes: Mutex<HashMap<u8, (MacAddr, Instant)>>,
    all_keys: Mutex<Vec<String>>,
}

fn matches_pattern(key: &str, pattern: &str) -> bool {
    if pattern == "*" || pattern.is_empty() { return true; }
    if !pattern.contains('*') { return key == pattern; }
    let parts: Vec<&str> = pattern.split('*').collect();
    let mut remaining = key;
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() { continue; }
        if i == 0 {
            if !remaining.starts_with(part) { return false; }
            remaining = &remaining[part.len()..];
        } else if i == parts.len() - 1 {
            return remaining.ends_with(part);
        } else if let Some(pos) = remaining.find(part) {
            remaining = &remaining[pos + part.len()..];
        } else {
            return false;
        }
    }
    true
}

#[derive(Serialize)]
struct StatsResponse {
    active_packets: usize,
    capacity_limit_warning: bool,
    dead_nodes: Vec<u8>,
}

fn spawn_node(interface_name: String, node_id: u32, shared: Option<Arc<SharedState>>, enable_snapshots: bool, use_virtual_mac: bool) {
    let interfaces = datalink::interfaces();
    let interface = interfaces.into_iter().find(|i| i.name == interface_name).expect("Interface not found");

    let my_mac = if use_virtual_mac {
        get_mac(node_id)
    } else {
        interface.mac.expect("Interface must have a hardware MAC address")
    };
    
    let mut config = datalink::Config::default();
    config.read_timeout = Some(Duration::from_millis(10));

    let (mut tx, mut rx) = match datalink::channel(&interface, config) {
        Ok(Channel::Ethernet(tx, rx)) => (tx, rx),
        _ => panic!("Need Ethernet channel"),
    };

    let mut seen_epochs = HashMap::<(String, u16), u32>::new();
    let mut recent_data = HashMap::<(String, u16), (Vec<u8>, u16, u32, Instant)>::new();
    let mut recent_queries = HashMap::<String, (u8, Instant)>::new();
    let mut live_nodes = HashMap::<u8, (MacAddr, Instant)>::new();
    let mut known_dead = HashMap::<u8, bool>::new();

    let mut last_cleanup = Instant::now();
    let mut last_snapshot = Instant::now();
    // Fire an immediate heartbeat so peers discover us without waiting 100ms
    let mut last_heartbeat = Instant::now() - Duration::from_millis(200);

    loop {
        let now = Instant::now();
        
        if now.duration_since(last_heartbeat) > Duration::from_millis(100) {
            let hb = Packet::Heartbeat { origin_mac: node_id as u8 };
            send_l2_packet(&mut tx, my_mac, MacAddr::broadcast(), &hb);
            last_heartbeat = now;
            live_nodes.insert(node_id as u8, (my_mac, now));
            if let Some(ref s) = shared {
                s.global_live_nodes.lock().unwrap().insert(node_id as u8, (my_mac, now));
            }
        }

        // True Self-Healing: Data Rescue Operation for DEAD Nodes
        let alive_ids: Vec<u8> = live_nodes.iter()
            .filter(|&(&id, &(_, time))| id != node_id as u8 && now.duration_since(time) < Duration::from_millis(500))
            .map(|(&id, _)| id)
            .collect();
        for &i in &alive_ids {
            known_dead.remove(&i);
        }
        let all_peer_ids: Vec<u8> = live_nodes.keys().filter(|&&id| id != node_id as u8).copied().collect();
        for &i in &all_peer_ids {
            let is_alive = alive_ids.contains(&i);
            if !is_alive && !known_dead.contains_key(&i) {
                known_dead.insert(i, true);
                for ((k, chunk_idx), (data, total_chunks, epoch, _)) in recent_data.iter() {
                    let rescue_packet = Packet::Data { key: k.clone(), chunk_index: *chunk_idx, total_chunks: *total_chunks, data: data.clone(), epoch: *epoch };
                    send_l2_packet(&mut tx, my_mac, MacAddr::broadcast(), &rescue_packet);
                }
            }
        }

        // Biological Self-Healing: Packet Cloning for In-Flight Drops
        // Threshold is 700ms — aggressive enough to recover drops, passive enough to not flood the channel
        let mut to_regenerate = Vec::new();
        for ((k, chunk_idx), (data, total_chunks, epoch, time)) in recent_data.iter() {
            if now.duration_since(*time) > Duration::from_millis(700) && now.duration_since(*time) < HISTORY_DURATION {
                to_regenerate.push((k.clone(), *chunk_idx, data.clone(), *total_chunks, *epoch));
            }
        }
        for (k, chunk_idx, data, total_chunks, epoch) in to_regenerate {
            // Update time to prevent immediate re-cloning
            recent_data.insert((k.clone(), chunk_idx), (data.clone(), total_chunks, epoch, Instant::now()));
            let rescue_packet = Packet::Data { key: k, chunk_index: chunk_idx, total_chunks, data, epoch };
            send_l2_packet(&mut tx, my_mac, MacAddr::broadcast(), &rescue_packet);
        }

        if let Ok(frame) = rx.next() {
            if let Some(eth) = EthernetPacket::new(frame) {
                let dest = eth.get_destination();
                // Filter broadcasts from ourselves to prevent loopbacks if the switch bounces it
                let is_our_broadcast = eth.get_source() == my_mac;

                if eth.get_ethertype() == OUROBOROS_ETHERTYPE && (dest == my_mac || (dest == MacAddr::broadcast() && !is_our_broadcast)) {
                    if let Some(packet) = Packet::from_bytes(eth.payload()) {
                        let now = Instant::now();
                        
                        let data_next_mac = get_next_alive(node_id, &live_nodes, true);
                        let query_next_mac = get_next_alive(node_id, &live_nodes, false);

                        match packet {
                            Packet::Heartbeat { origin_mac } => {
                                let sender_mac = eth.get_source();
                                live_nodes.insert(origin_mac, (sender_mac, now));
                                if let Some(ref s) = shared {
                                    s.global_live_nodes.lock().unwrap().insert(origin_mac, (sender_mac, now));
                                }
                            }
                            Packet::Data { key, chunk_index, total_chunks, data, epoch } => {
                                if dest == my_mac || dest == MacAddr::broadcast() {
                                    let map_key = (key.clone(), chunk_index);
                                    if let Some(&highest) = seen_epochs.get(&map_key) {
                                        if epoch < highest { continue; }
                                    }
                                    seen_epochs.insert(map_key.clone(), epoch);

                                    if let Some(&(origin_node, _)) = recent_queries.get(&key) {
                                        let response = Packet::Response { key: key.clone(), chunk_index, total_chunks, data: data.clone(), epoch };
                                        let target_mac = live_nodes.get(&origin_node).map(|(m, _)| *m).unwrap_or(MacAddr::broadcast());
                                        send_l2_packet(&mut tx, my_mac, target_mac, &response);
                                    }

                                    recent_data.insert(map_key, (data.clone(), total_chunks, epoch, now));
                                    let new_packet = Packet::Data { key, chunk_index, total_chunks, data, epoch };
                                    send_l2_packet(&mut tx, my_mac, data_next_mac, &new_packet);
                                }
                            }
                            Packet::Query { key, origin_mac, mut ttl } => {
                                if dest == my_mac || dest == MacAddr::broadcast() {
                                    for ((k, chunk_idx), (data, total_chunks, epoch, _)) in recent_data.iter() {
                                        if k == &key {
                                            let response = Packet::Response { key: key.clone(), chunk_index: *chunk_idx, total_chunks: *total_chunks, data: data.clone(), epoch: *epoch };
                                            let target_mac = live_nodes.get(&origin_mac).map(|(m, _)| *m).unwrap_or(MacAddr::broadcast());
                                            send_l2_packet(&mut tx, my_mac, target_mac, &response);
                                        }
                                    }

                                    recent_queries.insert(key.clone(), (origin_mac, now));
                                    ttl -= 1;
                                    if ttl > 0 {
                                        let new_packet = Packet::Query { key, origin_mac, ttl };
                                        send_l2_packet(&mut tx, my_mac, query_next_mac, &new_packet);
                                    }
                                }
                            }
                            Packet::Response { key, chunk_index, total_chunks, data, .. } => {
                                if dest == my_mac || dest == MacAddr::broadcast() {
                                    if let Some(ref s) = shared {
                                        let mut map = s.http_responses.lock().unwrap();
                                        let entry = map.entry(key).or_insert_with(|| (HashMap::new(), total_chunks));
                                        entry.0.insert(chunk_index, data);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let now = Instant::now();
        if now.duration_since(last_cleanup) > Duration::from_millis(5) {
            recent_data.retain(|_, (_, _, _, time)| now.duration_since(*time) < HISTORY_DURATION);
            recent_queries.retain(|_, (_, time)| now.duration_since(*time) < HISTORY_DURATION);
            
            if let Some(ref s) = shared {
                *s.active_data_count.lock().unwrap() = recent_data.len();
                // Sync unique key names for /api/keys wildcard scan
                let unique: std::collections::HashSet<String> = recent_data.keys().map(|(k, _)| k.clone()).collect();
                *s.all_keys.lock().unwrap() = unique.into_iter().collect();
            }
            last_cleanup = now;
        }

        if enable_snapshots && node_id == 1 && now.duration_since(last_snapshot) > Duration::from_secs(10) {            let mut assembled = HashMap::<String, (HashMap<u16, Vec<u8>>, u16)>::new();
            for ((k, chunk_idx), (data, total_chunks, _, _)) in recent_data.iter() {
                let entry = assembled.entry(k.clone()).or_insert_with(|| (HashMap::new(), *total_chunks));
                entry.0.insert(*chunk_idx, data.clone());
            }

            let mut items = Vec::new();
            for (k, (chunks_map, total_chunks)) in assembled {
                if chunks_map.len() == total_chunks as usize {
                    let mut full_data = Vec::new();
                    for i in 0..total_chunks {
                        if let Some(chunk) = chunks_map.get(&i) {
                            full_data.extend_from_slice(chunk);
                        }
                    }
                    if let Ok(value) = String::from_utf8(full_data) {
                        items.push(SnapshotItem { key: k, value });
                    }
                }
            }

            if let Ok(json) = serde_json::to_string(&items) {
                let _ = fs::write("ouroboros_snapshot.json", json);
            }
            last_snapshot = now;
        }
    }
}

fn inject_string_payload(web_tx: &mut Box<dyn datalink::DataLinkSender>, node1_mac: MacAddr, dest_mac: MacAddr, key: String, value: String) {
    let bytes = value.into_bytes();
    let total_chunks = (bytes.len() as f64 / MAX_CHUNK_SIZE as f64).ceil() as u16;
    let total_chunks = if total_chunks == 0 { 1 } else { total_chunks };
    let epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u32;

    if bytes.is_empty() {
        let p = Packet::Data { key, chunk_index: 0, total_chunks: 1, data: vec![], epoch };
        send_l2_packet(web_tx, node1_mac, dest_mac, &p);
        return;
    }

    for i in 0..total_chunks {
        let start = (i as usize) * MAX_CHUNK_SIZE;
        let end = std::cmp::min(start + MAX_CHUNK_SIZE, bytes.len());
        let chunk_data = bytes[start..end].to_vec();
        
        let p = Packet::Data { key: key.clone(), chunk_index: i, total_chunks, data: chunk_data, epoch };
        send_l2_packet(web_tx, node1_mac, dest_mac, &p);
        thread::sleep(Duration::from_micros(10)); 
    }
}

fn start_tcp_ipc(interface_name: String, node_id: u32, use_virtual_mac: bool, shared: Arc<SharedState>) {
    let interfaces = datalink::interfaces();
    let interface = interfaces.into_iter().find(|i| i.name == interface_name).expect("Interface not found");
    let mut config = datalink::Config::default();
    config.read_timeout = Some(Duration::from_millis(10));
    let my_mac = if use_virtual_mac { get_mac(node_id) } else { interface.mac.expect("No MAC") };

    let listener = TcpListener::bind("0.0.0.0:8825").expect("Could not bind to port 8825 for TCP IPC");
    println!("Starting Raw TCP IPC on port 8825");

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let _ = stream.set_nodelay(true);
                let shared = Arc::clone(&shared);
                let mut tx_box = match datalink::channel(&interface, config) {
                    Ok(Channel::Ethernet(tx, _)) => tx,
                    _ => panic!("Need Ethernet channel"),
                };
                let my_mac = my_mac;
                thread::spawn(move || {
                    let mut reader = BufReader::new(stream.try_clone().unwrap());
                    let mut line = String::new();
                    while let Ok(n) = reader.read_line(&mut line) {
                        if n == 0 { break; }
                        let parts: Vec<&str> = line.trim().split_whitespace().collect();
                        if parts.is_empty() { line.clear(); continue; }

                        match parts[0].to_uppercase().as_str() {
                            "GET" => {
                                if parts.len() < 2 {
                                    let _ = stream.write_all(b"-ERR missing key\n");
                                } else {
                                    let key = parts[1].to_string();
                                    shared.http_responses.lock().unwrap().remove(&key);
                                    let map = shared.global_live_nodes.lock().unwrap();
                                    let query_next_mac = get_next_alive(node_id, &*map, false);
                                    drop(map);

                                    let p = Packet::Query { key: key.clone(), origin_mac: node_id as u8, ttl: DEFAULT_TTL };
                                    send_l2_packet(&mut tx_box, my_mac, query_next_mac, &p);

                                    let start = Instant::now();
                                    let mut assembled_value = None;
                                    while start.elapsed() < Duration::from_secs(2) {
                                        let map = shared.http_responses.lock().unwrap();
                                        if let Some((chunks_map, total_chunks)) = map.get(&key) {
                                            if chunks_map.len() == *total_chunks as usize {
                                                let mut full_data = Vec::new();
                                                for i in 0..*total_chunks {
                                                    if let Some(chunk) = chunks_map.get(&i) {
                                                        full_data.extend_from_slice(chunk);
                                                    }
                                                }
                                                if let Ok(s) = String::from_utf8(full_data) {
                                                    assembled_value = Some(s);
                                                    break;
                                                }
                                            }
                                        }
                                        drop(map);
                                        thread::sleep(Duration::from_micros(50));
                                    }

                                    if let Some(v) = assembled_value {
                                        let _ = stream.write_all(format!("+{}\n", v).as_bytes());
                                    } else {
                                        let _ = stream.write_all(b"$-1\n");
                                    }
                                }
                            }
                            "SET" => {
                                if parts.len() < 3 {
                                    let _ = stream.write_all(b"-ERR missing key or value\n");
                                } else {
                                    let key = parts[1].to_string();
                                    let value = parts[2..].join(" ");
                                    let map = shared.global_live_nodes.lock().unwrap();
                                    let dest = get_next_alive(node_id, &*map, true);
                                    drop(map);
                                    inject_string_payload(&mut tx_box, my_mac, dest, key, value);
                                    let _ = stream.write_all(b"+OK\n");
                                }
                            }
                            "KEYS" => {
                                let pattern = if parts.len() > 1 { parts[1] } else { "*" };
                                let keys = shared.all_keys.lock().unwrap();
                                let matching: Vec<&String> = keys.iter().filter(|k| matches_pattern(k, pattern)).collect();
                                let _ = stream.write_all(format!("*{}\n", matching.len()).as_bytes());
                                for k in matching {
                                    let _ = stream.write_all(format!("${}\n{}\n", k.len(), k).as_bytes());
                                }
                            }
                            _ => {
                                let _ = stream.write_all(b"-ERR unknown command\n");
                            }
                        }
                        line.clear();
                    }
                });
            }
            Err(e) => eprintln!("TCP Connection failed: {}", e),
        }
    }
}

fn main() {
    let mut interface_name = String::new();
    let mut node_id: u32 = 1;
    let mut total_nodes: u32 = 3;
    let mut enable_snapshots = false;
    let mut use_virtual_mac = false;

    let args: Vec<String> = env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--node" => { i += 1; if i < args.len() { node_id = args[i].parse().unwrap_or(1); } }
            "--total-nodes" => { i += 1; if i < args.len() { total_nodes = args[i].parse().unwrap_or(3); } }
            "--snapshots" => { enable_snapshots = true; }
            "--virtual-mac" => { use_virtual_mac = true; }
            "--virutal-mac" => { 
                eprintln!("Warning: did you mean --virtual-mac? Enabling it anyway.");
                use_virtual_mac = true; 
            }
            val => {
                if interface_name.is_empty() && !val.starts_with("--") {
                    interface_name = val.to_string();
                }
            }
        }
        i += 1;
    }

    if interface_name.is_empty() {
        eprintln!("Usage: sudo cargo run --bin server <INTERFACE> [--node ID] [--total-nodes N] [--snapshots]");
        std::process::exit(1);
    }

    println!("Starting Ouroboros L2 Node {} on interface {} (Peer Autodiscovery Active)", node_id, interface_name);

    // In virtual-mac mode, Node 1 owns all relay threads — single command, full cluster
    if use_virtual_mac && node_id == 1 {
        for relay_id in 2..=total_nodes {
            let iface = interface_name.clone();
            thread::spawn(move || spawn_node(iface, relay_id, None, false, true));
            println!("  Spawned virtual relay Node {} as background thread.", relay_id);
        }
        thread::sleep(Duration::from_millis(500));
    }

    if node_id != 1 && !use_virtual_mac {
        // Relay node: gets its own HTTP gateway on port 3000 so CLI can query locally
        let shared = Arc::new(SharedState {
            http_responses: Mutex::new(HashMap::new()),
            active_data_count: Mutex::new(0),
            global_live_nodes: Mutex::new(HashMap::new()),
            all_keys: Mutex::new(Vec::new()),
        });

        let iface = interface_name.clone();
        let s = Arc::clone(&shared);
        thread::spawn(move || spawn_node(iface, node_id, Some(s), false, use_virtual_mac));

        let iface = interface_name.clone();
        let s = Arc::clone(&shared);
        thread::spawn(move || start_tcp_ipc(iface, node_id, use_virtual_mac, s));

        // Give the ring loop a moment to warm up
        thread::sleep(Duration::from_millis(1000));

        // Open a second raw TX channel for the HTTP gateway
        let interfaces = datalink::interfaces();
        let interface = interfaces.into_iter().find(|i| i.name == interface_name).expect("Interface not found");
        let mut config2 = datalink::Config::default();
        config2.read_timeout = Some(Duration::from_millis(10));
        let mut relay_tx = match datalink::channel(&interface, config2) {
            Ok(Channel::Ethernet(tx, _)) => tx,
            _ => panic!("Need Ethernet channel"),
        };
        let relay_mac = if use_virtual_mac { get_mac(node_id) } else { interface.mac.expect("No MAC") };

        let server = (3000u16..3010)
            .find_map(|port| {
                match Server::http(format!("0.0.0.0:{}", port)) {
                    Ok(s) => { println!("Starting local HTTP gateway on http://0.0.0.0:{}", port); Some(s) }
                    Err(_) => None,
                }
            })
            .expect("Could not bind to any port in range 3000-3009");
        for request in server.incoming_requests() {
            let url = request.url().to_string();
            if url.starts_with("/api/get") {
                if let Some((_, query)) = url.split_once('?') {
                    if let Some(kv) = query.split('=').collect::<Vec<&str>>().get(1) {
                        let key = decode(kv).unwrap_or_default().to_string();
                        shared.http_responses.lock().unwrap().remove(&key);

                        let map = shared.global_live_nodes.lock().unwrap();
                        let query_next_mac = get_next_alive(node_id, &*map, false);
                        drop(map);

                        let p = Packet::Query { key: key.clone(), origin_mac: node_id as u8, ttl: DEFAULT_TTL };
                        send_l2_packet(&mut relay_tx, relay_mac, query_next_mac, &p);

                        let start = Instant::now();
                        let mut assembled_value = None;
                        while start.elapsed() < Duration::from_secs(2) {
                            let map = shared.http_responses.lock().unwrap();
                            if let Some((chunks_map, total_chunks)) = map.get(&key) {
                                if chunks_map.len() == *total_chunks as usize {
                                    let mut full_data = Vec::new();
                                    for i in 0..*total_chunks {
                                        if let Some(chunk) = chunks_map.get(&i) {
                                            full_data.extend_from_slice(chunk);
                                        }
                                    }
                                    if let Ok(s) = String::from_utf8(full_data) {
                                        assembled_value = Some(s);
                                        break;
                                    }
                                }
                            }
                            drop(map);
                            thread::sleep(Duration::from_micros(50));
                        }
                        let latency = start.elapsed().as_micros();
                        if let Some(v) = assembled_value {
                            let json = serde_json::json!({ "value": v, "latency_us": latency });
                            request.respond(Response::from_string(json.to_string())).unwrap();
                        } else {
                            request.respond(Response::from_string("{\"error\":\"Not Found or Dropped\"}")).unwrap();
                        }
                    }
                }
            } else if url.starts_with("/api/set") {
                if let Some((_, query)) = url.split_once('?') {
                    let parts: Vec<&str> = query.split('&').collect();
                    let mut k = String::new(); let mut v = String::new();
                    for part in parts {
                        if let Some((k_str, v_str)) = part.split_once('=') {
                            if k_str == "k" { k = decode(v_str).unwrap_or_default().to_string(); }
                            if k_str == "v" { v = decode(v_str).unwrap_or_default().to_string(); }
                        }
                    }
                    let map = shared.global_live_nodes.lock().unwrap();
                    let dest = get_next_alive(node_id, &*map, true);
                    drop(map);
                    let start = Instant::now();
                    inject_string_payload(&mut relay_tx, relay_mac, dest, k, v);
                    let latency = start.elapsed().as_micros();
                    request.respond(Response::from_string(format!("{{\"status\":\"ok\",\"latency_us\":{}}}", latency))).unwrap();
                }
            } else if url.starts_with("/api/keys") {
                let pattern = if let Some((_, query)) = url.split_once('?') {
                    query.split('=').nth(1)
                        .map(|p| decode(p).unwrap_or_default().to_string())
                        .unwrap_or_else(|| "*".to_string())
                } else { "*".to_string() };
                let keys = shared.all_keys.lock().unwrap();
                let matching: Vec<&String> = keys.iter().filter(|k| matches_pattern(k, &pattern)).collect();
                let json = serde_json::json!({"keys": matching, "count": matching.len()});
                request.respond(Response::from_string(json.to_string())).unwrap();
            } else {
                request.respond(Response::from_string("{\"error\":\"relay node: only /api/get, /api/set and /api/keys are supported\"}")).unwrap();
            }
        }
        return;
    }

    // Node 1 is the Gateway/Orchestrator
    let shared = Arc::new(SharedState {
        http_responses: Mutex::new(HashMap::new()),
        active_data_count: Mutex::new(0),
        global_live_nodes: Mutex::new(HashMap::new()),
        all_keys: Mutex::new(Vec::new()),
    });

    let iface = interface_name.clone();
    let s = Arc::clone(&shared);
    let snaps = enable_snapshots;
    let v_mac = use_virtual_mac;
    thread::spawn(move || spawn_node(iface, 1, Some(s), snaps, v_mac));

    let iface = interface_name.clone();
    let s = Arc::clone(&shared);
    thread::spawn(move || start_tcp_ipc(iface, 1, use_virtual_mac, s));

    let interfaces = datalink::interfaces();
    let interface = interfaces.into_iter().find(|i| i.name == interface_name).expect("Interface not found");
    let mut config = datalink::Config::default();
    config.read_timeout = Some(Duration::from_millis(10));
    let mut web_tx = match datalink::channel(&interface, config) {
        Ok(Channel::Ethernet(tx, _)) => tx,
        _ => panic!("Need Ethernet channel"),
    };
    
    let node1_mac = if use_virtual_mac {
        get_mac(1)
    } else {
        interface.mac.expect("Interface must have a hardware MAC address")
    };

    println!("Gateway orchestrated. L2 rings operational. Warming up hardware...");
    thread::sleep(Duration::from_millis(1000));

    if enable_snapshots {
        if let Ok(data) = fs::read_to_string("ouroboros_snapshot.json") {
            if let Ok(items) = serde_json::from_str::<Vec<SnapshotItem>>(&data) {
                println!("Restoring {} items from snapshot...", items.len());
                for item in items {
                    let map = shared.global_live_nodes.lock().unwrap();
                    let dest_mac = get_next_alive(1, &*map, true);
                    drop(map);
                    inject_string_payload(&mut web_tx, node1_mac, dest_mac, item.key, item.value);
                }
                println!("Snapshot restoration complete.");
            }
        }
    }

    let server = (3000u16..3010)
        .find_map(|port| {
            match Server::http(format!("0.0.0.0:{}", port)) {
                Ok(s) => { println!("Starting Web Dashboard on http://0.0.0.0:{}", port); Some(s) }
                Err(_) => None,
            }
        })
        .expect("Could not bind to any port in range 3000-3009");
    let html = include_str!("index.html");

    for request in server.incoming_requests() {
        let url = request.url().to_string();
        
        if url == "/" {
            let response = Response::from_string(html).with_header(Header::from_bytes(&b"Content-Type"[..], &b"text/html; charset=utf-8"[..]).unwrap());
            request.respond(response).unwrap();
        } else if url.starts_with("/api/set") {
            if let Some((_, query)) = url.split_once('?') {
                let parts: Vec<&str> = query.split('&').collect();
                let mut k = String::new(); let mut v = String::new();
                for part in parts {
                    if let Some((k_str, v_str)) = part.split_once('=') {
                        if k_str == "k" { k = decode(v_str).unwrap_or_default().to_string(); }
                        if k_str == "v" { v = decode(v_str).unwrap_or_default().to_string(); }
                    }
                }
                
                let map = shared.global_live_nodes.lock().unwrap();
                let data_next_mac = get_next_alive(1, &*map, true);
                drop(map);

                let start = Instant::now();
                inject_string_payload(&mut web_tx, node1_mac, data_next_mac, k, v);
                let latency = start.elapsed().as_micros();
                request.respond(Response::from_string(format!("{{\"status\":\"ok\",\"latency_us\":{}}}", latency))).unwrap();
            }
        } else if url.starts_with("/api/get") {
            if let Some((_, query)) = url.split_once('?') {
                if let Some(kv) = query.split('=').collect::<Vec<&str>>().get(1) {
                    let key = decode(kv).unwrap_or_default().to_string();
                    let start = Instant::now();
                    
                    shared.http_responses.lock().unwrap().remove(&key);

                    let map = shared.global_live_nodes.lock().unwrap();
                    let query_next_mac = get_next_alive(1, &*map, false);
                    drop(map);

                    let p = Packet::Query { key: key.clone(), origin_mac: 1, ttl: DEFAULT_TTL };
                    send_l2_packet(&mut web_tx, node1_mac, query_next_mac, &p);
                    
                    let mut assembled_value = None;
                    let wait_start = Instant::now();

                    while wait_start.elapsed() < Duration::from_secs(2) {
                        let map = shared.http_responses.lock().unwrap();
                        if let Some((chunks_map, total_chunks)) = map.get(&key) {
                            if chunks_map.len() == *total_chunks as usize {
                                let mut full_data = Vec::new();
                                for i in 0..*total_chunks {
                                    if let Some(chunk) = chunks_map.get(&i) {
                                        full_data.extend_from_slice(chunk);
                                    }
                                }
                                if let Ok(s) = String::from_utf8(full_data) {
                                    assembled_value = Some(s);
                                    break;
                                }
                            }
                        }
                        drop(map);
                        thread::sleep(Duration::from_micros(50));
                    }
                    
                    let latency = start.elapsed().as_micros();
                    if let Some(v) = assembled_value {
                        let json = serde_json::json!({
                            "value": v,
                            "latency_us": latency
                        });
                        request.respond(Response::from_string(json.to_string())).unwrap();
                    } else {
                        request.respond(Response::from_string("{\"error\":\"Not Found or Dropped\"}")).unwrap();
                    }
                }
            }
        } else if url.starts_with("/api/kill") {
            request.respond(Response::from_string("{\"status\":\"API Kill Disabled in Multi-Machine Mode\"}")).unwrap();
        } else if url.starts_with("/api/keys") {
            let pattern = if let Some((_, query)) = url.split_once('?') {
                query.split('=').nth(1)
                    .map(|p| decode(p).unwrap_or_default().to_string())
                    .unwrap_or_else(|| "*".to_string())
            } else { "*".to_string() };
            let keys = shared.all_keys.lock().unwrap();
            let matching: Vec<&String> = keys.iter().filter(|k| matches_pattern(k, &pattern)).collect();
            let json = serde_json::json!({"keys": matching, "count": matching.len()});
            request.respond(Response::from_string(json.to_string())).unwrap();
        } else if url == "/api/stats" {
            let count = *shared.active_data_count.lock().unwrap();
            let map = shared.global_live_nodes.lock().unwrap();
            let mut dead_nodes: Vec<u8> = Vec::new();
            // Dead = nodes we've seen before but haven't heartbeated in 500ms
            for (&id, &(_, time)) in map.iter() {
                if id != 1 && Instant::now().duration_since(time) >= Duration::from_millis(500) {
                    dead_nodes.push(id);
                }
            }
            drop(map);

            let stats = StatsResponse {
                active_packets: count,
                capacity_limit_warning: count > 800,
                dead_nodes,
            };
            let json = serde_json::to_string(&stats).unwrap();
            request.respond(Response::from_string(json)).unwrap();
        } else {
            request.respond(Response::from_string("404")).unwrap();
        }
    }
}
