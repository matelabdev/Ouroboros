use pnet::datalink::{self, Channel, MacAddr};
use pnet::packet::ethernet::{EtherType, EthernetPacket, MutableEthernetPacket};
use pnet::packet::Packet as PnetPacket;
use std::collections::HashMap;
use std::env;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::thread;

const IMPACT_ETHERTYPE: EtherType = EtherType(0x88B5);
const MAX_CHUNK_SIZE: usize = 1000;
const DEFAULT_TTL: u8 = 10;

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

fn send_l2_packet(tx: &mut Box<dyn datalink::DataLinkSender>, my_mac: MacAddr, packet: &Packet) {
    let payload = packet.to_bytes();
    let frame_size = std::cmp::max(60, 14 + payload.len());
    let mut ethernet_buffer = vec![0u8; frame_size];
    let mut eth_packet = MutableEthernetPacket::new(&mut ethernet_buffer).unwrap();
    eth_packet.set_destination(MacAddr::broadcast());
    eth_packet.set_source(my_mac);
    eth_packet.set_ethertype(IMPACT_ETHERTYPE);
    eth_packet.set_payload(&payload);
    tx.send_to(eth_packet.packet(), None);
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        eprintln!("Usage:");
        eprintln!("  impactdb <interface> get <key>");
        eprintln!("  impactdb <interface> set <key> <value>");
        std::process::exit(1);
    }

    let interface_name = &args[1];
    let command = &args[2];
    let key = &args[3];

    let interfaces = datalink::interfaces();
    let interface = interfaces.into_iter().find(|i| i.name == *interface_name).expect("Interface not found");
    let my_mac = interface.mac.expect("Interface must have a hardware MAC address");

    let mut config = datalink::Config::default();
    config.read_timeout = Some(Duration::from_millis(10));

    let (mut tx, mut rx) = match datalink::channel(&interface, config) {
        Ok(Channel::Ethernet(tx, rx)) => (tx, rx),
        _ => panic!("Need Ethernet channel"),
    };

    // Use CLI Node ID 255 (Ephemeral Client)
    let cli_node_id = 255;

    if command == "set" {
        if args.len() < 5 {
            eprintln!("Error: Value is missing for set command.");
            std::process::exit(1);
        }
        let value = &args[4];
        let bytes = value.clone().into_bytes();
        let total_chunks = (bytes.len() as f64 / MAX_CHUNK_SIZE as f64).ceil() as u16;
        let total_chunks = if total_chunks == 0 { 1 } else { total_chunks };
        let epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u32;

        let start = Instant::now();

        if bytes.is_empty() {
            let p = Packet::Data { key: key.clone(), chunk_index: 0, total_chunks: 1, data: vec![], epoch };
            send_l2_packet(&mut tx, my_mac, &p);
        } else {
            for i in 0..total_chunks {
                let start_idx = (i as usize) * MAX_CHUNK_SIZE;
                let end_idx = std::cmp::min(start_idx + MAX_CHUNK_SIZE, bytes.len());
                let chunk_data = bytes[start_idx..end_idx].to_vec();
                let p = Packet::Data { key: key.clone(), chunk_index: i, total_chunks, data: chunk_data, epoch };
                send_l2_packet(&mut tx, my_mac, &p);
                thread::sleep(Duration::from_micros(10));
            }
        }
        println!("OK ({} µs)", start.elapsed().as_micros());

    } else if command == "get" {
        let p = Packet::Query { key: key.clone(), origin_mac: cli_node_id, ttl: DEFAULT_TTL };
        
        let start = Instant::now();
        send_l2_packet(&mut tx, my_mac, &p);

        let mut chunks_map = HashMap::<u16, Vec<u8>>::new();
        let mut expected_chunks = 0;

        loop {
            if start.elapsed() > Duration::from_secs(2) {
                eprintln!("(nil) - Query Missed: Not Found or Dropped");
                std::process::exit(1);
            }

            if let Ok(frame) = rx.next() {
                if let Some(eth) = EthernetPacket::new(frame) {
                    if eth.get_ethertype() == IMPACT_ETHERTYPE {
                        if let Some(packet) = Packet::from_bytes(eth.payload()) {
                            match packet {
                                Packet::Response { key: r_key, chunk_index, total_chunks, data, .. } => {
                                    if r_key == *key {
                                        chunks_map.insert(chunk_index, data);
                                        expected_chunks = total_chunks;

                                        if chunks_map.len() == expected_chunks as usize {
                                            let mut full_data = Vec::new();
                                            for i in 0..expected_chunks {
                                                if let Some(chunk) = chunks_map.get(&i) {
                                                    full_data.extend_from_slice(chunk);
                                                }
                                            }
                                            if let Ok(s) = String::from_utf8(full_data) {
                                                println!("\"{}\"", s);
                                                println!("(Latency: {} µs)", start.elapsed().as_micros());
                                                return;
                                            }
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }
    } else {
        eprintln!("Unknown command. Use 'get' or 'set'.");
    }
}
