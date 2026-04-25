# impactDB 🚀
**The Layer 2 "Data-in-Flight" Storage Engine**

impactDB is an experimental, extremely high-performance storage engine that completely bypasses persistent storage (RAM/Disk) and the traditional OS network stack (TCP/UDP/IP). Instead, it stores data **in the network cables**, continuously orbiting Ethernet frames in a Ring Topology using raw Layer 2 packets.

---

## ⚡ Core Philosophy
Why store data when you can just keep it moving?
In impactDB, data is injected into the network as custom Ethernet frames (EtherType `0x88B5`). The nodes in the cluster act purely as relay stations, instantly forwarding the data to the next node.
- **Storage Medium:** Network Wire (Fiber/Copper)
- **Persistence:** None (unless Snapshotting is enabled)
- **Latency:** Sub-100 microseconds (µs) hardware round-trips.

## 🛠 Features

1. **Kernel Bypass (Layer 2 Nirvana):**
   Powered by `pnet`, impactDB communicates directly with the NIC using Raw Sockets, skipping the Linux Kernel's network stack entirely for nanosecond-level packet processing.

2. **Payload Fragmentation:**
   Data strings exceeding 1000 bytes are automatically fragmented into chunks, routed independently, and reassembled in-memory upon query interception.

3. **Fault Tolerance & Self-Healing (Heartbeats):**
   The cluster maintains resilience through `100ms` Layer 2 Heartbeats. If a node goes offline, the remaining nodes detect the failure within `500ms`, dynamically re-route the ring, and execute a **Rescue Operation** to re-inject in-flight data.

4. **Biological Packet Cloning:**
   If a packet fails to complete its ring orbit within 200ms (Wi-Fi drop, AP congestion), every node autonomously re-broadcasts a fresh clone of the packet. The cluster self-regulates clone frequency based on network health.

5. **Ephemeral Snapshots:**
   An optional `--snapshots` mode dumps the entire in-flight stream to disk every 10 seconds. Upon reboot, the cluster restores the snapshot back onto the wire.

6. **True Hardware MAC Discovery:**
   Each node reads its physical NIC's MAC address and propagates it to peers via Heartbeat frames. No hardcoded addresses — dynamic ARP-like peer discovery at Layer 2.

7. **HTTP Gateway on every node:**
   Every node (orchestrator and relay) exposes a local HTTP API on port `3000`. This allows clients and CLI tools to interact with the ring without competing for raw socket access.

8. **Glassmorphic Web Dashboard:**
   Node 1 hosts a Vue.js Web UI on `http://localhost:3000` to visualize packet flow, monitor node health, and measure microsecond latencies.

## 🚀 Getting Started

### System Requirements
- **macOS:** `libpcap` is included by default.
- **Linux:** `sudo apt install libpcap-dev`
- **Permissions:** Root (`sudo`) access is required to bind Raw Ethernet Sockets.
- **Rust:** Install via [rustup.rs](https://rustup.rs)

### Installation

```bash
git clone git@github.com:matelabdev/impactDB.git
cd impactDB
```

### 1. Starting the Orchestrator (Node 1 — Gateway + Web UI)
```bash
sudo cargo run --bin server <INTERFACE> --node 1 --total-nodes 2
```
> Web dashboard available at `http://localhost:3000`
> Replace `<INTERFACE>` with your network interface (`en0`, `eth0`, `bridge0`, etc.)

*To run multiple nodes on a single machine for testing, append `--virtual-mac`.*

### 2. Starting a Relay Node (Node 2+)
```bash
sudo cargo run --bin server <INTERFACE> --node 2 --total-nodes 2
```
> Each relay node also exposes a local HTTP gateway at `http://localhost:3000`

### 3. Using the CLI (impactdb)
The CLI connects to the local node's HTTP gateway (`localhost:3000`) — no `sudo` required:

```bash
# Build the CLI
cargo build --bin impactdb

# Set a value
cargo run --bin impactdb set my_key "Hello L2 Network!"

# Get a value
cargo run --bin impactdb get my_key
```

Expected output:
```
"Hello L2 Network!"
(Latency: 380 µs)
```

## 🏗 System Architecture
```text
           [ Node 1 (Gateway/Orchestrator) ]
             ↗                       ↘
       (L2 EtherType 0x88B5)    (L2 EtherType 0x88B5)
           /                           \
    [ Node 3 ]  <------------------  [ Node 2 ]
```
