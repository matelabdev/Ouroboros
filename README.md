# impactDB 🚀
**The Layer 2 "Data-in-Flight" Storage Engine**

impactDB is a conceptual, extremely high-performance storage engine that completely bypasses persistent storage (RAM/Disk) and the traditional OS network stack (TCP/UDP/IP). Instead, it stores data **in the network cables**, continuously orbiting Ethernet frames in a Ring Topology using raw Layer 2 packets.

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

2. **Payload Fragmentation (TCP over L2):**
   Data strings exceeding the Ethernet MTU (~1500 bytes) are automatically fragmented into chunks, routed independently, and instantly reassembled in-memory upon query interception.

3. **Fault Tolerance & Self-Healing (Heartbeats):**
   The cluster maintains resilience through `100ms` Layer 2 Heartbeats. If a node goes offline, the remaining nodes detect the failure within `500ms`, dynamically re-route the ring, and execute a **Rescue Operation** to re-inject in-flight data, guaranteeing zero data loss.

4. **Ephemeral Snapshots:**
   An optional `--snapshots` mode allows the orchestrator to dump the entire "Data-in-Flight" stream to an NVMe disk every 10 seconds. Upon reboot, the cluster injects the snapshot back into the wire at light speed, restoring the orbital network.

5. **Glassmorphic Web Dashboard:**
   An integrated `tiny_http` server hosts a responsive, futuristic UI on `http://localhost:3000` to visualize packet flow, simulate node crashes, and measure microsecond latencies.

## 🚀 Getting Started

### System Requirements
Because impactDB operates at OSI Layer 2 using raw sockets, it requires specialized low-level networking libraries:
- **macOS:** `libpcap` is included by default. No extra networking libraries required.
- **Linux:** Requires `libpcap-dev` (Debian/Ubuntu: `sudo apt install libpcap-dev`).
- **Permissions:** Root (`sudo`) access is strictly required to bind to Raw Ethernet Sockets.
- **Environment:** Rust Toolchain (`cargo`) installed.

### Installation & Run
```bash
git clone git@github.com:matelabdev/impactDB.git
cd impactDB

### 1. Starting the Orchestrator (Node 1)
```bash
sudo cargo run --bin server <INTERFACE> --node 1 --total-nodes 2
```
*Note: To run multiple nodes on a single physical machine for testing, append `--virtual-mac`.*

### 2. Starting a Relay Node (Node 2)
```bash
sudo cargo run --bin server <INTERFACE> --node 2 --total-nodes 2
```

### 3. Using the Standalone CLI (impactdb)
You can inject and intercept data directly via the command-line, completely bypassing the Web Gateway. The CLI operates natively at Layer 2:
```bash
# Inject Data
sudo cargo run --bin impactdb <INTERFACE> set my_key "Hello L2 Network!"

# Retrieve Data
sudo cargo run --bin impactdb <INTERFACE> get my_key
```

## 🏗 System Architecture
```text
           [ Node 1 (Gateway/Orchestrator) ]
             ↗                       ↘
       (L2 EtherType 0x88B5)    (L2 EtherType 0x88B5)
           /                           \
    [ Node 3 ]  <------------------  [ Node 2 ]
```
