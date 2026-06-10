# Fault-Tolerant Serverless Java Execution Cluster

A distributed systems project built incrementally across 5 stages for **Introduction to Distributed Systems (COM 3800)**. The final system is a scalable, fault-tolerant cluster that accepts Java source code from clients via HTTP, compiles and executes it on worker nodes, and returns the output — similar in concept to AWS Lambda or Google Cloud Functions.

---

## Architecture Overview

```
          Client (HTTP)
               │
               ▼
        ┌─────────────┐
        │   Gateway   │  ← HTTP endpoint, request caching, observer in cluster
        └──────┬──────┘
               │ TCP
               ▼
        ┌─────────────┐
        │   Leader    │  ← Elected via ZooKeeper-style algorithm; distributes work
        └──────┬──────┘
      ┌────────┼────────┐
      ▼        ▼        ▼
  Worker    Worker    Worker   ← Compile & run Java code, return results
```

All internal cluster coordination uses **UDP** (leader election, gossip heartbeats). Client requests flow over **TCP** between the gateway and leader, and over **HTTP** between the client and gateway.

---

## Stages

### Stage 1 — Client & Server
A single HTTP server that accepts Java source code via `POST /compileandrun`, compiles and runs it using the JDK's `javax.tools.JavaCompiler`, and returns the output. A matching client sends requests and receives responses.

### Stage 2 — Distributed Leader Election
A cluster of peer servers that communicate over **UDP** to elect a leader using the **ZooKeeper leader election algorithm** (simplified — based on server ID only). Each server runs 3 threads: a main server thread, a UDP sender, and a UDP receiver.

### Stage 3 — Master-Worker
The elected leader acts as **master**: it accepts client requests and distributes them to worker nodes on a **round-robin** basis using request IDs to track work asynchronously. Workers compile and run the Java code and return results to the master.

### Stage 4 — Architectural Completeness
Adds a **Gateway server** as the fixed public HTTP endpoint, sitting in front of whichever node is currently the leader. Key additions:
- Gateway acts as an **observer** in leader election (no vote, but tracks the winner)
- **Response caching** at the gateway — identical requests are served from cache without hitting the cluster
- Full **protocol separation**: UDP for election, TCP for cluster work, HTTP for client-gateway communication
- Per-thread **file-based logging** across all nodes

### Stage 5 — Fault Tolerance
Adds full fault tolerance using **Gossip-Style Heartbeats**:
- Every node gossips heartbeat sequences to detect failures autonomously (no central monitor)
- **Dead follower**: removed from work rotation; any in-flight work reassigned to another worker
- **Dead leader**: followers switch to `LOOKING`, run a new election; gateway queues incoming requests until a new leader is elected; no client requests are lost
- HTTP endpoints on each node to retrieve summary and verbose log files
- A `demo5.sh` bash script that starts a full 7-node + gateway cluster, sends requests, kills a follower, kills the leader, and verifies recovery end-to-end

---

## Key Technical Details

| Concern | Approach |
|---|---|
| Leader election | ZooKeeper-style (epoch + server ID) over UDP |
| Failure detection | Gossip-style heartbeats |
| Client protocol | HTTP (JDK `com.sun.net.httpserver`) |
| Cluster protocol | TCP for work, UDP for coordination |
| Code execution | `javax.tools.JavaCompiler` + Java Reflection |
| Caching | `ConcurrentHashMap` of request hash → response |
| Concurrency | `LinkedBlockingQueue`, `ConcurrentHashMap`, `AtomicLong`, thread pools |
| Build | Maven (JDK 21) |
| Testing | JUnit 5 |

---

## Running the Demo

```bash
cd stage5
bash demo5.sh
```

This will build the project, start a cluster of 8 JVMs (7 peer servers + 1 gateway), run through failure scenarios, and print all results to both stdout and `output.log`.

---

## Project Structure

```
com3800/
├── stage1/   # HTTP client & server, JavaRunner
├── stage2/   # Leader election over UDP
├── stage3/   # Master-worker task distribution
├── stage4/   # Gateway, caching, TCP, logging
└── stage5/   # Fault tolerance, gossip heartbeats, demo
```

Each stage is a fully self-contained Maven project.
