
# Project Title

A brief description of what this project does and who it's for

## 1. Experimental Purpose

The primary objective of this experimental framework is to empirically quantify the computational tax, microarchitectural efficiency, network throughput, and energy consumption of Layer 7 (L7) serialization protocols in cloud-native microservices.

By benchmarking **JSON**, **MessagePack**, **gRPC / Protocol Buffers**, **Apache Thrift**, and **FlatBuffers**, this methodology provides the empirical foundation to define a novel **Protocol Computational Multiplier ($\mathcal{M}_{\text{protocol}}$)** for Research Objective 1 (RO1 / Paper 1). This metric replaces naive raw volume models (such as MB/s) by formalizing how L7 encoding choices alter host CPU cycle allocation, memory bus utilization, and dynamic energy exhaustion in Kubernetes clusters.

---

## 2. Architecture & Role of `main.go`

The `main.go` harness acts as the unified, multi-protocol workload binary deployed across all isolated Kubernetes namespaces (`ns-json`, `ns-msgpack`, `ns-grpc`, `ns-thrift`, `ns-flatbuffers`). Its execution path is controlled dynamically at container startup via environment variables (`SERVICE_ROLE`, `PROTOCOL`, and `PAYLOAD_SIZE`).

```
                         +-----------------------------------+
                         |         Locust Load Generator     |
                         +-----------------------------------+
                                           | HTTP GET /checkout
                                           v
+-----------------------------------------------------------------------------------+
| GATEWAY SERVICE (SERVICE_ROLE=gateway)                                            |
| 1. Reads PAYLOAD_SIZE (10 or 50 items)                                             |
| 2. Generates dummy struct -> Serializes via target PROTOCOL                        |
| 3. Computes wire byte size -> Logs "BENCHMARK_PAYLOAD_BYTES: <bytes>"             |
| 4. Dispatches serialized bytes over protocol-specific transport (HTTP/1, HTTP/2, TCP)|
+-----------------------------------------------------------------------------------+
       |                  |                  |                  |                  |
   HTTP/1.1            HTTP/1.1            HTTP/2             Raw TCP           HTTP/1.1
    :8080              :8080               :50051             :9090             :8080
       |                  |                  |                  |                  |
       v                  v                  v                  v                  v
+--------------+   +--------------+   +--------------+   +--------------+   +--------------+
| JSON Worker  |   | MsgPack Work |   | gRPC Worker  |   | Thrift Work  |   | FlatBuffers  |
| json.Decode  |   | msgpack.Unm  |   | pb.ProcessOr |   | payload.Read |   | GetUOffsetT  |
+--------------+   +--------------+   +--------------+   +--------------+   +--------------+
|                          WORKER SERVICE (SERVICE_ROLE=worker)                     |
+-----------------------------------------------------------------------------------+

```

### Key Components of `main.go`

1. **Synthetic Data Engine (`generateDummyPayload`)**:
Constructs a standardized `OrderPayload` struct containing order IDs, user details, and an array of `OrderItem` objects. The array length is parameterized via `PAYLOAD_SIZE` (defaulting to 10 or 50 items), enabling systematic evaluation across payload scale quadrants.


2. **Gateway Service (`runGateway`)**:
* Accepts incoming requests from the load generator on `/checkout`.


* Converts `OrderPayload` into target wire bytes for the active protocol.


* Tracks the exact serialized wire size (`payloadBytes`) and logs `BENCHMARK_PAYLOAD_BYTES: <bytes>` to `stdout` for automated bash extraction.


* Transmits payloads over protocol-matched network layers: HTTP/1.1 POST for JSON, MessagePack, and FlatBuffers; HTTP/2 streams via gRPC; and framed raw TCP sockets for Apache Thrift.




3. **Worker Service (`runWorker`)**:
Isolates protocol-specific unmarshaling mechanics to isolate CPU cycle overhead:


* **JSON (`ns-json`)**: Decodes streaming text directly from HTTP body via `json.NewDecoder`.


* **MessagePack (`ns-msgpack`)**: Buffer-unmarshals binary tags via `msgpack.Unmarshal`.


* **gRPC / Protobuf (`ns-grpc`)**: Listens on TCP `:50051` over HTTP/2, auto-decoding varints via compiled Protobuf stubs.


* **Apache Thrift (`ns-thrift`)**: Listens on TCP `:9090`, reads 4-byte big-endian framing length headers, and unmarshals field tags via `TBinaryProtocolConf` into `thrift_order.OrderPayload`.


* **FlatBuffers (`ns-flatbuffers`)**: Reads HTTP body bytes and accesses memory offset tables in-place via `flatbuffers.GetUOffsetT(body)`, achieving zero-copy read access without heap allocations.





---

## 3. Comprehensive Experiment Methodology

The benchmark methodology uses a $2^2$ factorial design (scaling array items from 10 to 50, and user concurrency from 50 to 150) across five protocol namespaces.

```
  ==================================================================================
  PHASE 1: Baseline Initialization
  --> Query Kepler eBPF metrics for start dynamic Joules (E_start)
  ==================================================================================
                                         |
                                         v
  ==================================================================================
  PHASE 2: Wire Payload Size Extraction
  --> Trigger single request via Python urllib -> Extract BENCHMARK_PAYLOAD_BYTES (B_wire)
  ==================================================================================
                                         |
                                         v
  ==================================================================================
  PHASE 3: Workload Generation & Active Sampling Window (120 seconds)
  --> Locust Headless Engine drives concurrent user load -> Captures total requests (N_req)
  ----------------------------------------------------------------------------------
  | Parallel Telemetry Tasks (Terminal 2):                                         |
  |  a. perf record (45s @ 99Hz)  --> stack collapse --> stack_.folded             |
  |  b. perf stat   (45s PMU)     --> capture instructions & cycles --> calculate IPC|
  ----------------------------------------------------------------------------------
  ==================================================================================
                                         |
                                         v
  ==================================================================================
  PHASE 4: Metrics Aggregation & Stack Filtering
  --> Query Kepler end Joules (E_end) --> Calculate Net Joules (Delta E)
  --> Derive Req/J, MB/J, and IPC
  --> Apply regex filters to stack_.folded --> Calculate CPU Parsing Tax Ratio (R_parse)
  ==================================================================================

```

### Phase 1: Context Setup & Baseline Energy Capture

1. Identify the active Kepler pod running on host node `agent-0` and establish port-forwarding to local port `:9102`.


2. Set `TARGET_NS` environment variable to the isolated target namespace (e.g., `ns-json`).


3. Query Kepler eBPF metrics endpoint via HTTP GET to record dynamic dynamic package energy consumed prior to load injection ($E_{\text{start}}$):



$$\text{PromQL / Curl Filter: } \texttt{kepler\_container\_joules\_total\{container\_namespace="\$TARGET\_NS", mode="dynamic"\}}$$




### Phase 2: Automated Wire Payload Size Extraction

1. Dispatch a single 1-shot trigger request from the load generator pod to the Gateway endpoint using Python `urllib`.


2. Pause 1 second for container log stream buffer flushing.


3. Parse Gateway container logs via `kubectl logs` and extract `BENCHMARK_PAYLOAD_BYTES` to store exact single-request serialized wire size ($B_{\text{wire}}$).



### Phase 3: Workload Generation & Microarchitectural Profiling

1. **Load Injection**: Launch headless Locust load generator inside the cluster targeting `http://gateway-service/checkout` with random wait intervals ($0.001\text{s} - 0.005\text{s}$) for a 120-second steady state window.


2. **CPU Callstack Recording**: Concurrently execute Linux `perf record` at $99\text{ Hz}$ sampling frequency (`-F 99 -g -a`) across all CPUs for 45 seconds. Process raw trace files via `perf script` and collapse callstacks using `stackcollapse-perf.pl` to generate `stack_${TARGET_NS}.folded`.


3. **Hardware Counter Sampling**: Concurrently execute Linux `perf stat` for 45 seconds to sample hardware Performance Monitoring Units (PMUs) for raw CPU `instructions` and `cycles`.



### Phase 4: Energy & Multi-Metric Aggregation

1. Query Kepler metrics endpoint at the end of the load window to record $E_{\text{end}}$.


2. Extract total successful processed requests ($N_{\text{req}}$) from Locust runner execution statistics.


3. Compute derived metrics via shell arithmetic (`bc`):



| Metric | Mathematical Derivation / Expression | Purpose |
| --- | --- | --- |
| **Net Dynamic Energy ($\Delta E$)** | $\Delta E = E_{\text{end}} - E_{\text{start}} \quad (\text{Joules})$<br> | Macro host dynamic energy draw. |
| **Workload Energy Efficiency ($\text{Req/J}$)** | $\text{Req/J} = \frac{N_{\text{req}}}{\Delta E}$<br> | Request throughput completed per dynamic Joule. |
| **Transferred Data Volume ($\text{MB}_{\text{total}}$)** | $\text{MB}_{\text{total}} = \frac{N_{\text{req}} \times B_{\text{wire}}}{1,048,576}$<br> | Total application data transferred. |
| **Volumetric Energy Efficiency ($\text{MB/J}$)** | $\text{MB/J} = \frac{\text{MB}_{\text{total}}}{\Delta E}$<br> | Data volume processed per dynamic Joule. |
| **Instructions Per Cycle ($\text{IPC}$)** | $\text{IPC} = \frac{\text{Total CPU Instructions}}{\text{Total CPU Clock Cycles}}$<br> | Microarchitectural execution efficiency. |

### Phase 5: Stack Trace Stack Filtering & CPU Parsing Tax Ratio

Sum total CPU stack samples in `stack_${TARGET_NS}.folded` and apply narrow, protocol-specific regex filters to isolate deserialization callstacks:

$$\mathcal{R}_{\text{parse}} = \left( \frac{\text{Filtered Protocol Deserialization Samples}}{\text{Total CPU Samples}} \right) \times 100\%$$

#### Protocol Regex Filter Rules:

* **JSON (`ns-json`)**: `grep -i -E "encoding/json|json\.\(\*Decoder\)|json\.Unmarshal|jsonparser"`

* **MessagePack (`ns-msgpack`)**: `grep -i -E "vmihailenco/msgpack|msgpack\.\(\*Decoder\)|msgpack\.Unmarshal"`

* **gRPC / Protobuf (`ns-grpc`)**: `grep -i -E "protoimpl|proto\.Unmarshal|protobuf.*Unmarshal|fastReflection"`

* **Apache Thrift (`ns-thrift`)**: `grep -i -E "apache/thrift|TBinaryProtocol|thrift\.\(\*TDecoder\)|binary\.Read"`

* **FlatBuffers (`ns-flatbuffers`)**: `grep -i -E "google/flatbuffers|flatbuffers\.Get|GetUOffsetT"`


---

## 4. Execution Workflow Summary

```
+-----------------------------------------------------------------------------------+
| STEP 1: Deploy Target Quadrant (sed manifest tag -> set PAYLOAD_SIZE=10 or 50)     |
+-----------------------------------------------------------------------------------+
                                         |
                                         v
+-----------------------------------------------------------------------------------+
| STEP 2: Execute Master Script for Namespace (ns-json -> ns-flatbuffers)          |
|  1. Record E_start                                                                |
|  2. Trigger urllib -> extract B_wire                                              |
|  3. Run 120s Locust -> run perf record (45s) -> run perf stat (45s)               |
|  4. Record E_end -> calculate Req/J, MB/J, IPC                                    |
|  5. Process stackcollapse -> calculate R_parse                                    |
+-----------------------------------------------------------------------------------+
                                         |
                                         v
+-----------------------------------------------------------------------------------+
| STEP 3: Aggregate Telemetry into Composite Protocol Weight Matrix (M_protocol)    |
+-----------------------------------------------------------------------------------+

```

This experimental procedure captures micro-level CPU behavior (IPC, $\mathcal{R}_{\text{parse}}$) and macro-level host impact ($\Delta E$, Req/J, MB/J) across all candidate protocols, supplying the analytical rigor required to justify the Protocol Multiplier ($\mathcal{M}_{\text{protocol}}$) metric.