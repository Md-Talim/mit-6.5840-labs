# MIT 6.5840: Distributed Systems

My implementation of the labs from [MIT 6.5840 (Spring 2026)](https://pdos.csail.mit.edu/6.824/).

Building a strong foundation in distributed systems: consensus, replication, fault tolerance, and more.

## Lab Progress

| Done | Lab                          | Link                                                                       |
| ---- | ---------------------------- | -------------------------------------------------------------------------- |
| [x]  | Lab 1 — MapReduce            | [lab-mr.html](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)           |
| [x]  | **Lab 2** — Key/Value Server | [lab-kvsrv1.html](https://pdos.csail.mit.edu/6.824/labs/lab-kvsrv1.html)   |
| [ ]  | **Lab 3** — Raft             | [lab-raft1.html](https://pdos.csail.mit.edu/6.824/labs/lab-raft1.html)     |
| [ ]  | 3A — Leader election         |                                                                            |
| [ ]  | 3B — Log                     |                                                                            |
| [ ]  | 3C — Persistence             |                                                                            |
| [ ]  | 3D — Log compaction          |                                                                            |
| [ ]  | **Lab 4** — KV Raft          | [lab-kvraft1.html](https://pdos.csail.mit.edu/6.824/labs/lab-kvraft1.html) |
| [ ]  | 4A                           |                                                                            |
| [ ]  | 4B+C                         |                                                                            |
| [ ]  | **Lab 5** — Sharded KV       | [lab-shard1.html](https://pdos.csail.mit.edu/6.824/labs/lab-shard1.html)   |
| [ ]  | 5A                           |                                                                            |
| [ ]  | 5B+C+D                       |                                                                            |

## Lab 1: MapReduce

A distributed MapReduce implementation in Go, consisting of a coordinator and multiple workers communicating via RPC.

### What it does

- Workers request tasks from the coordinator in a loop
- **Map phase**: Each worker reads an input file, applies the map function, and writes intermediate files (`mr-X-Y`) partitioned by `ihash(key) % nReduce`
- **Reduce phase**: Each worker reads all intermediate files for its bucket, sorts by key, applies the reduce function, and writes output to `mr-out-Y`
- Handles worker crashes via 10-second timeout-based task reassignment

### How to run

**Prerequisites**: Go 1.17+

```bash
cd src
```

**Run all MapReduce tests:**

```bash
make mr
```

**Run a specific test:**

```bash
make RUN="-run Wc" mr          # Word count
make RUN="-run Indexer" mr     # Indexer
make RUN="-run MapParallel" mr # Map parallelism
make RUN="-run ReduceParallel" mr # Reduce parallelism
make RUN="-run JobCount" mr    # Job count
make RUN="-run EarlyExit" mr   # Early exit
make RUN="-run CrashWorker" mr # Crash recovery
```

**Run manually:**

```bash
# Build the word-count plugin
cd main
go build -buildmode=plugin ../mrapps/wc.go

# Start coordinator (in one terminal)
rm mr-out*
go run mrcoordinator.go sock123 pg-*.txt

# Start one or more workers (in other terminals)
go run mrworker.go wc.so sock123

# Check output
cat mr-out-* | sort
```

### Implementation files

| File                                         | Purpose                                         |
| -------------------------------------------- | ----------------------------------------------- |
| [`mr/coordinator.go`](src/mr/coordinator.go) | Task assignment, state tracking, crash recovery |
| [`mr/worker.go`](src/mr/worker.go)           | Task execution (map/reduce), file I/O           |
| [`mr/rpc.go`](src/mr/rpc.go)                 | RPC message definitions                         |

### Key design decisions

- **Three-state task model**: `unstarted → in-progress → completed` prevents duplicate assignment
- **Timeout-based crash recovery**: Coordinator reassigns tasks after 10 seconds with no response
- **JSON-encoded intermediate files**: Map output written as `mr-X-Y` for correct reduce bucket partitioning
- **Mutex-protected coordinator**: All shared state guarded by `sync.Mutex` for concurrent RPC handling
- **Graceful worker exit**: Workers exit when coordinator is unreachable or signals job completion

## Lab 2: Key/Value Server

A versioned in-memory key/value server and client, extended to handle unreliable RPC communication and distributed lock acquisition.

### What it does

- Provides `Get` and versioned `Put` operations over RPC
- Client retries RPCs when the network drops a request or response
- Handles ambiguous `Put` results using `ErrMaybe`
- Implements a distributed lock using the key/value server
- Handles ambiguous lock acquisition by checking whether the client became the lock owner
- Passes the complete Lab 2 test suite, including unreliable-network tests with multiple concurrent clients

### Implementation files

| File                                             | Purpose                                                                        |
| ------------------------------------------------ | ------------------------------------------------------------------------------ |
| [`kvsrv1/server.go`](src/kvsrv1/server.go)       | Key/value server implementing `Get` and versioned `Put`                        |
| [`kvsrv1/client.go`](src/kvsrv1/client.go)       | `Clerk` responsible for making RPC calls and handling unreliable communication |
| [`kvsrv1/lock/lock.go`](src/kvsrv1/lock/lock.go) | Distributed lock acquisition and release                                       |

### Key design decisions

- **Versioned writes**: `Put` uses the client's expected version to detect concurrent updates
- **RPC retries**: The client retries when an RPC call fails because of an unreliable network
- **Ambiguous writes**: A failed `Put` may have been applied by the server even if the response was lost, so subsequent failures can be reported as `ErrMaybe`
- **Lock ownership**: Each lock client has a unique client ID stored as the lock value
- **Ambiguous lock acquisition**: After `ErrMaybe`, the client checks the current lock owner; if it is itself, the acquisition succeeded
- **Concurrent clients**: The implementation handles multiple clients competing for the same lock over an unreliable network
