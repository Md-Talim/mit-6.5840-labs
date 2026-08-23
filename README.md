# MIT 6.5840: Distributed Systems

My implementation of the labs from [MIT 6.5840 (Spring 2026)](https://pdos.csail.mit.edu/6.824/).

Building a strong foundation in distributed systems: consensus, replication, fault tolerance, and more.

## Lab Progress

| Done | Lab                          | Link                                                                       |
| ---- | ---------------------------- | -------------------------------------------------------------------------- |
| [x]  | Lab 1 — MapReduce            | [lab-mr.html](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)           |
| [ ]  | **Lab 2** — Key/Value Server | [lab-kvsrv1.html](https://pdos.csail.mit.edu/6.824/labs/lab-kvsrv1.html)   |
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
