# Distributed Key-Value Store (Go)

An educational three-node distributed key-value store built around Raft-style leader election and log replication, with gRPC transport, JSON-backed local persistence, a CLI client, health checks, and tests.

## Current scope

Implemented in this version:
- Thread-safe key/value state machine
- Raft-style terms, follower/candidate/leader states, vote requests, election timeouts, heartbeats
- AppendEntries-style log replication and majority-based commit
- gRPC RPC transport (JSON codec over gRPC; messages are Go structs rather than generated protobuf messages)
- Local JSON persistence of term, vote, log, commit index, and state machine
- CLI client that tries configured nodes and follows leader hints
- Unit tests for store and core Raft vote behavior

Still to add before claiming the entire resume description:
- PostgreSQL-backed durable storage (current persistence is JSON files)
- Production-grade Raft edge cases, joint consensus, log compaction/snapshot installation, and extensive chaos/integration testing
- Docker Compose and operational metrics

This is a learning implementation, not production-grade consensus software. Do not describe unimplemented items as completed.

## Requirements

- Go 1.23+
- Network access for `go mod tidy` to download gRPC dependencies

## Build and test

```bash
go mod tidy
go test -race ./...
go build ./cmd/node
go build ./cmd/client
```

## Run a local 3-node cluster

Open three Terminal windows from this repository. Run:

```bash
NODE_ID=node1 NODE_ADDR=127.0.0.1:7001 PEERS=node2=127.0.0.1:7002,node3=127.0.0.1:7003 DATA_DIR=./data/node1 go run ./cmd/node
```

```bash
NODE_ID=node2 NODE_ADDR=127.0.0.1:7002 PEERS=node1=127.0.0.1:7001,node3=127.0.0.1:7003 DATA_DIR=./data/node2 go run ./cmd/node
```

```bash
NODE_ID=node3 NODE_ADDR=127.0.0.1:7003 PEERS=node1=127.0.0.1:7001,node2=127.0.0.1:7002 DATA_DIR=./data/node3 go run ./cmd/node
```

In a fourth Terminal window:

```bash
go run ./cmd/client -nodes 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 put greeting hello
go run ./cmd/client -nodes 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 get greeting
go run ./cmd/client -nodes 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 health
```

The first election may take a moment. This implementation is intentionally compact for study; verify behavior locally before relying on it in a resume or interview.
