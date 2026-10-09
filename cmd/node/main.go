package main

import (
	"log"
	"net"
	"os"
	"strings"
	"time"

	"distributed-kv-store/internal/raft"
	"distributed-kv-store/internal/transport"
	"google.golang.org/grpc"
)

func main() {
	id := env("NODE_ID", "node1")
	addr := env("NODE_ADDR", "127.0.0.1:7001")
	dataDir := env("DATA_DIR", "./data/"+id)
	peers := parsePeers(os.Getenv("PEERS"))
	network := transport.NewClient()
	node, err := raft.NewNode(raft.Config{ID: id, Addr: addr, Peers: peers, DataDir: dataDir, ElectionMin: 700 * time.Millisecond, ElectionMax: 1200 * time.Millisecond, Heartbeat: 150 * time.Millisecond}, network)
	if err != nil {
		log.Fatal(err)
	}
	node.Start()
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	server := grpc.NewServer()
	transport.Register(server, node)
	log.Printf("node %s listening on %s; peers=%v", id, addr, peers)
	if err := server.Serve(lis); err != nil {
		log.Fatal(err)
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func parsePeers(raw string) map[string]string {
	out := map[string]string{}
	for _, item := range strings.Split(raw, ",") {
		if item == "" {
			continue
		}
		p := strings.SplitN(item, "=", 2)
		if len(p) == 2 {
			out[p[0]] = p[1]
		}
	}
	return out
}
