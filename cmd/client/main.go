package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"distributed-kv-store/internal/raft"
	"distributed-kv-store/internal/transport"
)

func main() {
	nodesFlag := flag.String("nodes", "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003", "comma-separated node addresses")
	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		usage()
	}
	nodes := strings.Split(*nodesFlag, ",")
	client := transport.NewClient()
	op := strings.ToLower(args[0])
	var req *raft.ClientRequest
	switch op {
	case "put":
		if len(args) != 3 {
			usage()
		}
		req = &raft.ClientRequest{Op: "put", Key: args[1], Value: args[2]}
	case "get":
		if len(args) != 2 {
			usage()
		}
		req = &raft.ClientRequest{Op: "get", Key: args[1]}
	case "delete":
		if len(args) != 2 {
			usage()
		}
		req = &raft.ClientRequest{Op: "delete", Key: args[1]}
	case "health":
		for _, addr := range nodes {
			ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
			h, err := client.Health(ctx, addr)
			cancel()
			if err != nil {
				fmt.Printf("%s: unavailable (%v)\n", addr, err)
				continue
			}
			fmt.Printf("%s: state=%s term=%d leader=%s\n", h.ID, h.State, h.Term, h.LeaderID)
		}
		return
	default:
		usage()
	}
	for attempt := 0; attempt < 8; attempt++ {
		for _, addr := range nodes {
			ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
			resp, err := client.ClientCall(ctx, addr, req)
			cancel()
			if err != nil {
				continue
			}
			if resp.OK {
				if op == "get" {
					fmt.Println(resp.Value)
				} else {
					fmt.Println("OK")
				}
				return
			}
			if resp.LeaderAddr != "" {
				nodes = append([]string{resp.LeaderAddr}, nodes...)
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
	fmt.Fprintln(os.Stderr, "request failed: no leader responded successfully")
	os.Exit(1)
}
func usage() {
	fmt.Fprintln(os.Stderr, "usage: client [-nodes addr1,addr2,addr3] put key value | get key | delete key | health")
	os.Exit(2)
}
