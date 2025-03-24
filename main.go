package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"raft-kv-store/kvstore"
	"raft-kv-store/raft"
	"strings"
	"time"
)

func main() {
	nodeID := flag.Int("id", 0, "Node ID")
	clusterSize := flag.Int("size", 3, "Cluster size")
	flag.Parse()

	rand.Seed(time.Now().UnixNano())

	// Create all channels first
	applyChs := make([]chan raft.ApplyMsg, *clusterSize)
	clientChs := make([]chan raft.ClientRequest, *clusterSize)
	for i := range applyChs {
		applyChs[i] = make(chan raft.ApplyMsg, 100)
		clientChs[i] = make(chan raft.ClientRequest, 100)
	}

	// Create all nodes with temporary nil peers
	raftNodes := make([]*raft.RaftNode, *clusterSize)
	for i := 0; i < *clusterSize; i++ {
		persister := raft.MakePersister()
		raftNodes[i] = raft.NewRaftNode(i, nil, persister, applyChs[i])
		log.Printf("Created node %d", i)
	}

	// Now connect peers properly
	for i := 0; i < *clusterSize; i++ {
		peers := make([]*raft.RaftNode, 0, *clusterSize-1)
		for j := 0; j < *clusterSize; j++ {
			if i != j {
				peers = append(peers, raftNodes[j])
			}
		}
		raftNodes[i].UpdatePeers(peers)
		log.Printf("Node %d peers updated: %v", i, getPeerIDs(peers))
	}

	// Create KV stores
	stores := make([]*kvstore.KVStore, *clusterSize)
	for i := 0; i < *clusterSize; i++ {
		raftNodes[i].SetClientChannel(clientChs[i])
		stores[i] = kvstore.NewKVStore(raftNodes[i])
		log.Printf("Created KV store for node %d", i)
	}

	// Start all nodes
	for i := 0; i < *clusterSize; i++ {
		go raftNodes[i].Run()
		log.Printf("Started node %d", i)
	}

	// Wait for leader election
	log.Println("Waiting for leader election...")
	time.Sleep(2 * time.Second)

	// Simple CLI for the specified node
	if *nodeID >= 0 && *nodeID < *clusterSize {
		store := stores[*nodeID]
		raftNode := raftNodes[*nodeID]
		fmt.Printf("Node %d ready. Enter commands:\n", *nodeID)

		scanner := bufio.NewScanner(os.Stdin)
		for {
			fmt.Print("> ")
			if !scanner.Scan() {
				break
			}

			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}

			parts := strings.Fields(line)
			if len(parts) == 0 {
				continue
			}

			op := strings.ToLower(parts[0])
			switch op {
			case "put":
				if len(parts) != 3 {
					fmt.Println("Usage: put <key> <value>")
					continue
				}
				success := store.Put(parts[1], parts[2])
				fmt.Printf("Put result: %v\n", success)
				if !success {
					leaderID := raftNode.GetLeaderID()
					if leaderID >= 0 {
						fmt.Printf("Current leader is node %d\n", leaderID)
					} else {
						fmt.Println("No leader currently elected")
					}
				}

			case "append":
				if len(parts) != 3 {
					fmt.Println("Usage: append <key> <value>")
					continue
				}
				success := store.Append(parts[1], parts[2])
				fmt.Printf("Append result: %v\n", success)
				if !success {
					leaderID := raftNode.GetLeaderID()
					if leaderID >= 0 {
						fmt.Printf("Current leader is node %d\n", leaderID)
					} else {
						fmt.Println("No leader currently elected")
					}
				}

			case "get":
				if len(parts) != 2 {
					fmt.Println("Usage: get <key>")
					continue
				}
				val, ok := store.Get(parts[1])
				if ok {
					fmt.Printf("Value: %s\n", val)
				} else {
					fmt.Println("Key not found")
				}

			case "state":
				state := raftNode.GetState()
				var stateStr string
				switch state.State {
				case raft.Follower:
					stateStr = "Follower"
				case raft.Candidate:
					stateStr = "Candidate"
				case raft.Leader:
					stateStr = "Leader"
				default:
					stateStr = "Unknown"
				}
				fmt.Printf("Node state: %s, Term: %d, LeaderID: %d\n",
					stateStr, state.Term, state.LeaderID)

			case "exit":
				return

			default:
				fmt.Println("Unknown command. Available commands:")
				fmt.Println("  put <key> <value>")
				fmt.Println("  append <key> <value>")
				fmt.Println("  get <key>")
				fmt.Println("  state - show node state")
				fmt.Println("  exit")
			}
		}

		if err := scanner.Err(); err != nil {
			log.Printf("Error reading input: %v", err)
		}
	} else {
		fmt.Println("Running in cluster mode...")
		select {} // Block forever
	}
}

func getPeerIDs(peers []*raft.RaftNode) []int {
	ids := make([]int, 0, len(peers))
	for _, p := range peers {
		if p != nil {
			ids = append(ids, p.GetID())
		}
	}
	return ids
}
