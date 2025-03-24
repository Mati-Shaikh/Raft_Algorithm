package kvstore

import (
	"log"
	"raft-kv-store/raft"
	"sync"
	"time"
)

type KVStore struct {
	raft *raft.RaftNode
	data map[string]string
	mu   sync.Mutex
}

func NewKVStore(raftNode *raft.RaftNode) *KVStore {
	kv := &KVStore{
		raft: raftNode,
		data: make(map[string]string),
	}
	go kv.applyCommitted(raftNode.GetApplyChannel())
	return kv
}

func (kv *KVStore) applyCommitted(applyCh <-chan raft.ApplyMsg) {
	for msg := range applyCh {
		if msg.CommandValid {
			req, ok := msg.Command.(raft.ClientRequest)
			if !ok {
				continue
			}

			kv.mu.Lock()
			switch req.Op {
			case "Put":
				kv.data[req.Key] = req.Value
			case "Append":
				kv.data[req.Key] += req.Value
			}
			kv.mu.Unlock()

			if req.Resp != nil {
				select {
				case req.Resp <- raft.ClientResponse{Success: true}:
				case <-time.After(100 * time.Millisecond):
					// Don't block if client isn't listening
				}
			}
		}
	}
}

// In kvstore/kvstore.go
// In kvstore/kvstore.go

func (kv *KVStore) Put(key, value string) bool {
	respCh := make(chan raft.ClientResponse, 1)
	req := raft.ClientRequest{
		Op:    "Put",
		Key:   key,
		Value: value,
		Resp:  respCh,
	}

	// Try sending with timeout and deadlock protection
	var sent bool
	select {
	case kv.raft.GetClientChannel() <- req:
		sent = true
	case <-time.After(500 * time.Millisecond):
		log.Printf("Put timeout: could not send request to raft")
		return false
	}

	if !sent {
		return false
	}

	// Wait for response with comprehensive timeout handling
	select {
	case resp := <-respCh:
		if !resp.Success {
			// Non-blocking leader check
			leaderCh := make(chan int, 1)
			go func() {
				leaderCh <- kv.raft.GetLeaderID()
				close(leaderCh)
			}()

			select {
			case leaderID := <-leaderCh:
				if leaderID >= 0 {
					log.Printf("Put failed - current leader is node %d", leaderID)
				} else {
					log.Printf("Put failed - no leader elected")
				}
			case <-time.After(100 * time.Millisecond):
				log.Printf("Put failed - could not determine leader")
			}
		}
		return resp.Success

	case <-time.After(1 * time.Second):
		log.Printf("Put timeout: no response from raft")

		// Verify if node is still alive
		state := kv.raft.GetState()
		if state.State == raft.Leader {
			log.Printf("Node is leader but request timed out")
		} else {
			log.Printf("Node is not leader (state: %v)", state.State)
		}

		return false
	}
}
func (kv *KVStore) Append(key, value string) bool {
	respCh := make(chan raft.ClientResponse, 1) // Buffered channel
	req := raft.ClientRequest{
		Op:    "Append",
		Key:   key,
		Value: value,
		Resp:  respCh,
	}

	// Send with timeout
	select {
	case kv.raft.GetClientChannel() <- req:
		// Request sent successfully
	case <-time.After(1 * time.Second):
		return false
	}

	// Wait for response with timeout
	select {
	case resp := <-respCh:
		return resp.Success
	case <-time.After(1 * time.Second):
		return false
	}
}

func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	val, ok := kv.data[key]
	return val, ok
}

func (kv *KVStore) Snapshot() ([]byte, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	data := make([]byte, 0)
	for k, v := range kv.data {
		data = append(data, k...)
		data = append(data, '=')
		data = append(data, v...)
		data = append(data, '\n')
	}
	return data, nil
}

func (kv *KVStore) RestoreSnapshot(data []byte) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.data = make(map[string]string)
	var key, value []byte
	readingKey := true

	for _, b := range data {
		if b == '\n' {
			if len(key) > 0 {
				kv.data[string(key)] = string(value)
			}
			key = nil
			value = nil
			readingKey = true
		} else if b == '=' {
			readingKey = false
		} else if readingKey {
			key = append(key, b)
		} else {
			value = append(value, b)
		}
	}
	return nil
}
