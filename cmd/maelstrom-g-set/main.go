package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/manelmontilla/maelstrom-challenges/broadcast"
)

func main() {
	n := NewGSetNode[int]()
	if err := n.Run(); err != nil {
		log.Fatalf("node stopped with err: %v", err)
	}
}

type set[T comparable] struct {
	sync.RWMutex
	values map[T]struct{}
}

// Add adds a value to the set.
func (s *set[T]) Add(v T) {
	s.Lock()
	defer s.Unlock()
	s.values[v] = struct{}{}
}

// Remove removes a value from the set.
func (s *set[T]) Remove(v T) {
	s.Lock()
	defer s.Unlock()
	delete(s.values, v)
}

// Values returns the values in the set.
func (s *set[T]) Values() []T {
	s.RLock()
	defer s.RUnlock()
	values := make([]T, 0, len(s.values))
	for k := range s.values {
		values = append(values, k)
	}
	return values
}

// Merge merges a given set of values into the set.
func (s *set[T]) Merge(values []T) {
	s.Lock()
	defer s.Unlock()
	for _, v := range values {
		s.values[v] = struct{}{}
	}
}

// GSetNode is node that implements a [maelstrom g-set].
//
// [maelstrom g-set]: https://github.com/jepsen-io/maelstrom/blob/main/doc/04-crdts/01-g-set.md
type GSetNode[T comparable] struct {
	*maelstrom.Node
	replicator *broadcast.Replicator[T]
	set        *set[T]
}

// NewGSetNode creates a new GSetNode.
func NewGSetNode[T comparable]() *GSetNode[T] {
	node := maelstrom.NewNode()
	set := set[T]{
		values: map[T]struct{}{},
	}
	replicator := broadcast.NewReplicator[T](context.Background(), &set, node, 1*time.Second)
	g := GSetNode[T]{
		Node:       node,
		set:        &set,
		replicator: replicator,
	}
	node.Handle("replicate", g.HandleReplicate)
	node.Handle("add", g.HandleAdd)
	node.Handle("read", g.HandleRead)
	return &g
}

// HandleReplicate handles the replicate message.
func (s *GSetNode[T]) HandleReplicate(msg maelstrom.Message) error {
	var repMsg broadcast.ReplicationMessage[T]
	err := json.Unmarshal(msg.Body, &repMsg)
	if err != nil {
		return err
	}
	s.set.Merge(repMsg.Values)
	return nil
}

// HandleAdd handles the add message.
func (s *GSetNode[T]) HandleAdd(msg maelstrom.Message) error {
	addMsg := struct {
		Element T `json:"element"`
	}{}
	err := json.Unmarshal(msg.Body, &addMsg)
	if err != nil {
		return err
	}
	s.set.Add(addMsg.Element)
	reply := map[string]any{
		"type": "add_ok",
	}
	return s.Node.Reply(msg, reply)
}

// HandleRead handles the read message.
func (s *GSetNode[T]) HandleRead(msg maelstrom.Message) error {
	resp := map[string]any{
		"type":  "read_ok",
		"value": s.set.Values(),
	}
	return s.Node.Reply(msg, resp)
}
