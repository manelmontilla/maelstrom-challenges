// Package kafka contains the implementation of the kafka challenge.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Sequences allows to create named sequences that are sequentially consistent. It
// uses the sequential consistent key value store provided by Maelstrom.
type Sequences struct {
	kv   *maelstrom.KV
	node *maelstrom.Node
}

// newSequences creates a new sequentially consistent sequence using the
// maelstrom node.
func newSequences(node *maelstrom.Node) *Sequences {
	kv := maelstrom.NewSeqKV(node)
	seq := &Sequences{
		kv:   kv,
		node: node,
	}
	node.Handle("current_local", seq.HandleCurrentLocal)
	return seq
}

// Next return the next value of the sequence with the given name.
func (s *Sequences) Next(name string) (int, error) {
	value, err := s.syncWrite(name)
	if err != nil {
		return 0, err
	}
	return value, nil
}

// Current returns the current value of a sequence.
func (s *Sequences) Current(name string) (int, error) {
	var value int
	err := s.kv.ReadInto(context.Background(), name, &value)
	var rpcErr *maelstrom.RPCError
	if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
		return 0, fmt.Errorf("Sequence %s does not exist", name)
	}
	if err != nil {
		return 0, err
	}
	// Using a sequential consistent key value store means we can return stale
	// values of the counter, so, in order to get the newest value, we also
	// read the values from the other nodes and return the one with the highest
	// value.
	other, err := s.newestValueFromNodes(s.node)
	if err != nil {
		return 0, err
	}
	if other > value {
		value = other
	}
	return value, nil
}

// HandleCurrentLocal handles the read_local messages for the sequence.
func (s *Sequences) HandleCurrentLocal(msg maelstrom.Message) error {
	currentMsg := struct {
		Name string `json:"name"`
	}{}
	if currentMsg.Name == "" {
		return errors.New("name cannot be empty")
	}
	name := currentMsg.Name
	var value int
	err := s.kv.ReadInto(context.Background(), name, &value)
	var rpcErr *maelstrom.RPCError
	if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
		err = nil
	}
	if err != nil {
		log.Printf("error handling read message, err: %+v", err)
		return err
	}
	reply := map[string]interface{}{
		"type":  "current_local_ok",
		"value": value,
	}
	return s.node.Reply(msg, reply)
}

// newestValueFromNodes reads the value of the key from the other nodes and
// returns the one with the highest value.
func (s *Sequences) newestValueFromNodes(n *maelstrom.Node) (int, error) {
	var newest int
	for _, id := range n.NodeIDs() {
		if id == n.ID() {
			continue
		}
		value, err := s.valueFromNode(id)
		if errors.Is(err, context.DeadlineExceeded) {
			continue
		}
		if err != nil {
			return 0, err
		}
		if value >= newest {
			newest = value
		}
	}
	return newest, nil
}

func (s *Sequences) valueFromNode(ID string) (int, error) {
	maxWait := 500 * time.Millisecond
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(maxWait))
	defer cancel()
	m, err := s.node.SyncRPC(ctx, ID, map[string]any{"type": "read_local"})
	if err != nil {
		return 0, err
	}
	value := 0
	err = json.Unmarshal(m.Body, &value)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func (s *Sequences) syncWrite(name string) (int, error) {
	for {
		currentVal := 0
		err := s.kv.ReadInto(context.Background(), name, &currentVal)
		var rpcErr *maelstrom.RPCError
		increment := 1
		if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
			err = nil
			rpcErr = nil
			// if the key does not exist we don't increment the counter as we
			// want the first value of the counter to be 0.
			increment = 0
		}
		if err != nil {
			return 0, err
		}
		newVal := currentVal + increment
		err = s.kv.CompareAndSwap(context.Background(), name, currentVal, newVal, true)
		if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.PreconditionFailed {
			continue
		}
		if err != nil {
			return 0, err
		}
		return newVal, nil
	}
}
