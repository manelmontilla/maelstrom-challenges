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

// SeqNode allows to create names sequences that are sequentially consistent. It
// used the sequential consist key value store provided by Maelstrom.
//   - The message next receives a json with shape
//     ```{"name": "sequence_name"}``` and increments the sequence with the given name.
//   - The message current receives a json with shape ```{"name": "sequence_name"}```
//     and returns the current value of the sequence with the given name.
type SeqNode struct {
	*maelstrom.Node
	kv *maelstrom.KV
}

// NewSeqNode creates a new node that implements a sequentially consistent
// sequence in the given maelstrom node.
func NewSeqNode(node *maelstrom.Node) *SeqNode {
	kv := maelstrom.NewSeqKV(node)
	gnode := SeqNode{
		Node: node,
		kv:   kv,
	}
	node.Handle("next", gnode.HandleNext)
	node.Handle("current", gnode.HandleCurrent)
	node.Handle("current_local", gnode.HandleCurrentLocal)
	return &gnode
}

// HandleNext handles the next messages for SeqNode.
func (k *SeqNode) HandleNext(msg maelstrom.Message) error {
	addMsg := struct {
		Name string `json:"name"`
	}{}
	err := json.Unmarshal(msg.Body, &addMsg)
	if err != nil {
		return err
	}
	if addMsg.Name == "" {
		return errors.New("name cannot be empty")
	}
	name := addMsg.Name
	value, err := k.syncWrite(name)
	if err != nil {
		return err
	}
	response := struct {
		Type  string `json:"type"`
		Value int    `json:"value"`
	}{
		Type:  "next_ok",
		Value: value,
	}
	return k.Reply(msg, response)
}

// HandleCurrent handles the current messages for the sequence.
func (k *SeqNode) HandleCurrent(msg maelstrom.Message) error {
	currentMsg := struct {
		Name string `json:"name"`
	}{}
	err := json.Unmarshal(msg.Body, &currentMsg)
	if err != nil {
		return err
	}
	if currentMsg.Name == "" {
		return errors.New("name cannot be empty")
	}
	var value counterValue
	name := currentMsg.Name
	err = k.kv.ReadInto(context.Background(), name, &value)
	var rpcErr *maelstrom.RPCError
	if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
		return fmt.Errorf("Sequence %s does not exist", name)
	}
	if err != nil {
		log.Printf("error handling current message, err: %+v", err)
		return err
	}
	// Using a sequential consistent key value store means we can return stale
	// values of the counter, so, in order to get the newest value, we also
	// read the values from the other nodes and return the one with the highest
	// value.
	other, err := k.newestValueFromNodes(k.Node)
	if err != nil {
		return err
	}
	if other.Counter > value.Counter {
		value = other
	}
	type replyMsg struct {
		Type  string `json:"type"`
		Value int    `json:"value"`
	}
	reply := replyMsg{
		Type:  "current_ok",
		Value: value.Counter,
	}
	return k.Reply(msg, reply)
}

// HandleCurrentLocal handles the read_local messages for the sequence.
func (k *SeqNode) HandleCurrentLocal(msg maelstrom.Message) error {
	currentMsg := struct {
		Name string `json:"name"`
	}{}
	if currentMsg.Name == "" {
		return errors.New("name cannot be empty")
	}
	name := currentMsg.Name
	var value counterValue
	err := k.kv.ReadInto(context.Background(), name, &value)
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
		"value": value.Counter,
	}
	return k.Reply(msg, reply)
}

// newestValueFromNodes reads the value of the key from the other nodes and
// returns the one with the highest value.
func (k *SeqNode) newestValueFromNodes(n *maelstrom.Node) (counterValue, error) {
	var newest counterValue
	for _, id := range n.NodeIDs() {
		if id == n.ID() {
			continue
		}
		value, err := k.valueFromNode(id)
		if errors.Is(err, context.DeadlineExceeded) {
			continue
		}
		if err != nil {
			return counterValue{}, err
		}
		if value.Counter >= newest.Counter {
			newest = value
		}
	}
	return newest, nil
}

func (k *SeqNode) valueFromNode(ID string) (counterValue, error) {
	maxWait := 500 * time.Millisecond
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(maxWait))
	defer cancel()
	m, err := k.Node.SyncRPC(ctx, ID, map[string]any{"type": "read_local"})
	if err != nil {
		return counterValue{}, err
	}
	value := counterValue{}
	err = json.Unmarshal(m.Body, &value)
	if err != nil {
		return counterValue{}, err
	}
	return value, nil
}

type counterValue struct {
	Counter int `json:"counter"`
}

func (k *SeqNode) syncWrite(name string) (int, error) {
	for {
		currentVal := counterValue{}
		err := k.kv.ReadInto(context.Background(), name, &currentVal)
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
		newVal := counterValue{
			Counter: currentVal.Counter + increment,
		}
		err = k.kv.CompareAndSwap(context.Background(), name, currentVal, newVal, true)
		if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.PreconditionFailed {
			continue
		}
		if err != nil {
			return 0, err
		}
		return currentVal.Counter, nil
	}
}
