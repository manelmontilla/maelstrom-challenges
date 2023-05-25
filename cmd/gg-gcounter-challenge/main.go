// package main implements the a solution to the grow only counter of the
// [gossip glomers challenges].
//
// [gossip glomers challenge]: https://fly.io/dist-sys/4/
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := NewKVGCounterNode()
	if err := n.Run(); err != nil {
		log.Fatalf("node stopped with err: %v", err)
	}
}

// KVGCounterNode is a grow only counter that uses the sequential consistency
// key value store provied by Maelstrom to implement a distributed grow only
// counter.
type KVGCounterNode struct {
	*maelstrom.Node
	kv *maelstrom.KV
}

// NewKVGCounterNode creates a new node that implements a grow only counter
func NewKVGCounterNode() *KVGCounterNode {
	node := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(node)
	gnode := KVGCounterNode{
		Node: node,
		kv:   kv,
	}
	node.Handle("add", gnode.HandleAdd)
	node.Handle("read", gnode.HandleRead)
	node.Handle("read_local", gnode.HandleRead)
	return &gnode
}

// HandleAdd handles the add messages for the grow only counter.
func (k *KVGCounterNode) HandleAdd(msg maelstrom.Message) error {
	addMsg := struct {
		Delta int `json:"delta"`
	}{}
	err := json.Unmarshal(msg.Body, &addMsg)
	if err != nil {
		return err
	}
	if err := k.syncWrite(addMsg.Delta); err != nil {
		return err
	}
	return k.Reply(msg, map[string]any{"type": "add_ok"})
}

// HandleRead handles the read messages for the grow only counter.
func (k *KVGCounterNode) HandleRead(msg maelstrom.Message) error {
	var value counterValue
	err := k.kv.ReadInto(context.Background(), "counter", &value)
	var rpcErr *maelstrom.RPCError
	if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
		err = nil
	}
	if err != nil {
		log.Printf("error handling read message, err: %+v", err)
		return err
	}
	// Using a sequential consistet key value store means we can return stale
	// values of the counter, in order to get the newest value we also read the
	// values from the other nodes and return the one with the highest version.
	// newer than the one we got.
	other, err := k.newestValueFromNodes(k.Node)
	if err != nil {
		return err
	}
	if other.Version > value.Version {
		value = other
	}
	reply := map[string]interface{}{
		"type":  "read_ok",
		"value": value.Counter,
	}
	return k.Reply(msg, reply)
}

// HandleReadLocal handles the read_local messages for the grow only counter.
func (k *KVGCounterNode) HandleReadLocal(msg maelstrom.Message) error {
	var value counterValue
	err := k.kv.ReadInto(context.Background(), "counter", &value)
	var rpcErr *maelstrom.RPCError
	if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
		err = nil
	}
	if err != nil {
		log.Printf("error handling read message, err: %+v", err)
		return err
	}
	reply := map[string]interface{}{
		"type":    "read_local_ok",
		"value":   value.Counter,
		"version": value.Version,
	}
	return k.Reply(msg, reply)
}

// newestValueFromNodes reads the value of the key from the other nodes and
// resturns the one with the highest version.
func (k *KVGCounterNode) newestValueFromNodes(n *maelstrom.Node) (counterValue, error) {
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
		if value.Version >= newest.Version {
			newest = value
		}
	}
	return newest, nil
}

func (k *KVGCounterNode) valueFromNode(ID string) (counterValue, error) {
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
	Counter int `json:"value"`
	Version int `json:"version"`
}

func (k *KVGCounterNode) syncWrite(delta int) error {
	for {
		currentVal := counterValue{}
		err := k.kv.ReadInto(context.Background(), "counter", &currentVal)
		var rpcErr *maelstrom.RPCError
		if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
			err = nil
			rpcErr = nil
		}
		if err != nil {
			return err
		}
		newVal := counterValue{
			Counter: currentVal.Counter + delta,
			Version: currentVal.Version + 1,
		}
		err = k.kv.CompareAndSwap(context.Background(), "counter", currentVal, newVal, true)
		if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.PreconditionFailed {
			continue
		}
		if err != nil {
			return err
		}
		currentVal = counterValue{}
		err = k.kv.ReadInto(context.Background(), "counter", &currentVal)
		if err != nil {
			return err
		}

		return nil
	}
}
