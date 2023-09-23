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

// NewKVGCounterNode creates a new node that implements a grow only counter.
func NewKVGCounterNode() *KVGCounterNode {
	node := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(node)
	gnode := KVGCounterNode{
		Node: node,
		kv:   kv,
	}
	node.Handle("add", gnode.HandleAdd)
	node.Handle("read", gnode.HandleRead)
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
	reply := map[string]interface{}{"type": "read_ok", "value": value.Counter}
	return k.Reply(msg, reply)
}

type counterValue struct {
	Counter  int    `json:"counter"`
	LastNode string `json:"last_node"`
}

func (k *KVGCounterNode) syncWrite(delta int) error {
	// In an ever growing counter, if the delta is 0 and we apply it, it
	// could go back.
	if delta == 0 {
		return nil
	}
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
			Counter:  currentVal.Counter + delta,
			LastNode: k.Node.ID(),
		}
		err = k.kv.CompareAndSwap(context.Background(), "counter", currentVal, newVal, true)
		if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.PreconditionFailed {
			continue
		}
		if err != nil {
			return err
		}
		// If we are her the update was successful, so the delta has been
		// successfully applied.
		return nil
	}
}
