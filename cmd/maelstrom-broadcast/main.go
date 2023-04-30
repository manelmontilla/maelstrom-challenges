package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"

	"github.com/manelmontilla/maelstrom-challenges/infra"
)

func main() {
	n := NewBroadcast()
	if err := n.Run(); err != nil {
		log.Fatalf("node stopped with err: %v", err)
	}
}

// Broadcast represents a broadcast server.
type Broadcast struct {
	*maelstrom.Node
	neighbors Topology
	messages  []any
	sync.RWMutex
}

// NewBroadcast returns a new broadcast code ready to be run.
func NewBroadcast() *Broadcast {
	b := &Broadcast{
		RWMutex: sync.RWMutex{},
		Node:    maelstrom.NewNode(),
	}
	tHandler := infra.NodeHandler(b.HandleTopology)
	tHandler.Register("topology", b.Node)

	rHandler := infra.NodeHandler(b.HandleRead)
	rHandler.Register("read", b.Node)

	bHandler := infra.NodeHandler(b.HandleBroadcast)
	bHandler.Register("broadcast", b.Node)

	return b

}

// HandleRead handles messages of type read.
func (n *Broadcast) HandleRead(msg maelstrom.Message, node *maelstrom.Node) error {
	var body map[string]any
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return err
	}
	n.RWMutex.RLock()
	messages := make([]any, 0, len(n.messages))
	copy(messages, n.messages)
	n.RUnlock()
	body["type"] = "read_ok"
	body["messages"] = messages
	return node.Reply(msg, body)
}

// HandleBroadcast handles messages of type broadcast.
func (n *Broadcast) HandleBroadcast(msg maelstrom.Message, node *maelstrom.Node) error {
	var body map[string]any
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return err
	}
	m, ok := body["message"]
	if !ok {
		return errors.New("invalid message of type broadcast, the message field does not exist")
	}
	n.RWMutex.Lock()
	n.messages = append(n.messages, m)
	n.RWMutex.Unlock()
	return node.Reply(msg, body)
}

// HandleTopology handles messages of type topology.
func (n *Broadcast) HandleTopology(msg maelstrom.Message, node *maelstrom.Node) error {
	log.Printf("Processing message: %v\n", string(msg.Body))
	var topology Topology
	if err := json.Unmarshal(msg.Body, &topology); err != nil {
		return fmt.Errorf("error unmarshaling topology: %w", err)
	}
	log.Printf("Neighbors received %+v\n", topology)
	n.Lock()
	defer n.Unlock()
	n.neighbors = topology
	return nil
}

// Run starts the inner maelstrom sever.
func (n *Broadcast) Run() error {
	return n.Node.Run()
}

// Topology stores the payload of a topology message.
type Topology map[string][]string

// UnmarshalJSON unmarshals the topology field from a topology message payload.
func (t *Topology) UnmarshalJSON(data []byte) error {
	var msg map[string]interface{}
	if err := json.Unmarshal(data, &msg); err != nil {
		return err
	}
	topology, ok := msg["topology"]
	if !ok {
		return errors.New("invalid topology message")
	}
	tmap, ok := topology.(map[string]interface{})
	if !ok {
		return errors.New("invalid topology payload")
	}
	if len(tmap) == 0 {
		return nil
	}
	topo := make(map[string][]string, len(tmap))
	for k, v := range tmap {
		values, ok := v.([]interface{})
		if !ok {
			return fmt.Errorf("invalid topology values: %+v, values type: %T", v, v)
		}
		strVals := make([]string, 0, len(values))
		for _, value := range values {
			vstr, ok := value.(string)
			if !ok {
				return fmt.Errorf("invalid topology string value: %+v, values type: %T", value, value)
			}
			strVals = append(strVals, vstr)
		}
		topo[k] = strVals
	}
	*t = topo
	return nil
}
