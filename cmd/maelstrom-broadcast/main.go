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
	neighbors   []string
	messages    map[int]struct{}
	broadcaster *infra.Broadcaster
	sync.RWMutex
}

// NewBroadcast returns a new broadcast code ready to be run.
func NewBroadcast() *Broadcast {
	node := maelstrom.NewNode()
	b := &Broadcast{
		Node:        node,
		messages:    map[int]struct{}{},
		RWMutex:     sync.RWMutex{},
		broadcaster: infra.NewBroadcaster(node),
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
	messages := make([]int, len(n.messages), len(n.messages))
	log.Printf("Reading messages, messages in node: %+v, messages in response %+v",
		n.messages, messages)
	for k := range n.messages {
		messages = append(messages, k)
	}
	n.RUnlock()
	reply := map[string]any{
		"type":     "read_ok",
		"messages": messages,
	}
	return node.Reply(msg, reply)
}

// HandleBroadcast handles messages of type broadcast.
func (n *Broadcast) HandleBroadcast(msg maelstrom.Message, node *maelstrom.Node) error {
	var bMsg BroadcastMessage
	if err := json.Unmarshal(msg.Body, &bMsg); err != nil {
		return err
	}
	var exist bool
	n.Lock()
	_, exist = n.messages[bMsg.Message]
	if !exist {
		n.messages[bMsg.Message] = struct{}{}
	}
	n.Unlock()
	err := node.Reply(msg, map[string]any{"type": "broadcast_ok"})
	if err != nil {
		return err
	}
	if !exist {
		// Send messages to neighbors.
		neighbors := n.neighbors
		src := msg.Src
		for _, neighbor := range neighbors {
			// Don`t broadcast the message to the node the sent it to us.
			if src == neighbor {
				continue
			}
			nodeMsg := NodeBroadcastMessage{
				Destination: neighbor,
				Message:     bMsg.Message,
			}
			n.broadcaster.Send(nodeMsg)
		}
	}
	return nil
}

// HandleTopology handles messages of type topology.
func (n *Broadcast) HandleTopology(msg maelstrom.Message, node *maelstrom.Node) error {
	var topology TopologyMessage
	if err := json.Unmarshal(msg.Body, &topology); err != nil {
		return fmt.Errorf("error unmarshaling topology: %w", err)
	}
	n.Lock()
	nodeID := n.ID()
	n.neighbors = topology.Topology[nodeID]
	n.Unlock()
	n.Node.Reply(msg, map[string]any{"type": "topology_ok"})
	return nil
}

// Run starts the inner maelstrom sever.
func (n *Broadcast) Run() error {
	return n.Node.Run()
}

// BroadcastMessage represents the message received in a bradcast operation.
type BroadcastMessage struct {
	MsgID   int `json:"msg_id,omitempty"`
	Message int `json:"message,omitempty"`
}

// MarshalJSON marshals a [BroadcastMessage].
func (b BroadcastMessage) MarshalJSON() ([]byte, error) {
	data := map[string]any{
		"type":    "broadcast",
		"message": b.Message,
	}
	if b.MsgID != 0 {
		data["msg_id"] = b.MsgID
	}
	return json.Marshal(data)
}

// UnmarshalJSON unmarshals a [BroadcastMessage].
func (b *BroadcastMessage) UnmarshalJSON(data []byte) error {
	var body map[string]any
	err := json.Unmarshal(data, &body)
	if err != nil {
		return err
	}
	typ := body["type"]
	if typ.(string) != "broadcast" {
		err := fmt.Errorf("invalid message of type broadcast, expected message type broadcast, got type: %v", typ)
		return err
	}
	rawMessage, ok := body["message"]
	if !ok {
		err := errors.New("invalid message of type broadcast, the message field does not exist")
		return err
	}
	payload := int(rawMessage.(float64))
	msgID := 0
	if id, ok := body["msg_id"]; ok {
		fMsgID, ok := id.(float64)
		if !ok {
			err := fmt.Errorf("invalid message of type broadcast, the message field msg_id is not a string, is a: %T", id)
			return err
		}
		msgID = int(fMsgID)
	}
	broadcast := BroadcastMessage{
		Message: payload,
		MsgID:   msgID,
	}
	*b = broadcast
	return nil
}

// TopologyMessage represents a message received in a topology operation.
type TopologyMessage struct {
	Topology map[string][]string
}

// UnmarshalJSON unmarshals a [TopologyMessage].
func (t *TopologyMessage) UnmarshalJSON(data []byte) error {
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
	t.Topology = topo
	return nil
}

type NodeBroadcastMessage struct {
	Destination string
	Message     int
}

func (n NodeBroadcastMessage) ID() int {
	return n.Message
}

func (n NodeBroadcastMessage) Dest() string {
	return n.Destination
}

func (n NodeBroadcastMessage) Body() map[string]any {
	return map[string]any{
		"type":    "broadcast",
		"message": n.Message,
	}
}
