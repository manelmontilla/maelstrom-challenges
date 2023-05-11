package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"

	"github.com/bits-and-blooms/bitset"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"

	"github.com/manelmontilla/maelstrom-challenges/infra"
)

func main() {
	n := NewBroadcast()
	if err := n.Run(); err != nil {
		log.Fatalf("node stopped with err: %v", err)
	}
}

type nodeIDs []nID

func nodeIDsWithIDs(IDs []string) nodeIDs {
	var neighbors []nID
	for _, id := range IDs {
		id := nID(id)
		neighbors = append(neighbors, id)
	}
	return neighbors
}

func nodeIDsWithBitSet(bs *bitset.BitSet) nodeIDs {
	var ids nodeIDs
	for i, e := bs.NextSet(0); e; i, e = bs.NextSet(i + 1) {
		var id nID
		id = id.WithIndex(uint8(i))
		ids = append(ids, id)
	}
	return ids
}

func (n nodeIDs) BitSet() (*bitset.BitSet, error) {
	bs := bitset.New(uint(len(n)))
	for _, neighbor := range n {
		index, err := nID(neighbor).Index()
		if err != nil {
			return nil, err
		}
		bs.Set(uint(index))
	}
	return bs, nil
}

type nID string

func (nID) WithIndex(i uint8) nID {
	return nID(fmt.Sprintf("n%d", i))
}

func (n nID) Index() (uint8, error) {
	// node ID examples: n1, n21
	if len(n) < 2 {
		return 0, fmt.Errorf("invalid node ID: %s", n)
	}
	strIndex := n[1:]
	i, err := strconv.Atoi(string(strIndex))
	if err != nil {
		return 0, fmt.Errorf("invalid node ID %s: %w", n, err)
	}
	return uint8(i), nil
}

// Broadcast represents a broadcast server.
type Broadcast struct {
	*maelstrom.Node
	neighbors   []nID
	topology    Topology
	messages    map[int]*bitset.BitSet
	broadcaster *infra.Broadcaster
	sync.RWMutex
}

// NewBroadcast returns a new broadcast code ready to be run.
func NewBroadcast() *Broadcast {
	node := maelstrom.NewNode()
	b := &Broadcast{
		Node:        node,
		messages:    map[int]*bitset.BitSet{},
		RWMutex:     sync.RWMutex{},
		broadcaster: infra.NewBroadcaster(node),
		topology:    DefaultTopology,
	}

	node.Handle("topology", b.HandleTopology)
	node.Handle("read", b.HandleRead)
	node.Handle("broadcast", b.HandleBroadcast)

	return b

}

// HandleRead handles messages of type read.
func (n *Broadcast) HandleRead(msg maelstrom.Message) error {
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
	return n.Reply(msg, reply)
}

// HandleBroadcast handles messages of type broadcast.
func (n *Broadcast) HandleBroadcast(msg maelstrom.Message) error {
	var bMsg BroadcastMessage
	if err := json.Unmarshal(msg.Body, &bMsg); err != nil {
		return err
	}
	var exist bool
	n.Lock()
	_, exist = n.messages[bMsg.Message]
	if !exist {
		length := len(n.NodeIDs())
		bsInit := bitset.New(uint(length))
		selfID := nID(n.ID())
		selfIndex, err := selfID.Index()
		if err != nil {
			return err
		}
		bsInit = bsInit.Set(uint(selfIndex))
		n.messages[bMsg.Message] = bsInit
	}
	msgSent := n.messages[bMsg.Message]
	msgSent = msgSent.Union(&bMsg.Sent)

	n.messages[bMsg.Message] = msgSent

	alreadySent := msgSent.Clone()
	neighbors := nodeIDs(n.neighbors)
	n.Unlock()
	err := n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	if err != nil {
		return err
	}
	if exist {
		return nil
	}
	go func() {
		alreadySentIDs := nodeIDsWithBitSet(alreadySent)
		var sendTo []string
		for _, n := range neighbors {
			var found bool
			for _, sID := range alreadySentIDs {
				if sID == n {
					found = true
					break
				}
			}
			if !found {
				sendTo = append(sendTo, string(n))
			}
		}

		sent, err := neighbors.BitSet()
		if err != nil {
			log.Printf("error in broadcast: %+v", err)
		}
		sent = sent.Union(alreadySent)
		log.Printf("Broadcast Message: %d, already sent to: %+v, neighbors: %+v, sending to: %+v", bMsg.Message, alreadySentIDs, neighbors, sendTo)
		for _, s := range sendTo {

			nodeMsg := BroadcastMessage{
				Destination: s,
				Message:     bMsg.Message,
				Sent:        *sent,
			}
			n.broadcaster.Send(nodeMsg)
		}
	}()
	return nil
}

// HandleTopology handles messages of type topology.
func (n *Broadcast) HandleTopology(msg maelstrom.Message) error {
	var topologyMsg TopologyMessage
	if err := json.Unmarshal(msg.Body, &topologyMsg); err != nil {
		return fmt.Errorf("error unmarshalling topology: %w", err)
	}
	neighbors, err := n.topology(n.Node, msg, topologyMsg.Topology)
	if err != nil {
		return err
	}
	n.Lock()
	n.neighbors = nodeIDsWithIDs(neighbors)
	n.Unlock()
	n.Node.Reply(msg, map[string]any{"type": "topology_ok"})
	return nil
}

// Topology defines a functions that returns the list of neighbors of a a node
// given the current node and the topology info received in a Topology message.
type Topology func(node *maelstrom.Node, msg maelstrom.Message, topology map[string][]string) ([]string, error)

// DefaultTopology represents the topology used by the node when no other
// topology has been specified.
func DefaultTopology(node *maelstrom.Node, msg maelstrom.Message, topology map[string][]string) ([]string, error) {
	neighbors, ok := topology[node.ID()]
	if !ok {
		err := fmt.Errorf("no information for the current node: %s, in the topology info", node.ID())
		return nil, err
	}
	return neighbors, nil
}

// Run starts the inner maelstrom sever.
func (n *Broadcast) Run() error {
	return n.Node.Run()
}

// BroadcastMessage represents the message received in a broadcast operation.
type BroadcastMessage struct {
	MsgID       int    `json:"msg_id,omitempty"`
	Destination string `json:"destination,omitempty"`
	Message     int    `json:"message,omitempty"`
	// Sent contatins the set of nodes that this message has been sent so far.
	Sent bitset.BitSet `json:"sent,omitempty"`
}

// Body returns the body of the messsage as a map.
func (b BroadcastMessage) Body() map[string]any {
	buff, _ := b.Sent.MarshalBinary()
	sent := base64.StdEncoding.EncodeToString(buff)
	body := map[string]any{
		"type":    "broadcast",
		"message": b.Message,
		"sent":    sent,
	}
	if b.MsgID != 0 {
		body["msg_id"] = b.MsgID
	}
	return body
}

// ID returns and ID that uniquely represents the payload of the message.
func (b BroadcastMessage) ID() string {
	return strconv.FormatInt(int64(b.Message), 10)
}

// Dest returns the dest node of the message.
func (b BroadcastMessage) Dest() string {
	return strconv.FormatInt(int64(b.Message), 10)
}

// MarshalJSON marshals a [BroadcastMessage].
func (b BroadcastMessage) MarshalJSON() ([]byte, error) {
	return json.Marshal(b.Body())
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

	var sentBitSet bitset.BitSet
	sent, ok := body["sent"]
	if ok {
		sentStr, ok := sent.(string)
		if !ok {
			err := fmt.Errorf("invalid message of type broadcast, the message field sent is not a string, is a: %T", sent)
			return err
		}
		sentBytes, err := base64.StdEncoding.DecodeString(sentStr)
		if err != nil {
			err := fmt.Errorf("invalid message of type broadcast, the message field sent: %s, can't be decoded: %+w, ", sentStr, err)
			return err
		}
		err = sentBitSet.UnmarshalBinary(sentBytes)
		if err != nil {
			err := fmt.Errorf("invalid message of type broadcast, the message field sent: %s, can't be decoded: %+w, ", sentBytes, err)
			return err
		}
	}

	broadcast := BroadcastMessage{
		Message: payload,
		MsgID:   msgID,
		Sent:    sentBitSet,
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
