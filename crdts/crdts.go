package crdts

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"

	"github.com/manelmontilla/maelstrom-challenges/broadcast"
)

// CRCounter represents the shape of the conflict-free replicated counters that
// a [CounterNode] can manage.
type CRCounter interface {
	// Add adds a value to the counter.
	Add(ID string, v int)
	// Values returns the values of the counter as a slice.
	Values() []CounterItem
	// Merge merges the given values into the counter.
	Merge(values []CounterItem)
	// Read returns he current value of the counter.
	Read() int
}

// CounterItem stores one element of a counter so it can be marshalled using a
// simple JSON object.
type CounterItem struct {
	Key   string `json:"key"`
	Value int    `json:"value"`
}

// NewGCounter creates and initializes new GCounter.
func NewGCounter() *GCounter {
	return &GCounter{
		Counters: map[string]int{},
	}
}

// GCounter is a grow-only counter.
type GCounter struct {
	sync.RWMutex
	Counters map[string]int
}

// Add adds a value to the set.
func (g *GCounter) Add(ID string, v int) {
	g.Lock()
	defer g.Unlock()
	g.Counters[ID] = g.Counters[ID] + v
}

// Values returns the counters in the set.
func (g *GCounter) Values() []CounterItem {
	g.RLock()
	defer g.RUnlock()
	counters := make([]CounterItem, len(g.Counters))
	for k, v := range g.Counters {
		counters = append(counters, CounterItem{
			Key:   k,
			Value: v,
		})
	}
	return counters
}

// Merge merges the given values into the GCounter.
func (g *GCounter) Merge(values []CounterItem) {
	g.Lock()
	defer g.Unlock()
	for _, v := range values {
		if v.Value > g.Counters[v.Key] {
			g.Counters[v.Key] = v.Value
		}
	}
}

// Read returns the current values of the counter by summing the value of each
// node.
func (g *GCounter) Read() int {
	g.RLock()
	defer g.RUnlock()
	var sum int
	for _, v := range g.Counters {
		sum += v
	}
	return sum
}

// NewPNCounter creates and initializes a new PNCounter.
func NewPNCounter() *PNCounter {
	return &PNCounter{
		Inc: NewGCounter(),
		Dec: NewGCounter(),
	}
}

// PNCounter is CRDT that can be incremented and decremented.
type PNCounter struct {
	sync.RWMutex
	// Inc stores the positive values.
	Inc *GCounter
	// Dec stores the negative values.
	Dec *GCounter
}

// Add adds a value to the counter.
func (p *PNCounter) Add(ID string, v int) {
	p.Lock()
	defer p.Unlock()
	if v > 0 {
		p.Inc.Add(ID, v)
		return
	}
	p.Dec.Add(ID, -v)
}

// Values returns the values of the PNCounter.
func (p *PNCounter) Values() []CounterItem {
	p.RLock()
	defer p.RUnlock()
	counters := make([]CounterItem, len(p.Inc.Counters)+len(p.Dec.Counters))
	positive := p.Inc.Values()
	negative := []CounterItem{}
	for _, v := range p.Dec.Values() {
		v.Value = -v.Value
		negative = append(negative, v)
	}
	counters = append(counters, positive...)
	counters = append(counters, negative...)
	return counters
}

// Merge merges the given values into the PNCounter.
func (p *PNCounter) Merge(values []CounterItem) {
	p.Lock()
	defer p.Unlock()
	incValues := []CounterItem{}
	decValues := []CounterItem{}
	for _, v := range values {
		if v.Value > 0 {
			incValues = append(incValues, v)
			continue
		}
		v.Value = -v.Value
		decValues = append(decValues, v)
	}
	p.Inc.Merge(incValues)
	p.Dec.Merge(decValues)
}

// Read returns the current value of the counter.
func (p *PNCounter) Read() int {
	p.RLock()
	defer p.RUnlock()
	sum := p.Inc.Read() - p.Dec.Read()
	return sum
}

// CounterNode is a node that implements counter CRDT as defined in [maelstrom g-counter].
//
// [maelstrom g-counter]: https://github.com/jepsen-io/maelstrom/blob/main/doc/04-crdts/02-counters.md
type CounterNode struct {
	*maelstrom.Node
	replicator *broadcast.Replicator[CounterItem]
	Counter    CRCounter
}

// NewCounterNode creates a node that can handle counter CRDT.
func NewCounterNode(counter CRCounter) *CounterNode {
	node := maelstrom.NewNode()
	replicator := broadcast.NewReplicator[CounterItem](context.Background(), counter, node, 1*time.Second)
	cn := CounterNode{
		Node:       node,
		Counter:    counter,
		replicator: replicator,
	}
	node.Handle("replicate", cn.HandleReplicate)
	node.Handle("add", cn.HandleAdd)
	node.Handle("read", cn.HandleRead)
	return &cn
}

// HandleReplicate handles the replicate message.
func (c *CounterNode) HandleReplicate(msg maelstrom.Message) error {
	var repMsg broadcast.ReplicationMessage[CounterItem]
	err := json.Unmarshal(msg.Body, &repMsg)
	if err != nil {
		return err
	}
	c.Counter.Merge(repMsg.Values)
	return nil
}

// HandleAdd handles the add message.
func (c *CounterNode) HandleAdd(msg maelstrom.Message) error {
	addMsg := struct {
		Delta int `json:"delta"`
	}{}
	err := json.Unmarshal(msg.Body, &addMsg)
	if err != nil {
		return err
	}
	c.Counter.Add(c.Node.ID(), addMsg.Delta)
	reply := map[string]any{
		"type": "add_ok",
	}
	return c.Node.Reply(msg, reply)
}

// HandleRead handles the read message.
func (c *CounterNode) HandleRead(msg maelstrom.Message) error {
	resp := map[string]any{
		"type":  "read_ok",
		"value": c.Counter.Read(),
	}
	return c.Node.Reply(msg, resp)
}
