package infra

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// NodeHandler defines a maelstrom handler that apart from the message where
// it's registered received the maelstrom node where the handler is registered.
type NodeHandler func(msg maelstrom.Message, node *maelstrom.Node) error

// Register the NodeHandler for the specified type in the given node.
func (m NodeHandler) Register(typ string, node *maelstrom.Node) {
	f := func(msg maelstrom.Message) error {
		return m(msg, node)
	}
	node.Handle(typ, f)
}

// Broadcaster sends messages to a specified destination node. If doesn't
// receives an ACK from the destination node in the specified amount of time it
// retries the operation.
type Broadcaster struct {
	sync.RWMutex
	Node       *maelstrom.Node
	ACKTimeout time.Duration
	wg         sync.WaitGroup
	sets       map[string]map[int]BroadcasterMessage
}

// NewBroadcaster creates a new broadcaster that allows to send messages
func NewBroadcaster(node *maelstrom.Node) *Broadcaster {
	return &Broadcaster{
		Node:       node,
		ACKTimeout: 10 * time.Second,
		wg:         sync.WaitGroup{},
		sets:       map[string]map[int]BroadcasterMessage{},
	}
}

// Send sends a Broadcast message with retries.
func (b *Broadcaster) Send(msg BroadcasterMessage) {
	b.Lock()
	defer b.Unlock()
	destinationSet, ok := b.sets[msg.Dest()]
	if !ok {
		destinationSet = map[int]BroadcasterMessage{}
		b.sets[msg.Dest()] = destinationSet
	}
	// If the message is already being sent do nothing.
	if _, ok := destinationSet[msg.ID()]; ok {
		return
	}
	destinationSet[msg.ID()] = msg
	b.sendMessage(msg)
}

func (b *Broadcaster) sendMessage(msg BroadcasterMessage) {
	go func() {
		retries := 0
		for {
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(b.ACKTimeout))
			defer cancel()
			_, err := b.Node.SyncRPC(ctx, msg.Dest(), msg.Body())
			if errors.Is(err, context.DeadlineExceeded) {
				retries++
				log.Printf("Timeout broadcasting message %+v, to: %+v, starting retry #: %d\n", msg.Dest(), msg.Body(), retries)
				break
			}
			if err != nil {
				log.Printf("Unexpected error sending broadcast message %v, message: %+v", err, msg)
				return
			}
			if retries > 0 {
				log.Printf("Timeout broadcasting message %+v, to: %+v, recovered\n", msg.Dest(), msg.Body())
			}
			b.Lock()
			defer b.Unlock()
			destinationSet, ok := b.sets[msg.Dest()]
			if !ok {
				log.Printf("Unexpected error removing message: %+v, destination not present in the destinations set", msg)
			}
			delete(destinationSet, msg.ID())
		}
	}()
}

// BroadcasterMessage defines a message to be sent by the [Broadcaster].
type BroadcasterMessage interface {
	ID() int
	Body() map[string]any
	Dest() string
}
