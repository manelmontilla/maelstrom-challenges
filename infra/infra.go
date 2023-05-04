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
	pennding   *pendingMessages
}

// NewBroadcaster creates a new broadcaster that allows to send messages
func NewBroadcaster(node *maelstrom.Node) *Broadcaster {
	msgSet := newPendingMessages()
	return &Broadcaster{
		Node:       node,
		ACKTimeout: 5 * time.Second,
		wg:         sync.WaitGroup{},
		pennding:   msgSet,
	}
}

// Send sends a Broadcast message with retries.
func (b *Broadcaster) Send(msg BroadcasterMessage) {
	if ok := b.pennding.AddIfNotExits(msg); ok {
		b.sendMessage(msg)
	}
}

func (b *Broadcaster) sendMessage(msg BroadcasterMessage) {
	go func() {
		retry := 0
	Loop:
		for {
			log.Printf("Sending broadcast message: %+v, to: %+v, retry #%d\n", msg.Body(), msg.Dest(), retry)
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(b.ACKTimeout))
			defer cancel()
			_, err := b.Node.SyncRPC(ctx, msg.Dest(), msg.Body())
			if errors.Is(err, context.DeadlineExceeded) {
				retry++
				log.Printf("Timeout broadcasting message: %+v, to: %+v, starting retry #%d\n", msg.Body(), msg.Dest(), retry)
				continue Loop
			}
			if err != nil {
				log.Printf("Unexpected error sending broadcast message: %v, message: %+v, to: %+v, retry #%d\n", err, msg.Body(), msg.Dest(), retry)
			} else {
				log.Printf("Finished broadcast operation, message: %+v, to: %+v, retry #%d\n", msg.Body(), msg.Dest(), retry)
			}
			if ok := b.pennding.RemoveMessage(msg); !ok {
				log.Printf("Unexpected error removing message: %+v, destination not present in the destinations set\n", msg)
			}
			break
		}
	}()
}

// BroadcasterMessage defines a message to be sent by the [Broadcaster].
type BroadcasterMessage interface {
	ID() int
	Body() map[string]any
	Dest() string
}

type pendingMessages struct {
	sync.Mutex
	destMessages map[string]map[int]BroadcasterMessage
}

func newPendingMessages() *pendingMessages {
	return &pendingMessages{
		Mutex:        sync.Mutex{},
		destMessages: map[string]map[int]BroadcasterMessage{},
	}
}

// AddIfNotExist adds a message pending to be sent to a destination if the
// messages is not already pending to be sent. Returns true if the message has
// been added, false otherwise.
func (p *pendingMessages) AddIfNotExits(msg BroadcasterMessage) bool {
	dest := msg.Dest()
	p.Lock()
	defer p.Unlock()
	destinationSet, ok := p.destMessages[dest]
	if !ok {
		destinationSet = map[int]BroadcasterMessage{}
		p.destMessages[dest] = destinationSet
	}
	// If the message is already being sent do nothing.
	if _, ok := destinationSet[msg.ID()]; ok {
		return false
	}
	destinationSet[msg.ID()] = msg
	return true

}

// Removes the given message if exist and returning if the messages has been
// removed.
func (p *pendingMessages) RemoveMessage(msg BroadcasterMessage) bool {
	p.Lock()
	defer p.Unlock()
	destinationSet, ok := p.destMessages[msg.Dest()]
	if !ok {
		return false

	}
	delete(destinationSet, msg.ID())
	return true
}
