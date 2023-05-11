package infra

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Broadcaster sends messages to a specified destination node with retries. If doesn't
// receives an ACK from the destination node in the specified amount of time it
// retries the operation.
type Broadcaster struct {
	sync.RWMutex
	Node       *maelstrom.Node
	ACKTimeout time.Duration
	pennding   *pendingMessages
}

// NewBroadcaster creates a new broadcaster that allows to send messages
func NewBroadcaster(node *maelstrom.Node) *Broadcaster {
	msgSet := newPendingMessages()
	return &Broadcaster{
		Node:       node,
		ACKTimeout: 5 * time.Second,
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
			// log.Printf("Sending broadcast message: %+v, to: %+v, retry #%d\n", msg.Body(), msg.Dest(), retry)
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
			} //else {
			// 	log.Printf("Finished broadcast operation, message: %+v, to: %+v, retry #%d\n", msg.Body(), msg.Dest(), retry)
			// }
			if ok := b.pennding.RemoveMessage(msg.Dest(), msg.ID()); !ok {
				log.Printf("Unexpected error removing message: %+v, destination not present in the destinations set\n", msg)
			}
			break
		}
	}()
}

// BroadcasterMessage defines a message to be sent by the [Broadcaster].
type BroadcasterMessage interface {
	ID() string
	Body() map[string]any
	Dest() string
}

type pendingMessages struct {
	sync.Mutex
	destMessages map[string]map[string]BroadcasterMessage
}

func newPendingMessages() *pendingMessages {
	return &pendingMessages{
		Mutex:        sync.Mutex{},
		destMessages: map[string]map[string]BroadcasterMessage{},
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
		destinationSet = map[string]BroadcasterMessage{}
		p.destMessages[dest] = destinationSet
	}
	// If the message is already being sent do nothing.
	if _, ok := destinationSet[msg.ID()]; ok {
		return false
	}
	destinationSet[msg.ID()] = msg
	return true

}

// Removes the given message if exist returning if the messages has been
// removed.
func (p *pendingMessages) RemoveMessage(dest string, ID string) bool {
	p.Lock()
	defer p.Unlock()
	destinationSet, ok := p.destMessages[dest]
	if !ok {
		return false

	}
	delete(destinationSet, ID)
	return true
}
