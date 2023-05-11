package broadcast

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Message define the set of types that a a BatchBroadcaster can handle.
type Message[T any] interface {
	*T
	Body() map[string]any
	WithBody(map[string]any)
	ID() string
	Dest() string
}

// BatchBroadcaster sends messages to a specified destination node. It
// accumulates the messages during a specified amount of time before send them
// in a message of type broadcast_batch. It retries the operation if the dest
// doesn't ack the message before a specified amount of time.
type BatchBroadcaster[T any, P Message[T]] struct {
	sync.RWMutex
	Node       *maelstrom.Node
	ACKTimeout time.Duration
	BatchTime  time.Duration
	BufferLen  int
	messages   chan P
	done       chan struct{}
}

// NewBatchBroadcaster creates a new broadcaster that send messages in batches.
// The broadcaster accumulates the messages for each different destination,
// during the specified batch time, after that, it sends the batch of messages
// to each destination.
func NewBatchBroadcaster[T any, P Message[T]](ctx context.Context, node *maelstrom.Node) *BatchBroadcaster[T, P] {
	b := &BatchBroadcaster[T, P]{
		Node:       node,
		BufferLen:  50,
		ACKTimeout: 5 * time.Second,
		BatchTime:  2 * time.Second,
	}
	b.messages = make(chan P, b.BufferLen)
	b.done = make(chan struct{}, 1)
	go b.aggregateAndSend(ctx, b.BatchTime, b.done)
	return b
}

// Send sends a Broadcast message with retries. This function is async unless
// the buffer for sending the messages is full.
func (b *BatchBroadcaster[T, P]) Send(msg P, dest string) {
	// Detect if the channel is full.
	select {
	case b.messages <- msg:
		return
	default:
		log.Printf("BatchBroadcaster - Messages buffer is full")
	}
	// Try to write again to backpreassure.
	b.messages <- msg
	log.Printf("Message buffered")
}

func (b *BatchBroadcaster[T, P]) aggregateAndSend(ctx context.Context, batchTime time.Duration, done chan<- struct{}) {
	defer func() { done <- struct{}{} }()
	msgByDest := map[string]map[string]P{}
	var wg sync.WaitGroup
	defer wg.Wait()
	ticker := time.NewTicker(batchTime)
	defer ticker.Stop()
loop:
	for {
		select {
		case <-ctx.Done():
			log.Print("BatchBroadcaster - Stopping sending messages")
			break loop
		case msg := <-b.messages:
			msgs, ok := msgByDest[msg.Dest()]
			if !ok {
				msgs = map[string]P{}
			}
			msgs[msg.ID()] = msg
		case <-ticker.C:
			for dst, messages := range msgByDest {
				var batchOfMessages []P
				for _, msg := range messages {
					batchOfMessages = append(batchOfMessages, msg)
				}
				msg := newBatchBroadcastMessage(dst, batchOfMessages)
				wg.Add(1)
				b.sendWithRetries(dst, msg.Body(), &wg, b.Node)
			}
			msgByDest = map[string]map[string]P{}
		}
	}
}

func (b *BatchBroadcaster[T, P]) sendWithRetries(dest string, body map[string]any, wg *sync.WaitGroup, node *maelstrom.Node) {
	// TODO: implement a gracefully termination of the send and retry process
	// by listening the Done channel of context received as a parameter.
	go func() {
		defer func() { wg.Done() }()
		retry := 0
	Loop:
		for {
			// log.Printf("Sending broadcast message: %+v, to: %+v, retry #%d\n", msg.Body(), msg.Dest(), retry)
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(b.ACKTimeout))
			defer cancel()
			_, err := node.SyncRPC(ctx, dest, body)
			if errors.Is(err, context.DeadlineExceeded) {
				retry++
				log.Printf("Timeout broadcasting message: %+v, to: %+v, starting retry #%d\n", body, dest, retry)
				continue Loop
			}
			if err != nil {
				log.Printf("Unexpected error sending broadcast message: %v, message: %+v, to: %+v, retry #%d\n", err, body, dest, retry)
			} else {
				log.Printf("Finished broadcast operation, message: %+v, to: %+v, retry #%d\n", body, dest, retry)
			}
			break
		}
	}()
}

// BatchBroadcastMessage is used by the batch broadcaster to send a batch of
// messsages.
type BatchBroadcastMessage[T any, P Message[T]] struct {
	id   string
	dest string
	msgs []P
}

func newBatchBroadcastMessage[T any, P Message[T]](dst string, msgs []P) BatchBroadcastMessage[T, P] {
	sort.Slice(msgs, func(i, j int) bool {
		return strings.Compare(msgs[i].ID(), msgs[j].ID()) < 0
	})
	var id string
	for _, msg := range msgs {
		id = id + msg.ID()
	}
	return BatchBroadcastMessage[T, P]{
		id:   id,
		dest: dst,
		msgs: msgs,
	}

}

func (b BatchBroadcastMessage[T, P]) ID() string {
	return b.id
}

func (b BatchBroadcastMessage[T, P]) Dest() string {
	return b.dest
}

func (b BatchBroadcastMessage[T, P]) Body() map[string]any {
	var messages []map[string]any
	for _, v := range b.msgs {
		messages = append(messages, v.Body())
	}
	body := map[string]any{
		"type":     "batch_broadcast",
		"messages": messages,
	}
	return body
}

// UnmarshalJSON unmarshals a [BatchBroadcastMessage].
func (b *BatchBroadcastMessage[T, P]) UnmarshalJSON(data []byte) error {
	var body map[string]any
	err := json.Unmarshal(data, &body)
	if err != nil {
		return err
	}
	typ := body["type"]
	if typ.(string) != "batch_broadcast" {
		err := fmt.Errorf("invalid message type, expected type: batch_broadcast, got type: %v", typ)
		return err
	}

	untypedBodies, ok := body["messages"]
	if !ok {
		err := errors.New("invalid message of type batch_broadcast, the messages field does not exist")
		return err
	}
	bodies, ok := untypedBodies.([]map[string]any)
	if !ok {
		err := fmt.Errorf("invalid message of type batch_broadcast, unexpected messages type: %T", untypedBodies)
		return err
	}
	var messages []P
	for _, body := range bodies {
		var msg P
		msg.WithBody(body)
		messages = append(messages, msg)
	}
	broadcast := BatchBroadcastMessage[T, P]{
		msgs: messages,
	}
	*b = broadcast
	return nil
}
