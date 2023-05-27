package broadcast

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// RPCSender defines the methods needed by the Replicator to send RPC messages.
type RPCSender interface {
	// RPC sends a RPC message to the specified destination node.
	RPC(dest string, body any, handler maelstrom.HandlerFunc) error
	// NodeIDs returns the list of node IDs in the cluster.
	NodeIDs() []string
}

// ReplicateSet is a set of values that can be replicated using a Replicator.
type ReplicateSet[T any] interface {
	// Values returns a snapshot of the current values in the set.
	Values() []T
}

// Replicator replicates a set of values continuously
// every period of time.
type Replicator[T any] struct {
	sender            RPCSender
	ReplicationPeriod time.Duration
	replicate         ReplicateSet[T]
	done              chan struct{}
}

// NewReplicator creates a new replicator that replicates the specified values
// each period of time.
func NewReplicator[T any](ctx context.Context, set ReplicateSet[T], sender RPCSender, period time.Duration) *Replicator[T] {
	r := &Replicator[T]{
		sender:            sender,
		ReplicationPeriod: period,
		replicate:         set,
		done:              make(chan struct{}, 1),
	}
	go r.replicateValues(ctx)
	return r
}

func (r *Replicator[T]) replicateValues(ctx context.Context) {
	ticker := time.NewTicker(r.ReplicationPeriod)
	defer ticker.Stop()
	defer func() { r.done <- struct{}{} }()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			msg := ReplicationMessage[T]{
				Values: r.replicate.Values(),
			}
			for _, dest := range r.sender.NodeIDs() {
				if err := r.sender.RPC(dest, msg, nil); err != nil {
					log.Printf("Replicator - Error sending RPC message: %v to dest: %v", err, dest)
				}
			}
		}
	}
}

// Done returns a channel that will be closed when the replicator stops.
func (r *Replicator[T]) Done() <-chan struct{} {
	return r.done
}

// ReplicationMessage is sent by the [Replicator] in a replicate operation.
type ReplicationMessage[T any] struct {
	Values []T
}

func (r ReplicationMessage[T]) body() map[string]any {
	return map[string]any{
		"type":   "replicate",
		"values": r.Values,
	}
}

func (r ReplicationMessage[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.body())
}

func (r *ReplicationMessage[T]) UnmarshalJSON(data []byte) error {
	var body struct {
		Typ    string `json:"type"`
		Values []T    `json:"values"`
	}
	if err := json.Unmarshal(data, &body); err != nil {
		return err
	}
	if body.Typ != "replicate" {
		return fmt.Errorf("invalid message type, expected replicate, got %s", body.Typ)
	}
	msg := ReplicationMessage[T]{
		Values: body.Values,
	}
	*r = msg
	return nil
}
