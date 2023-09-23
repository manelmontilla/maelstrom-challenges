// Package kafka implements the primitives needed to implement a Kafka style
// log using either the kv stores provided by Maelstrom or in memory
// structures.
package kafka

import (
	"context"
	"errors"
	"fmt"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var alreadyUsedErr = errors.New("already used")

// Topics stores the values, the offsets and the committed offsets of topics
// using kv stores provided by Maelstrom.
type Topics struct {
	offsets   *Sequences
	values    *maelstrom.KV
	committed *maelstrom.KV
}

// NewTopics returns an initialized [Topics] struct.
func NewTopics(node *maelstrom.Node) Topics {
	lkv := maelstrom.NewLinKV(node)
	skv := maelstrom.NewSeqKV(node)
	offsets := newSequences(node)
	return Topics{
		offsets:   offsets,
		values:    lkv,
		committed: skv,
	}
}

// Send sends a message to a topic and returns the offset where the message was
// stored.
func (t Topics) Send(topic string, message int) (int, error) {
	for {
		offset, err := t.offsets.Next(topic)
		if err != nil {
			return 0, err
		}
		err = t.store(topic, offset, message)
		if errors.Is(err, alreadyUsedErr) {
			continue
		}
		if err != nil {
			return 0, err
		}
		return offset, nil
	}
}

// Read returns the next n values stored in the specified topic starting at
// the specified offset. If no values are stored at a given offset it returns
// -1 in the corresponding position.
func (t Topics) Read(topic string, offset, n int) ([]int, error) {
	// Preconditions
	var values []int
	for i := 0; i < n; i++ {
		v, err := t.readOffset(topic, offset)
		if err != nil {
			return nil, err
		}
		values = append(values, v)
		offset = offset + 1
	}
	return values, nil
}

// readOffset returns the value stored in the given offset, if the value doesn't
// exits it returns -1.
func (t Topics) readOffset(topic string, offset int) (int, error) {
	k := fmt.Sprintf("topic_v_%s_%d", topic, offset)
	var v int
	err := t.values.ReadInto(context.Background(), k, &v)
	var rpcErr *maelstrom.RPCError
	if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
		return -1, nil
	}
	if err != nil {
		return 0, err
	}
	return v, nil
}

// Committed returns the last committed offset for a topic, or -1, if no offset
// has been committed yet.
func (t Topics) Committed(topic string) (int, error) {
	k := fmt.Sprintf("topic_c_%s", topic)
	committed := 0
	err := t.committed.ReadInto(context.Background(), k, &committed)
	var rpcErr *maelstrom.RPCError
	if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
		return -1, nil
	}
	if err != nil {
		return 0, err
	}
	return committed, nil
}

// Commit commits an offset for the given topic.
func (t Topics) Commit(topic string, offset int) error {
	k := fmt.Sprintf("topic_c_%s", topic)
	// Try to commit the offset while the last committed offset is less than
	// the one we want to commit or there is an error.
	for {
		lastCommitted := 0
		err := t.committed.ReadInto(context.Background(), k, &lastCommitted)
		var rpcErr *maelstrom.RPCError
		if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
			err = nil
			rpcErr = nil
			lastCommitted = -1
		}
		if err != nil {
			return err
		}
		if offset < lastCommitted {
			return nil
		}
		err = t.committed.CompareAndSwap(context.Background(), k, lastCommitted, offset, true)
		if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.PreconditionFailed {
			continue
		}
		return err
	}
}

func (t Topics) store(topic string, offset int, value int) error {
	k := fmt.Sprintf("topic_v_%s_%d", topic, offset)
	// We use -1 as previous value as we consider values stored in topics to be
	// always not negative, so -1 will force the operation to only succeed if
	// the key did not exist.
	err := t.values.CompareAndSwap(context.Background(), k, -1, value, true)
	var rpcErr *maelstrom.RPCError
	if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.PreconditionFailed {
		return alreadyUsedErr
	}
	return err
}
