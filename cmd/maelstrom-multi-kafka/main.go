// The maelstrom-multi-kafka implements the [single node kafka challenge].
//
// [multi node kafka challenge]: https://fly.io/dist-sys/5a
package main

import (
	"encoding/json"
	"errors"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"

	"github.com/manelmontilla/maelstrom-challenges/internal/kafka"
)

func main() {
	node := maelstrom.NewNode()
	n := NewMultiNode(node)
	if err := n.Run(); err != nil {
		log.Fatalf("node stopped with err: %v", err)
	}
}

// MultiNode implements the [multi node kafka challenge].
//
// [multi node kafka challenge]: https://fly.io/dist-sys/5b
type MultiNode struct {
	*maelstrom.Node
	topics kafka.Topics
}

// NewMultiNode returns a new [*kafka.MultiNode] given a [*maelstrom.Node].
func NewMultiNode(node *maelstrom.Node) *MultiNode {
	topics := kafka.NewTopics(node)
	m := MultiNode{
		Node:   node,
		topics: topics,
	}
	m.Handle("send", m.HandleSend)
	m.Handle("poll", m.HandlePoll)
	m.Handle("commit_offsets", m.HandleCommitOffsets)
	m.Handle("list_committed_offsets", m.HandleListCommittedOffsets)
	return &m
}

// HandleSend handles the send messages.
func (m *MultiNode) HandleSend(msg maelstrom.Message) error {
	sendMsg := struct {
		Key string `json:"key"`
		Msg *int   `json:"msg,omitempty"`
	}{}

	if err := json.Unmarshal(msg.Body, &sendMsg); err != nil {
		return err
	}
	if sendMsg.Key == "" {
		return errors.New("key can't be empty")
	}
	if sendMsg.Msg == nil {
		return errors.New("msg can't be empty")
	}

	offset, err := m.topics.Send(sendMsg.Key, *sendMsg.Msg)
	if err != nil {
		return err
	}
	resp := struct {
		Type   string `json:"type"`
		Offset int    `json:"offset"`
	}{
		Type:   "send_ok",
		Offset: offset,
	}
	return m.Reply(msg, resp)
}

// HandlePoll handles the poll messages.
func (m *MultiNode) HandlePoll(msg maelstrom.Message) error {
	pollMsg := struct {
		Offsets map[string]int `json:"offsets"`
	}{}
	if err := json.Unmarshal(msg.Body, &pollMsg); err != nil {
		return err
	}
	var msgs = make(map[string][][2]int)
	for topic, offset := range pollMsg.Offsets {
		values, err := m.topics.Read(topic, offset, 3)
		if err != nil {
			return err
		}
		var res [][2]int
		for _, v := range values {
			if v >= 0 {
				res = append(res, [2]int{offset, v})
			}
			offset = offset + 1
		}
		msgs[topic] = res
	}
	resp := map[string]any{
		"type": "poll_ok",
		"msgs": msgs,
	}
	return m.Reply(msg, resp)
}

// HandleCommitOffsets handles the commit_offsets messages.
func (m *MultiNode) HandleCommitOffsets(msg maelstrom.Message) error {
	coMsg := struct {
		Offsets map[string]int `json:"offsets"`
	}{}
	if err := json.Unmarshal(msg.Body, &coMsg); err != nil {
		return err
	}
	for topic, offset := range coMsg.Offsets {
		err := m.topics.Commit(topic, offset)
		if err != nil {
			return err
		}
	}
	resp := map[string]string{"type": "commit_offsets_ok"}
	return m.Reply(msg, resp)
}

// HandleListCommittedOffsets handles the list_committed_offsets messages.
func (m *MultiNode) HandleListCommittedOffsets(msg maelstrom.Message) error {
	listMsg := struct {
		Keys []string `json:"keys"`
	}{}
	if err := json.Unmarshal(msg.Body, &listMsg); err != nil {
		return err
	}
	offsets := map[string]int{}
	for _, topic := range listMsg.Keys {
		offset, err := m.topics.Committed(topic)
		if err != nil {
			return err
		}
		if offset == -1 {
			continue
		}
		offsets[topic] = offset
	}
	resp := map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": offsets,
	}
	return m.Reply(msg, resp)
}
