// The maelstrom-single-kafka implements the [single node kafka challenge].
//
// [single node kafka challenge]: https://fly.io/dist-sys/5a
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
	n := NewSingleNode(node)
	if err := n.Run(); err != nil {
		log.Fatalf("node stopped with err: %v", err)
	}
}

// SingleNode implements the [single node kafka challenge].
//
// [single node kafka challenge]: https://fly.io/dist-sys/5a
type SingleNode struct {
	*maelstrom.Node
	topics kafka.InMemTopics
}

// NewSingleNode returns a new [*kafka.SingleNode] given a [*maelstrom.Node].
func NewSingleNode(node *maelstrom.Node) *SingleNode {
	topics := kafka.NewInMemTopics()
	s := SingleNode{
		Node:   node,
		topics: topics,
	}
	s.Handle("send", s.HandleSend)
	s.Handle("poll", s.HandlePoll)
	s.Handle("commit_offsets", s.HandleCommitOffsets)
	s.Handle("list_committed_offsets", s.HandleListCommittedOffsets)
	return &s
}

// HandleSend handles the send messages.
func (s *SingleNode) HandleSend(msg maelstrom.Message) error {
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
	offset := s.topics.Send(sendMsg.Key, *sendMsg.Msg)
	resp := struct {
		Type   string `json:"type"`
		Offset int    `json:"offset"`
	}{
		Type:   "send_ok",
		Offset: offset,
	}
	return s.Reply(msg, resp)
}

// HandlePoll handles the poll messages.
func (s *SingleNode) HandlePoll(msg maelstrom.Message) error {
	pollMsg := struct {
		Offsets map[string]int `json:"offsets"`
	}{}
	if err := json.Unmarshal(msg.Body, &pollMsg); err != nil {
		return err
	}
	var msgs = make(map[string][][2]int)
	for key, offset := range pollMsg.Offsets {
		logValues := s.topics.Poll(key, offset)
		if logValues == nil {
			logValues = []int{}
		}
		var vals [][2]int
		for _, v := range logValues {
			vals = append(vals, [2]int{offset, v})
			offset = offset + 1
		}
		msgs[key] = vals
	}
	resp := map[string]any{
		"type": "poll_ok",
		"msgs": msgs,
	}
	return s.Reply(msg, resp)
}

// HandleCommitOffsets handles the commit_offsets messages.
func (s *SingleNode) HandleCommitOffsets(msg maelstrom.Message) error {
	coMsg := struct {
		Offsets map[string]int `json:"offsets"`
	}{}
	if err := json.Unmarshal(msg.Body, &coMsg); err != nil {
		return err
	}
	for k, v := range coMsg.Offsets {
		s.topics.Commit(k, v)
	}
	resp := map[string]string{"type": "commit_offsets_ok"}
	return s.Reply(msg, resp)
}

// HandleListCommittedOffsets handles the list_committed_offsets messages.
func (s *SingleNode) HandleListCommittedOffsets(msg maelstrom.Message) error {
	listMsg := struct {
		Keys []string `json:"keys"`
	}{}
	if err := json.Unmarshal(msg.Body, &listMsg); err != nil {
		return err
	}
	offsets := map[string]int{}
	for _, k := range listMsg.Keys {
		offset := s.topics.Committed(k)
		if offset < 0 {
			continue
		}
		offsets[k] = offset
	}
	resp := map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": offsets,
	}
	return s.Reply(msg, resp)
}
