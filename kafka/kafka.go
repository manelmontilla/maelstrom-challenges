package kafka

import (
	"encoding/json"
	"errors"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type offsets struct {
	offsets chan map[string]chan int
}

// Commit commits an offset for the given key. If the offset to commit is less
// than the one already committed, the function does nothing.
func (o offsets) Commit(key string, offset int) {
	offsets := <-o.offsets
	keyOffset, ok := offsets[key]
	if !ok {
		keyOffset = make(chan int, 1)
		keyOffset <- -1
		offsets[key] = keyOffset
	}
	o.offsets <- offsets
	currentOffset := <-keyOffset
	defer func() {
		keyOffset <- currentOffset
	}()
	if offset <= currentOffset {
		return
	}
	currentOffset = offset
}

// Committed returns the current committed offset for a key. If no offset has
// been committedfor the key, it returns -1.
func (o offsets) Committed(key string) int {
	offsets := <-o.offsets
	keyOffset, ok := offsets[key]
	o.offsets <- offsets
	if !ok {
		return -1
	}
	offset := <-keyOffset
	keyOffset <- offset
	return offset
}

type logs struct {
	logs chan map[string]chan []int
}

// Log returns the channel of the log with the specified key.
func (l logs) Log(key string) chan []int {
	logs := <-l.logs
	valuesLog, ok := logs[key]
	if !ok {
		valuesLog = make(chan []int, 1)
		valuesLog <- nil
		logs[key] = valuesLog
	}
	l.logs <- logs
	return valuesLog
}

// Add appends the given value to the log identified with the given key
// returning the corresponding offset.
func (l logs) Add(key string, value int) int {
	logValues := l.Log(key)
	values := <-logValues
	defer func() {
		logValues <- values
	}()
	values = append(values, value)
	offset := len(values) - 1
	return offset
}

// Poll returns a slice with the messages stored in the given log, starting at
// a the given offset and finishing at the end of the log.
func (l logs) Poll(key string, offset int) []int {
	log := l.Log(key)
	values := <-log
	defer func() { log <- values }()
	if len(values)-1 < offset {
		return nil
	}
	return values[offset:]
}

// SingleNode implements the [single node kafka challenge].
//
// [single node kafka challenge]: https://fly.io/dist-sys/5a
type SingleNode struct {
	*maelstrom.Node
	logs    logs
	offsets offsets
}

// NewSingleNode returns a new [*kafka.SingleNode] given a [*maelstrom.Node].
func NewSingleNode(node *maelstrom.Node) *SingleNode {
	l := make(chan map[string]chan []int, 1)
	l <- map[string]chan []int{}
	o := make(chan map[string]chan int, 1)
	o <- map[string]chan int{}
	s := SingleNode{
		Node: node,
		logs: logs{
			logs: l,
		},
		offsets: offsets{
			offsets: o,
		},
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
	offset := s.logs.Add(sendMsg.Key, *sendMsg.Msg)
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
		logValues := s.logs.Poll(key, offset)
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
		s.offsets.Commit(k, v)
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
		offset := s.offsets.Committed(k)
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
