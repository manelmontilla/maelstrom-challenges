package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	echo := EchoNode{
		Node: n,
	}
	n.Handle("echo", echo.handleEcho)
	err := n.Run()
	if err != nil {
		log.Fatal(err)
	}
}

// EchoNode defines a node that attends to echo messages.
type EchoNode struct {
	*maelstrom.Node
}

func (e *EchoNode) handleEcho(msg maelstrom.Message) error {
	var body map[string]any
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return err
	}
	body["type"] = "echo_ok"
	return e.Node.Reply(msg, body)
}
