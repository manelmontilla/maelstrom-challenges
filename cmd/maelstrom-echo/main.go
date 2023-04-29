package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type mHandler func(msg maelstrom.Message, node *maelstrom.Node) error

func (m mHandler) MaelstromHandler(node *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		return m(msg, node)
	}
}

func main() {
	n := maelstrom.NewNode()
	echoH := mHandler(handleEcho)
	n.Handle("echo", echoH.MaelstromHandler(n))
	err := n.Run()
	if err != nil {
		log.Fatal(err)
	}
}

func handleEcho(msg maelstrom.Message, node *maelstrom.Node) error {
	var body map[string]any
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return err
	}
	body["type"] = "echo_ok"
	return node.Reply(msg, body)
}
