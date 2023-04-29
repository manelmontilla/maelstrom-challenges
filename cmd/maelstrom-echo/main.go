package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/manelmontilla/maelstrom-challenges/infra"
)

func main() {
	n := maelstrom.NewNode()
	echoH := infra.NodeHandler(handleEcho)
	echoH.Register("echo", n)
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
