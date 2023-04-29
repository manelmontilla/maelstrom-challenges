package infra

import (
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// NodeHandler defines a maelstrom handler that apart from the message where
// it's registered received the maelstrom node where the handler is registered.
type NodeHandler func(msg maelstrom.Message, node *maelstrom.Node) error

// Register the NodeHandler for the specified type in the given node.
func (m NodeHandler) Register(typ string, node *maelstrom.Node) {
	f := func(msg maelstrom.Message) error {
		return m(msg, node)
	}
	node.Handle(typ, f)
}
