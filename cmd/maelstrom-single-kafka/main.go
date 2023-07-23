// The maelstrom-single-kafka implements the [single node kafka challenge].
//
// [single node kafka challenge]: https://fly.io/dist-sys/5a
package main

import (
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"

	"github.com/manelmontilla/maelstrom-challenges/kafka"
)

func main() {
	node := maelstrom.NewNode()
	n := kafka.NewSingleNode(node)
	if err := n.Run(); err != nil {
		log.Fatalf("node stopped with err: %v", err)
	}
}
