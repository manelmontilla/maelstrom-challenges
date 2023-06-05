// The single-kafka-challenge implements the [single kafka challenge].
//
// [single kafka challenge]: https://fly.io/dist-sys/5a
package main

import (
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"

	"github.com/manelmontilla/maelstrom-challenges/kafka"
)

func main() {
	node := maelstrom.NewNode()
	n := kafka.NewSeqNode(node)
	if err := n.Run(); err != nil {
		log.Fatalf("node stopped with err: %v", err)
	}
}
