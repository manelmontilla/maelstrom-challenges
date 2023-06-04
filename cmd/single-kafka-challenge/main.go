// The single-kafka-challenge implements the [single kafka challenge].
//
// [single kafka challenge]: https://fly.io/dist-sys/5a
package main

import (
	"log"

	"github.com/manelmontilla/maelstrom-challenges/kafka"
)

func main() {
	n := kafka.NewSeqNode()
	if err := n.Run(); err != nil {
		log.Fatalf("node stopped with err: %v", err)
	}
}
