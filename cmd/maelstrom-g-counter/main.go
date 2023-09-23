package main

import (
	"log"

	"github.com/manelmontilla/maelstrom-challenges/internal/crdts"
)

func main() {
	counter := crdts.NewGCounter()
	n := crdts.NewCounterNode(counter)
	if err := n.Run(); err != nil {
		log.Fatalf("node stopped with err: %v", err)
	}
}
