package main

import (
	"log"

	"github.com/manelmontilla/maelstrom-challenges/crdts"
)

func main() {
	counter := crdts.NewPNCounter()
	n := crdts.NewCounterNode(counter)
	if err := n.Run(); err != nil {
		log.Fatalf("node stopped with err: %v", err)
	}
}
