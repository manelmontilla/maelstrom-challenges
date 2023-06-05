// Package test contains the integration tests for all the challenges.
package test

import (
	"encoding/json"
	"fmt"
	"io"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"

	"github.com/manelmontilla/maelstrom-challenges/kafka"
)

func TestKafkaSequence(t *testing.T) {
	type response struct {
		Type  string `json:"type"`
		Value *int   `json:"value,omitempty"`
	}
	type request struct {
		Type string  `json:"type"`
		Name *string `json:"name,omitempty"`
	}
	type input struct {
		Input      request
		WaitOutput *response
	}
	tests := []struct {
		Name string
		Send []input
		Want []any
	}{
		{
			Name: "SequenceGrow1by1",
			Send: []input{
				{
					Input: request{
						Type: "next",
						Name: strToPtr("test"),
					},
					WaitOutput: &response{
						Type: "next_ok",
					},
				},
				{
					Input: request{
						Type: "current",
						Name: strToPtr("test"),
					},
					WaitOutput: &response{
						Type:  "current_ok",
						Value: intToPtr(0),
					},
				},
				{
					Input: request{
						Type: "current",
						Name: strToPtr("test"),
					},
					WaitOutput: &response{
						Type:  "current_ok",
						Value: intToPtr(0),
					},
				},
				{
					Input: request{
						Type: "next",
						Name: strToPtr("test"),
					},
					WaitOutput: &response{
						Type: "next_ok",
					},
				},
				{
					Input: request{
						Type: "current",
						Name: strToPtr("test"),
					},
					WaitOutput: &response{
						Type:  "current_ok",
						Value: intToPtr(0),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			testNode := newTestNode()
			knode := kafka.NewSeqNode(testNode.Node)
			// TODO: find a way of gracefully finish a maelstrom node.
			go func() {
				err := knode.Run()
				if err != nil {
					t.Fatalf("error running kafka node: %+v", err)
				}
			}()
			clientID := uuid.NewString()
			for _, input := range tt.Send {
				testNode.WriteMsg(clientID, input.Input)
				if input.WaitOutput != nil {
					out, err := testNode.ReadMsg()
					response := response{}
					err = json.Unmarshal(out.Body, &response)
					if err != nil {
						t.Fatalf("error unmarshalling node message output: %v", err)
					}
					diff := cmp.Diff(input.WaitOutput, response)
					if diff != "" {
						t.Fatalf("want response %+v, got %+v, diff: %s", input.WaitOutput, response, diff)
					}
				}
			}
		})
	}
}

type testNode struct {
	Node         *maelstrom.Node
	inputWriter  io.WriteCloser
	outputReader io.ReadCloser
}

func newTestNode() testNode {
	nodeInput, nodeInputWriter := io.Pipe()
	nodeOutputReader, nodeOutput := io.Pipe()
	node := maelstrom.NewNode()
	node.Stdin = nodeInput
	node.Stdout = nodeOutput
	node.Init(uuid.NewString(), nil)
	testNode := testNode{
		inputWriter:  nodeInputWriter,
		outputReader: nodeOutputReader,
		Node:         node,
	}
	return testNode
}

// WriteMsg writes a message to the test node.
func (t *testNode) WriteMsg(clientID string, msg any) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("error marshalling body %+v: %+v", msg, err)
	}
	inputMsg := maelstrom.Message{
		Src:  clientID,
		Dest: t.Node.ID(),
		Body: body,
	}
	content, err := json.Marshal(inputMsg)
	if err != nil {
		return fmt.Errorf("error marshalling input message %+v: %+v", msg, err)
	}
	content = append(content, byte('\n'))
	_, err = t.inputWriter.Write(content)
	if err != nil {
		return fmt.Errorf("error writing %v", err)
	}
	return nil
}

// ReadMsg reads a message from the test node, blocks the calling goroutine
// until a message is read.
func (t *testNode) ReadMsg() (maelstrom.Message, error) {

	dec := json.NewDecoder(t.outputReader)
	msg := maelstrom.Message{}
	err := dec.Decode(&msg)
	if err != nil {
		return maelstrom.Message{}, fmt.Errorf("error decoding output: %+v", err)
	}
	return msg, nil
}

func intToPtr(i int) *int {
	return &i
}

func strToPtr(s string) *string {
	return &s
}
