package commands

import (
	"chaos-dragon/cli/internal/transport"
	"fmt"
)

// Instructs the target node to broadcast itself to its sibling nodes
func Broadcast() {
	var client transport.Client
	client.Init()

	err := client.SendRequestMessage(transport.Broadcast, "")
	if err == nil {
		fmt.Printf("Failed to send broadcast command: %s", err)
	} else {
		fmt.Println("Broadcast command sent to target node")
	}
}
