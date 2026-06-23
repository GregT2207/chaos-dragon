package commands

import (
	"chaos-dragon/cli/internal/transport"
	"fmt"
)

// Instructs target node to perform a new scan for sibling nodes
func Scan() {
	var client transport.Client
	client.Init()

	err := client.SendMessageRequest(transport.Scan, "")
	if err == nil {
		fmt.Printf("Failed to send scan command: %s", err)
	} else {
		fmt.Println("Scan command sent to target node")
	}
}
