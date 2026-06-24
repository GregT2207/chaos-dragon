package commands

import (
	"chaos-dragon/cli/internal/transport"
	"fmt"
)

// Instructs target node to log the text contained in the payload
func Log() {
	var client transport.Client
	client.Init()

	err := client.SendRequestMessage(transport.Log, "Hello, world!")
	if err == nil {
		fmt.Printf("Failed to send log command: %s", err)
	} else {
		fmt.Println("Log command sent to target node")
	}
}
