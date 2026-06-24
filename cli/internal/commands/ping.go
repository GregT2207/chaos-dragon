package commands

import (
	"chaos-dragon/cli/internal/transport"
	"fmt"
)

// Instructs the target node to respond with the timestamp it received the message at
func Ping() {
	var client transport.Client
	client.Init()

	err := client.SendRequestMessage(transport.Ping, "")
	if err == nil {
		fmt.Printf("Failed to send ping command: %s", err)
	} else {
		fmt.Println("Ping command sent to target node, awaiting response...")
	}
}
