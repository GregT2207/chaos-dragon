package main

import (
	"chaos-dragon/cli/internal/commands"
	"fmt"
	"os"
)

func main() {
	args := os.Args[1:]
	if len(args) == 0 || args[0] == "h" || args[0] == "-h" || args[0] == "help" || args[0] == "-help" {
		fmt.Println("Usage: chaos-dragon [-s something]")
		return
	}

	switch args[0] {
	case "ping":
		commands.Ping()
	case "log":
		commands.Log()
	case "scan":
		commands.Scan()
	case "broadcast":
		commands.Broadcast()
	default:
		fmt.Printf("Unknown command: %s\n", args[0])
		os.Exit(1)
	}

	os.Exit(0)
}
