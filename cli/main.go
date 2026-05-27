package main

import (
	"fmt"
	"os"
)

func main() {
	args := os.Args[1:]
	if len(args) == 0 || args[0] == "h" || args[0] == "-h" || args[0] == "help" || args[0] == "-help" {
		fmt.Println("Usage: chaos-dragon [-s something]")
		return
	}

	fmt.Println("CLI is not yet implemented!")
}
