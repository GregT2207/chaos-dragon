package main

import (
	"chaos-dragon/cli/internal/commands"
	"fmt"
	"log/slog"
	"os"
	"strings"
)

func main() {
	args := os.Args[1:]
	if len(args) == 0 || args[0] == "h" || args[0] == "-h" || args[0] == "help" || args[0] == "-help" {
		fmt.Println("Usage: chaos-dragon [-s something]")
		return
	}
	slog.Debug("Parsed valid args")

	logLevel := getLogLevel()
	options := &slog.HandlerOptions{Level: logLevel}
	handler := slog.NewTextHandler(os.Stdout, options)
	slog.SetDefault(slog.New(handler))
	slog.Debug("Configured logger", "level", logLevel)

	switch args[0] {
	case "ping":
		slog.Debug("Running ping command")
		commands.Ping()
	case "log":
		slog.Debug("Running log command")
		commands.Log()
	case "scan":
		slog.Debug("Running scan command")
		commands.Scan()
	case "broadcast":
		slog.Debug("Running broadcast command")
		commands.Broadcast()
	default:
		fmt.Printf("Unknown command: %s\n", args[0])
		os.Exit(1)
	}

	os.Exit(0)
}

func getLogLevel() slog.Level {
	logLevel := os.Getenv("LOG_LEVEL")

	switch strings.ToLower(logLevel) {
	case "trace":
		return slog.LevelDebug
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
