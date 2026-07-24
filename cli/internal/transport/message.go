package transport

import (
	"errors"
	"strings"
)

type Message struct {
	srcIp     string
	srcNodeId string
	direction MessageDirection
	kind      MessageKind
	payload   string
}

type MessageDirection int

const (
	Request MessageDirection = iota
	Response
)

type MessageKind int

const (
	Ping MessageKind = iota
	Log
	Scan
	Broadcast
)

func bytesToMessage(bytes []byte) (Message, error) {
	text := string(bytes)
	parts := strings.Split(text, "|")

	if len(parts) < 3 {
		return Message{}, errors.New("At least 3 message parts required")
	}

	direction, err := parseDirection(parts[1])
	if err != nil {
		return Message{}, err
	}

	kind, err := parseKind(parts[2])
	if err != nil {
		return Message{}, err
	}

	msg := Message{
		srcNodeId: parts[0],
		direction: direction,
		kind:      kind,
	}

	if len(parts) > 3 {
		msg.payload = parts[3]
	}

	return msg, nil
}

func messageToBytes(message Message) ([]byte, error) {
	text := message.srcNodeId + "|" + directionToString(message.direction) + "|" + kindToString(message.kind)
	if message.payload != "" {
		text += "|" + message.payload
	}

	return []byte(text), nil
}

func directionToString(d MessageDirection) string {
	switch d {
	case Request:
		return "req"
	case Response:
		return "res"
	default:
		return ""
	}
}

func parseDirection(s string) (MessageDirection, error) {
	switch s {
	case "req":
		return Request, nil
	case "res":
		return Response, nil
	default:
		return 0, errors.New("Invalid message direction: " + s)
	}
}

func kindToString(k MessageKind) string {
	switch k {
	case Ping:
		return "ping"
	case Log:
		return "log"
	case Scan:
		return "scan"
	case Broadcast:
		return "broadcast"
	default:
		return ""
	}
}

func parseKind(s string) (MessageKind, error) {
	switch s {
	case "ping":
		return Ping, nil
	case "log":
		return Log, nil
	case "scan":
		return Scan, nil
	case "broadcast":
		return Broadcast, nil
	default:
		return 0, errors.New("Invalid message kind: " + s)
	}
}
