package transport

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
	return Message{}, nil
}

// Unimplemented
func messageToBytes(message Message) ([]byte, error) {
	return []byte{}, nil
}
