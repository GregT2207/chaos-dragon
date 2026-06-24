package transport

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"
)

type Client struct {
	nodeHostName           string
	awaitResponseTimeoutMs uint16
}

func (client *Client) Init() error {
	nodeHostNameEnvVar := "NODE_HOST_NAME"
	nodeHostName, ok := os.LookupEnv(nodeHostNameEnvVar)
	if !ok {
		return fmt.Errorf("Environment variable %s not found", nodeHostNameEnvVar)
	}
	client.nodeHostName = nodeHostName

	timeoutEnvVar := "AWAIT_RESPONSE_TIMEOUT_MS"
	timeoutMsStr, ok := os.LookupEnv(timeoutEnvVar)
	if !ok {
		return fmt.Errorf("Environment variable %s not found", timeoutEnvVar)
	}
	awaitResponseTimeoutMs, err := strconv.Atoi(timeoutMsStr)
	client.awaitResponseTimeoutMs = uint16(awaitResponseTimeoutMs)
	if err != nil {
		return fmt.Errorf("Failed to parse environment variable %s with value %s as integer", timeoutEnvVar, timeoutMsStr)
	}

	return nil
}

func (client *Client) SendRequestMessage(kind MessageKind, payload string) error {
	var message Message = Message{"ip", "external", Request, kind, payload}
	return client.sendMessage(message)
}

func (client *Client) GetResponseMessage(kind MessageKind) (Message, error) {
	resolvedAddr, err := net.ResolveUDPAddr("udp", "localhost:3000")
	if err != nil {
		return Message{}, err
	}

	conn, err := net.ListenUDP("udp", resolvedAddr)
	if err != nil {
		return Message{}, err
	}
	defer conn.Close()

	buffer := make([]byte, 1024)
	start := time.Now()
	for {
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			return Message{}, err
		}

		if n > 0 {
			message, err := bytesToMessage(buffer[:n])
			if err != nil {
				return Message{}, err
			}

			return message, nil
		}

		if time.Since(start) >= time.Duration(client.awaitResponseTimeoutMs)*time.Millisecond {
			return Message{}, fmt.Errorf("Timed out waiting for response (message kind %d)", client.awaitResponseTimeoutMs)
		}
	}
}

func (client *Client) sendMessage(message Message) error {
	addr, err := client.getActiveNodeAddress()
	if err != nil {
		return err
	}

	resolvedAddr, err := net.ResolveUDPAddr("udp", addr.String())
	if err != nil {
		return err
	}

	conn, err := net.DialUDP("udp", nil, resolvedAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	messageBytes, err := messageToBytes(message)
	if err != nil {
		return err
	}

	_, err = conn.Write(messageBytes)
	return err
}

// Performs a DNS scan and attempts to return the first discovered node that responds to a ping
// Up to 3 DNS scans will be performed and pinged through before returning an error
func (client *Client) getActiveNodeAddress() (net.IP, error) {
	for range 3 {
		ips, err := net.LookupIP(client.nodeHostName)
		if err != nil {
			return net.IP{}, err
		}

		for _, ip := range ips {
			err = client.SendRequestMessage(Ping, "")
			if err != nil {
				return net.IP{}, err
			}

			_, err = client.GetResponseMessage(Ping)
			if err == nil {
				return ip, nil
			}
		}
	}

	return net.IP{}, errors.New("No active nodes found")
}
