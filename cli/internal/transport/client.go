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

func (client *Client) SendMessageRequest(kind MessageKind, payload string) error {
	var message Message = Message{"ip", "external", Request, kind, payload}
	return client.sendMessage(message)
}

func (client *Client) GetMessageResponse(kind MessageKind) (string, error) {
	start := time.Now()
	for {
		// Unimplemented - receive UDP packets here
		if time.Since(start) >= time.Duration(client.awaitResponseTimeoutMs)*time.Millisecond {
			return "", fmt.Errorf("Timed out waiting for response (message kind %d)", client.awaitResponseTimeoutMs)
		}
	}

	return "", nil
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
			err = client.SendMessageRequest(Ping, "")
			if err != nil {
				return net.IP{}, err
			}

			_, err = client.GetMessageResponse(Ping)
			if err == nil {
				return ip, nil
			}
		}
	}

	return net.IP{}, errors.New("No active nodes found")
}
