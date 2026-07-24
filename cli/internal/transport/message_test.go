package transport

import "testing"

func TestBuildsValidPingRequestMessage(t *testing.T) {
	bytes := ([]byte)("b62168f4fa9a|req|ping")

	_, err := bytesToMessage(bytes)
	if err != nil {
		t.Errorf("Expected message struct, got error: %v", err)
	}
}

func TestRejectsInvalidMessage(t *testing.T) {
	bytes := ([]byte)("123req|ping")

	_, err := bytesToMessage(bytes)
	if err == nil {
		t.Errorf("Expected error, got a message struct")
	}
}
