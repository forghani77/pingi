package main

import (
	"bytes"
	"testing"
)

func TestStreamIPs(t *testing.T) {
	input := "127.0.0.1\n8.8.8.8\n"
	jobs := make(chan string, 10)
	
	go func() {
		streamIPs(bytes.NewBufferString(input), jobs)
		close(jobs)
	}()

	var received []string
	for ip := range jobs {
		received = append(received, ip)
	}

	if len(received) != 2 {
		t.Errorf("expected 2 IPs, got %d", len(received))
	}
	if received[0] != "127.0.0.1" || received[1] != "8.8.8.8" {
		t.Errorf("unexpected IPs: %v", received)
	}
}

func TestStreamIPsCIDR(t *testing.T) {
	input := "192.168.1.0/30\n"
	jobs := make(chan string, 10)
	
	go func() {
		streamIPs(bytes.NewBufferString(input), jobs)
		close(jobs)
	}()

	var received []string
	for ip := range jobs {
		received = append(received, ip)
	}

	// 192.168.1.0/30 has 4 addresses: .0, .1, .2, .3
	if len(received) != 4 {
		t.Errorf("expected 4 IPs, got %d", len(received))
	}
	expected := []string{"192.168.1.0", "192.168.1.1", "192.168.1.2", "192.168.1.3"}
	for i, ip := range expected {
		if received[i] != ip {
			t.Errorf("expected %s, got %s", ip, received[i])
		}
	}
}
