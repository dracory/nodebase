package delay

// Pack-local unit tests for the delay node — duration handling and
// context cancellation.

import (
	"context"
	"testing"
	"time"

	nodestore "github.com/dracory/nodestore"
)

func TestDelayPasses(t *testing.T) {
	n := NewDelayNode(nodestore.NewNode("d1", "delay"), &DelayConfig{Duration: 1})
	in := nodestore.NewMessage().SetPayload("x")
	out, err := n.Execute(context.Background(), in)
	if err != nil || out != in {
		t.Fatalf("delay should pass the input through: %v", err)
	}
}

func TestDelayCancelled(t *testing.T) {
	n := NewDelayNode(nodestore.NewNode("d2", "delay"), &DelayConfig{Duration: 60000})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	if _, err := n.Execute(ctx, nodestore.NewMessage()); err == nil {
		t.Fatal("cancelled delay should error")
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("cancelled delay should return promptly")
	}
}
