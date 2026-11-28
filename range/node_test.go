package rangenode

// Pack-local unit tests for the range node — scale/drop/clamp/roll
// actions, rounding and the error paths.

import (
	"context"
	"testing"

	nodestore "github.com/dracory/nodestore"
)

func newNode(cfg *RangeConfig) *RangeNode {
	return NewRangeNode(nodestore.NewNode("r1", "range"), cfg)
}

func scale(t *testing.T, cfg *RangeConfig, payload interface{}) (nodestore.MessageInterface, error) {
	t.Helper()
	return newNode(cfg).Execute(context.Background(), nodestore.NewMessage().SetPayload(payload))
}

func TestRangeScale(t *testing.T) {
	cfg := &RangeConfig{MinIn: 0, MaxIn: 100, MinOut: 0, MaxOut: 1}
	msg, err := scale(t, cfg, 50.0)
	if err != nil || msg == nil {
		t.Fatalf("execute failed: %v", err)
	}
	if got := msg.GetPayload(); got != 0.5 {
		t.Fatalf("scaled value wrong: %v", got)
	}
}

func TestRangeDrop(t *testing.T) {
	cfg := &RangeConfig{Action: "drop", MinIn: 0, MaxIn: 10, MinOut: 0, MaxOut: 100}
	msg, err := scale(t, cfg, 15.0)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if msg != nil {
		t.Fatal("out-of-range should drop the message")
	}
	msg2, err := scale(t, cfg, 5.0)
	if err != nil || msg2 == nil {
		t.Fatal("in-range should pass")
	}
	if msg2.GetPayload() != 50.0 {
		t.Fatalf("scaled wrong: %v", msg2.GetPayload())
	}
}

func TestRangeClampAndRoll(t *testing.T) {
	cfg := &RangeConfig{Action: "clamp", MinIn: 0, MaxIn: 10, MinOut: 0, MaxOut: 10}
	msg, err := scale(t, cfg, 20.0)
	if err != nil || msg == nil {
		t.Fatalf("execute failed: %v", err)
	}
	if msg.GetPayload() != 10.0 {
		t.Fatalf("clamp high wrong: %v", msg.GetPayload())
	}
	msg2, err := scale(t, cfg, -5.0)
	if err != nil || msg2 == nil {
		t.Fatalf("execute failed: %v", err)
	}
	if msg2.GetPayload() != 0.0 {
		t.Fatalf("clamp low wrong: %v", msg2.GetPayload())
	}

	roll := &RangeConfig{Action: "roll", MinIn: 0, MaxIn: 10, MinOut: 0, MaxOut: 10}
	msg3, err := scale(t, roll, 12.0)
	if err != nil || msg3 == nil {
		t.Fatalf("execute failed: %v", err)
	}
	if msg3.GetPayload() != 2.0 {
		t.Fatalf("roll wrong: %v", msg3.GetPayload())
	}
	msg4, err := scale(t, roll, -3.0)
	if err != nil || msg4 == nil {
		t.Fatalf("execute failed: %v", err)
	}
	if msg4.GetPayload() != 7.0 {
		t.Fatalf("roll negative wrong: %v", msg4.GetPayload())
	}
}

func TestRangeRound(t *testing.T) {
	cfg := &RangeConfig{MinIn: 0, MaxIn: 3, MinOut: 0, MaxOut: 10, Round: true}
	msg, err := scale(t, cfg, 1.0)
	if err != nil || msg == nil {
		t.Fatalf("execute failed: %v", err)
	}
	if msg.GetPayload() != 3.0 {
		t.Fatalf("round wrong: %v", msg.GetPayload())
	}
}

func TestRangeZeroSpan(t *testing.T) {
	cfg := &RangeConfig{MinIn: 5, MaxIn: 5, MinOut: 7, MaxOut: 9}
	msg, err := scale(t, cfg, 5.0)
	if err != nil || msg == nil {
		t.Fatalf("zero span execute failed: %v", err)
	}
	if msg.GetPayload() != 7.0 {
		t.Fatalf("zero span should yield minout: %v", msg)
	}
}

func TestRangeNonNumeric(t *testing.T) {
	cfg := &RangeConfig{MinIn: 0, MaxIn: 10, MinOut: 0, MaxOut: 10}
	if _, err := scale(t, cfg, "abc"); err == nil {
		t.Fatal("non-numeric should error")
	}
}

func TestRangeMissingProperty(t *testing.T) {
	cfg := &RangeConfig{Property: "v", MinIn: 0, MaxIn: 10, MinOut: 0, MaxOut: 10}
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{"other": 1})
	out, err := newNode(cfg).Execute(context.Background(), msg)
	if err != nil || out != msg {
		t.Fatal("missing property should pass through")
	}
}

func TestRangeNestedProperty(t *testing.T) {
	cfg := &RangeConfig{Property: "s.temp", MinIn: 0, MaxIn: 100, MinOut: 32, MaxOut: 212}
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{
		"s": map[string]interface{}{"temp": 100.0},
	})
	out, err := newNode(cfg).Execute(context.Background(), msg)
	if err != nil || out == nil {
		t.Fatalf("execute failed: %v", err)
	}
	got := out.GetPayload().(map[string]interface{})["s"].(map[string]interface{})["temp"]
	if got != 212.0 {
		t.Fatalf("nested scale wrong: %v", got)
	}
}
