package inject

// Internal-method coverage ported from flowrunner's node_inject_test.go
// (buildPayload / intervalUnit are unexported, so these live inside the
// node package rather than in noderunner_test).

import (
	"context"
	"testing"
	"time"

	nodestore "github.com/dracory/nodestore"
)

// TestInjectPayloadTypes — NR-21: num/bool/env payload types.
func TestInjectPayloadTypes(t *testing.T) {

	build := func(cfg *InjectConfig) nodestore.MessageInterface {
		n := NewInjectNode(nodestore.NewNode("n", "inject").SetWorkflowID("wf"), cfg, nil)
		// Execute materializes payloadType from the config
		if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err != nil {
			t.Fatalf("execute failed: %v", err)
		}
		return n.buildPayload()
	}

	if d := build(&InjectConfig{Payload: "3.5", PayloadType: "num"}).GetPayload(); d != float64(3.5) {
		t.Fatalf("num payload: got %v", d)
	}
	if d := build(&InjectConfig{Payload: "true", PayloadType: "bool"}).GetPayload(); d != true {
		t.Fatalf("bool payload: got %v", d)
	}
	t.Setenv("NODEBASE_TEST_ENV", "seekrit")
	if d := build(&InjectConfig{Payload: "NODEBASE_TEST_ENV", PayloadType: "env"}).GetPayload(); d != "seekrit" {
		t.Fatalf("env payload: got %v", d)
	}
}

// TestInjectTopicAndUnits — NR-22: topic lands in Metadata, units scale.
func TestInjectTopicAndUnits(t *testing.T) {
	n := NewInjectNode(nodestore.NewNode("n", "inject").SetWorkflowID("wf"), &InjectConfig{Topic: "sensors", PayloadType: "string", Payload: "x"}, nil)
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if got := n.buildPayload().GetMetadata()["topic"]; got != "sensors" {
		t.Fatalf("expected topic sensors, got %v", got)
	}
	n2 := NewInjectNode(nodestore.NewNode("n", "inject").SetWorkflowID("wf"), &InjectConfig{IntervalUnits: "hr"}, nil)
	if n2.intervalUnit() != time.Hour {
		t.Fatalf("expected hour unit, got %v", n2.intervalUnit())
	}
	n3 := NewInjectNode(nodestore.NewNode("n", "inject").SetWorkflowID("wf"), &InjectConfig{}, nil)
	if n3.intervalUnit() != time.Second {
		t.Fatalf("expected second default, got %v", n3.intervalUnit())
	}
}
