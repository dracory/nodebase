package debug

// Pack-local unit tests for the debug node — the PostRunEvents
// formatting paths (complete modes, msg.path lookup, statusVal,
// active=false) the spec suite does not exercise.

import (
	"context"
	"strings"
	"testing"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

func newNode(data map[string]interface{}) *DebugNode {
	return NewDebugNode(nodestore.NewNode("d1", "debug").SetData(data), &DebugConfig{})
}

func TestDebugPassThrough(t *testing.T) {
	n := newNode(nil)
	in := nodestore.NewMessage().SetPayload("x")
	out, err := n.Execute(context.Background(), in)
	if err != nil || out != in {
		t.Fatal("debug should pass through")
	}
}

func TestDebugCompleteModes(t *testing.T) {
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{"a": 1})

	// default: payload only
	n := newNode(nil)
	ev := n.PostRunEvents(n.NodeInterface, msg)
	if len(ev) < 1 || !strings.Contains(ev[0].Message, `"a":1`) {
		t.Fatalf("default event wrong: %v", ev)
	}

	// complete=true: whole message incl metadata
	n2 := newNode(map[string]interface{}{"complete": "true"})
	ev2 := n2.PostRunEvents(n2.NodeInterface, msg)
	if !strings.Contains(ev2[0].Message, "payload") {
		t.Fatalf("complete=true should render whole msg: %v", ev2)
	}

	// complete=false: Node-RED legacy spelling for "payload"
	n3 := newNode(map[string]interface{}{"complete": "false"})
	ev3 := n3.PostRunEvents(n3.NodeInterface, msg)
	if !strings.Contains(ev3[0].Message, `"a":1`) {
		t.Fatalf("complete=false should render payload: %v", ev3)
	}

	// msg path into payload
	n4 := newNode(map[string]interface{}{"complete": "payload.a"})
	ev4 := n4.PostRunEvents(n4.NodeInterface, msg)
	if ev4[0].Message != "1" {
		t.Fatalf("payload path wrong: %v", ev4)
	}
}

func TestDebugMetadataPath(t *testing.T) {
	msg := nodestore.NewMessage().SetMetadata(map[string]interface{}{"topic": "t1"})
	n := newNode(map[string]interface{}{"complete": "topic"})
	ev := n.PostRunEvents(n.NodeInterface, msg)
	if ev[0].Message != "t1" { // strings render bare, Node-RED style
		t.Fatalf("metadata path wrong: %v", ev)
	}
}

func TestDebugMuted(t *testing.T) {
	n := newNode(map[string]interface{}{"active": false})
	if ev := n.PostRunEvents(n.NodeInterface, nodestore.NewMessage()); ev != nil {
		t.Fatal("muted node should emit no events")
	}
}

func TestDebugStatusVal(t *testing.T) {
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{"v": 7})
	n := newNode(map[string]interface{}{"statusVal": "v"})
	ev := n.PostRunEvents(n.NodeInterface, msg)
	if len(ev) != 2 || !strings.Contains(ev[1].Message, "status v: 7") {
		t.Fatalf("statusVal event wrong: %v", ev)
	}
	// metadata path
	msg2 := nodestore.NewMessage().SetMetadata(map[string]interface{}{"code": 200})
	n2 := newNode(map[string]interface{}{"statusVal": "code"})
	ev2 := n2.PostRunEvents(n2.NodeInterface, msg2)
	if len(ev2) != 2 {
		t.Fatal("metadata statusVal missing")
	}
	// whole payload
	n3 := newNode(map[string]interface{}{"statusVal": "payload"})
	ev3 := n3.PostRunEvents(n3.NodeInterface, msg)
	if len(ev3) != 2 {
		t.Fatal("payload statusVal missing")
	}
}

func TestDebugConsole(t *testing.T) {
	// console=true prints to stdout — just ensure no panic and event emitted
	n := newNode(map[string]interface{}{"console": true})
	ev := n.PostRunEvents(n.NodeInterface, nodestore.NewMessage().SetPayload("x"))
	if len(ev) != 1 {
		t.Fatal("console mode should still emit event")
	}
}

var _ = nodebase.RunEvent{} // keep import used if api shifts
