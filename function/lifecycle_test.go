package function

// Tests for the Node-RED sandbox semantics added with the lifecycle
// work: nested per-port message arrays, setTimeout-driven async sends,
// and the initialize/finalize (On Start/On Stop) lifecycle.

import (
	"context"
	"fmt"
	"testing"
	"time"

	nodestore "github.com/dracory/nodestore"
)

// waitDownstream polls until the send-queue drain delivers n messages
// (it is a background goroutine like the engine's traversals).
func waitDownstream(t *testing.T, host *fakeHost, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for len(host.downstream) < n && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if len(host.downstream) < n {
		t.Fatalf("expected %d downstream sends, got %d", n, len(host.downstream))
	}
}

// Node-RED's [[m1,m2],m3] multi-send: a returned array is per-port, and
// an element that is itself an array sends each message on that port.
func TestFunctionNestedArrayMultiOutput(t *testing.T) {
	host := newFakeHost()
	n := newNode(`return [[{payload:"a1"},{payload:"a2"},{payload:"a3"}], {payload:"b1"}];`, host)
	out, err := n.Execute(context.Background(), msg(1))
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out == nil || out.GetPayload() != "a1" {
		t.Fatalf("first send on lowest port should return, got %v", out)
	}
	if len(host.emitted) != 3 {
		t.Fatalf("expected 3 emitted sends, got %d", len(host.emitted))
	}
	if host.emitted[0].GetPayload() != "a2" || host.emitted[1].GetPayload() != "a3" {
		t.Fatalf("port 1 sequence wrong: %v", host.emitted)
	}
	if host.emitted[2].GetPayload() != "b1" {
		t.Fatalf("port 2 send wrong: %v", host.emitted[2].GetPayload())
	}
}

// setTimeout callbacks run on the session VM after the script returns;
// their sends emit through the queue once the run is async.
func TestFunctionSetTimeoutSend(t *testing.T) {
	host := newFakeHost()
	host.runningWf = nodestore.NewWorkflow("wf")
	n := newNode(`setTimeout(function () { node.send(msg); }, 50);`, host)
	out, err := n.Execute(context.Background(), msg("hi"))
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out != nil {
		t.Fatalf("script returned a value, expected drop; got %v", out.GetPayload())
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(host.downstream) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if len(host.downstream) != 1 {
		t.Fatalf("expected 1 downstream send, got %d", len(host.downstream))
	}
	if host.downstream[0].p.GetPayload() != "hi" {
		t.Fatalf("async send payload wrong: %v", host.downstream[0].p.GetPayload())
	}
	if len(host.downstream[0].ports) != 1 || host.downstream[0].ports[0] != "output_1" {
		t.Fatalf("async send ports wrong: %v", host.downstream[0].ports)
	}
}

// clearTimeout cancels a pending callback — the send must never arrive.
func TestFunctionClearTimeout(t *testing.T) {
	host := newFakeHost()
	host.runningWf = nodestore.NewWorkflow("wf")
	n := newNode(`var t = setTimeout(function () { node.send(msg); }, 50); clearTimeout(t);`, host)
	if _, err := n.Execute(context.Background(), msg("hi")); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if len(host.downstream) != 0 {
		t.Fatalf("cleared timer still fired: %v", host.downstream)
	}
}

// The On Start (initialize) script runs on Activate before messages;
// On Stop (finalize) runs on Stop.
func TestFunctionLifecycleScripts(t *testing.T) {
	host := newFakeHost()
	host.runningWf = nodestore.NewWorkflow("wf")
	n := newNode(`var c = global.get('count'); c += 1; global.set('count', c); msg.payload = c; return msg;`, host)
	n.config.Initialize = `global.set('count', 0);`
	n.config.Finalize = `global.set('closed', global.get('count'));`
	if err := n.Activate(context.Background()); err != nil {
		t.Fatalf("activate: %v", err)
	}
	// Fresh executor per message (the engine rebuilds executors) —
	// same node id shares state and context.
	msgNode := newNodeFor(n.GetID(), n.config.Func, host)
	if _, err := msgNode.Execute(context.Background(), msg(1)); err != nil {
		t.Fatalf("execute: %v", err)
	}
	waitDownstream(t, host, 1)
	if fmt.Sprintf("%v", host.downstream[0].p.GetPayload()) != "1" {
		t.Fatalf("expected counter 1 from initialize, got %v", host.downstream[0].p.GetPayload())
	}
	if err := n.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	v, ok, err := host.store.GetContext("global", "", "closed")
	if err != nil || !ok {
		t.Fatalf("finalize value missing: ok=%v err=%v", ok, err)
	}
	if fmt.Sprintf("%v", v) != "1" {
		t.Fatalf("finalize saw count %v, expected 1", v)
	}
}

// An initialize script returning a promise is awaited — its setTimeout
// resolves it, and Execute waits for setup before running.
func TestFunctionAsyncSetup(t *testing.T) {
	host := newFakeHost()
	host.runningWf = nodestore.NewWorkflow("wf")
	n := newNode(`msg.payload = global.get('message'); return msg;`, host)
	n.config.Initialize = `
global.set("message", "Initializing, World!");
var promise = new Promise((resolve, reject) => {
    setTimeout(() => { global.set("message", "Hello, World!"); resolve(); }, 100);
});
return promise;`
	start := time.Now()
	if err := n.Activate(context.Background()); err != nil {
		t.Fatalf("activate: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("activate returned in %v — did not await the setup promise", elapsed)
	}
	msgNode := newNodeFor(n.GetID(), n.config.Func, host)
	if _, err := msgNode.Execute(context.Background(), msg(1)); err != nil {
		t.Fatalf("execute: %v", err)
	}
	waitDownstream(t, host, 1)
	if host.downstream[0].p.GetPayload() != "Hello, World!" {
		t.Fatalf("expected 'Hello, World!' after async setup, got %v", host.downstream[0].p.GetPayload())
	}
}

// Stop must cancel pending timer callbacks — after stop they never fire.
func TestFunctionStopCancelsTimers(t *testing.T) {
	host := newFakeHost()
	host.runningWf = nodestore.NewWorkflow("wf")
	n := newNode(`setTimeout(function () { node.send(msg); }, 50);`, host)
	if err := n.Activate(context.Background()); err != nil {
		t.Fatalf("activate: %v", err)
	}
	msgNode := newNodeFor(n.GetID(), n.config.Func, host)
	if _, err := msgNode.Execute(context.Background(), msg("hi")); err != nil {
		t.Fatalf("execute: %v", err)
	}
	// Stop on the ACTIVATION executor — the only registered stopper —
	// cancels the pending session.
	if err := n.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if len(host.downstream) != 0 {
		t.Fatalf("timer fired after stop: %v", host.downstream)
	}
}
