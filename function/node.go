// Package function contains the function node implementation.
package function

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/dop251/goja"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(FunctionConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewFunctionNode(n, c, e), nil
}

// init registers the function node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "function",
			Name:        "Function",
			Description: "Runs arbitrary JavaScript over the message (goja sandbox)",
			Icon:        "code",
			Category:    "Flow Control",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the function node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"func": map[string]interface{}{
			"type":        "string",
			"format":      "textarea",
			"title":       "Function (JavaScript)",
			"description": "JavaScript body over msg — return msg to pass it on, node.send([m1,m2]) for multi-output, node.error(str) to raise",
			"default":     "return msg;",
		},
		"initialize": map[string]interface{}{
			"type":        "string",
			"format":      "textarea",
			"title":       "On Start (JavaScript)",
			"description": "Node-RED setup code — runs once when the node activates; a returned promise is awaited before messages flow",
		},
		"finalize": map[string]interface{}{
			"type":        "string",
			"format":      "textarea",
			"title":       "On Stop (JavaScript)",
			"description": "Node-RED close code — runs when the node stops",
		},
		"outputs": map[string]interface{}{
			"type":        "integer",
			"title":       "Outputs",
			"default":     1,
			"minimum":     1,
			"description": "Declared output ports — the editor draws one static output; extra ports need dynamic-port support (future work)",
		},
		"timeout": map[string]interface{}{
			"type":        "number",
			"title":       "Timeout (ms)",
			"description": "Maximum script runtime before the VM is interrupted (0 = no timeout, Node-RED default)",
			"default":     0,
		},
	},
	"required": []string{"func"},
}

// FunctionConfig holds configuration for the function node — the
// Node-RED function equivalent running arbitrary JavaScript via goja.
type FunctionConfig struct {
	// Func is the JavaScript source body. It is wrapped in
	// `function(msg, node, context, flow, global, env) { ... }` — return the msg to
	// pass it on, return null (or call node.send only) to control
	// outputs manually, or return an array for multi-output fan-out.
	Func string `json:"func"`
	// Outputs declares the number of output ports (default 1). The
	// editor only draws a single static output today — extra ports are
	// reachable in exported/imported flows but need dynamic Drawflow
	// port support to be visible (documented limitation).
	Outputs int `json:"outputs"`
	// Timeout is the maximum script runtime in milliseconds
	// (0 = no timeout, the Node-RED default). Expiry interrupts the VM
	// via vm.Interrupt.
	Timeout float64 `json:"timeout"`
	// Initialize is the Node-RED "On Start" setup script — run once
	// when the node activates (deploy). Returning a promise delays
	// message processing until it settles (NR async-setup semantics).
	Initialize string `json:"initialize"`
	// Finalize is the Node-RED "On Stop" close script — run when the
	// node stops (undeploy/redeploy).
	Finalize string `json:"finalize"`
}

// functionSend records one message the script pushed out through
// node.send or its return value. Port -1 means "auto" (first output).
type functionSend struct {
	port int
	msg  nodestore.MessageInterface
}

// scriptTimer is one pending setTimeout callback registered by the
// script. The callback runs on the session VM under the session mutex.
type scriptTimer struct {
	callable  goja.Callable
	cancelled atomic.Bool
}

// scriptSession owns one borrowed goja VM for the duration of a script
// run PLUS any setTimeout callbacks the script scheduled. goja VMs are
// single-threaded: every entry (the run itself and each timer
// callback) serializes on mu. A session closes when the run finished
// and all timers drained, or when the node stops.
type scriptSession struct {
	vm     *goja.Runtime
	mu     sync.Mutex
	n      *FunctionNode
	st     *functionState
	timers map[int64]*scriptTimer
	nextID int64
	runDone bool
	closed     chan struct{}
	closeOnce sync.Once
}

// isClosed reports whether the session was closed (node stopped or
// the run finished with no pending timers).
func (s *scriptSession) isClosed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

// close marks the session done: pending timer goroutines abort before
// their next VM call and the session unregisters from the state so
// Stop only pays for live sessions.
func (s *scriptSession) close() {
	s.closeOnce.Do(func() {
		close(s.closed)
		s.st.sessMu.Lock()
		delete(s.st.sessions, s)
		s.st.sessMu.Unlock()
	})
}

// addTimer registers a setTimeout callback and launches its goroutine.
// The goroutine sleeps for the delay, then runs the callback on the VM
// under the session mutex — any sends it makes go through the async
// path because the executor flipped asyncMode once the run finished.
func (s *scriptSession) addTimer(fn goja.Callable, delay time.Duration) int64 {
	s.mu.Lock()
	s.nextID++
	id := s.nextID
	if s.timers == nil {
		s.timers = map[int64]*scriptTimer{}
	}
	s.timers[id] = &scriptTimer{callable: fn}
	s.mu.Unlock()

	s.st.sessMu.Lock()
	if s.st.sessions == nil {
		s.st.sessions = map[*scriptSession]bool{}
	}
	s.st.sessions[s] = true
	s.st.sessMu.Unlock()

	go s.runTimer(id, delay)
	return id
}

// clearTimer cancels a pending timer (clearTimeout).
func (s *scriptSession) clearTimer(id int64) {
	s.mu.Lock()
	if t, ok := s.timers[id]; ok {
		t.cancelled.Store(true)
	}
	s.mu.Unlock()
}

// runTimer waits for the delay then invokes the callback on the VM.
// The callback executes under mu so it never overlaps RunProgram or a
// sibling timer; promise jobs queued by it drain on goja's leave().
func (s *scriptSession) runTimer(id int64, delay time.Duration) {
	if delay > 0 {
		t := time.NewTimer(delay)
		select {
		case <-t.C:
		case <-s.closed:
			t.Stop()
			s.finishTimer(id)
			return
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.finishTimer(id)
	t, ok := s.timers[id]
	if !ok || t.cancelled.Load() || s.isClosed() {
		return
	}
	if _, err := t.callable(goja.Undefined()); err != nil {
		s.n.emitScriptError(err)
	}
}

// finishTimer removes a timer; when the run is over and no timers
// remain the session closes itself.
func (s *scriptSession) finishTimer(id int64) {
	delete(s.timers, id)
	if len(s.timers) == 0 && s.runDone {
		s.close()
	}
}

// endRun marks the script run complete; when no timer ever registered
// the session closes immediately (the caller returns the VM to the
// pool in that case — hasTimers() stays false).
func (s *scriptSession) endRun() {
	s.mu.Lock()
	s.runDone = true
	if len(s.timers) == 0 {
		s.mu.Unlock()
		s.close()
		return
	}
	s.mu.Unlock()
}

// hasTimers reports whether the script scheduled callbacks — such VMs
// stay with the session and are never returned to the pool.
func (s *scriptSession) hasTimers() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.timers) > 0
}

// functionState is the per-node host state. goja VMs are NOT
// goroutine-safe and executors are rebuilt per message, so VMs are
// pooled here and each Execute borrows one, runs the script, then
// returns it. Node-RED context (context/flow/global) is NOT kept here —
// it lives in the host's ContextStore so it survives redeploys and,
// with a persistent backend, process restarts.
type functionState struct {
	pool  sync.Pool // *goja.Runtime
	mu    sync.Mutex
	progs map[string]*goja.Program // keyed "func" | "initialize" | "finalize"
	// sendQ lives inside the state: the host state slot is keyed by
	// workflow+node id so only ONE state object may exist per node.
	sendQ functionSendQueue
	// sessions holds live scriptSessions (runs that scheduled
	// setTimeout callbacks) so Stop can shut them down.
	sessMu   sync.Mutex
	sessions map[*scriptSession]bool
	// Setup lifecycle: setupStarted flips when Activate begins running
	// the initialize script; setupDone closes when it finishes —
	// Execute blocks on it so messages wait for async setup exactly
	// like Node-RED (and hosts that never call Activate leave
	// setupStarted false, so Execute never blocks for them).
	setupStarted atomic.Bool
	setupDone    chan struct{}
	setupOnce    sync.Once
	setupErr     error
}

// FunctionNode executes arbitrary JavaScript over the message — the
// Node-RED `function` node.
//
// SECURITY: the script is arbitrary user code. goja is a safe baseline:
// it is pure Go (cgo-free, matching the project constraint) and exposes
// NO filesystem/network/process access by default — the only reachable
// host surface is env.get (Node-RED parity: environment variables are
// intentionally readable, e.g. for credentials; do not run untrusted
// flows on hosts whose env carries secrets) and the context API —
// context/flow/global get/set backed by the host's ContextStore.
type FunctionNode struct {
	*nodebase.BaseNode
	config     *FunctionConfig
	host       nodebase.NodeHost
	workflowID string
	// sends collects (port, msg) pairs for THIS Execute call — safe on
	// the executor because executors are rebuilt per message.
	sends []functionSend
	// nodeErr is set when the script calls node.error(str).
	nodeErr string
	// firstPorts lists the output port keys that produced a first
	// message, in send order — SelectPorts restricts edges to these.
	firstPorts []string
	// inputID is the envelope ID of the message this Execute call is
	// processing — parent of every msg the script sends.
	inputID string
	// sendMu serializes n.sends collection vs dispatch: a setTimeout
	// callback calling node.send can interleave with the synchronous
	// dispatch, so both lock. asyncMode flips inside the same
	// critical section — a send after it goes straight to the queue.
	sendMu    sync.Mutex
	asyncMode atomic.Bool
	// sess is this run's script session (VM owner for timers).
	sess *scriptSession
	// st caches the state resolved in Activate — Stop must use this
	// pointer: host state is wiped before stoppers run, so
	// re-resolving there would fabricate an empty state.
	st *functionState
}

// NewFunctionNode creates a new function node
func NewFunctionNode(node nodestore.NodeInterface, config *FunctionConfig, host nodebase.NodeHost) *FunctionNode {
	return &FunctionNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
	}
}

// Execute runs the configured script against the incoming message:
// msg.payload maps to Payload, msg._msgid maps to nodestore.MessageInterface.ID (Node-RED
// compat), remaining msg keys merge into Metadata.
// A non-null return value counts as a send on output_1; node.send
// pushes (port, msg) pairs — array indices select the output port.
// Nothing sent → the message is dropped (nil, nil). Additional sends
// beyond the first per port are emitted asynchronously port-scoped, so
// each output port delivers its own payload downstream.
func (n *FunctionNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	state := n.state()
	if input != nil {
		n.inputID = input.GetID()
	}

	// Node-RED queues messages while async setup is in flight — wait
	// for a started initialize script before executing the body.
	if state.setupStarted.Load() {
		select {
		case <-state.setupDone:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if state.setupErr != nil {
			return nil, fmt.Errorf("function node setup failed: %w", state.setupErr)
		}
	}

	vm := state.acquire()
	if vm == nil {
		return nil, fmt.Errorf("function node %s: failed to create JS runtime", n.GetID())
	}
	// The VM is returned to the pool on success; a timed-out run may
	// carry a stale interrupt flag, and a run that scheduled
	// setTimeout callbacks keeps its VM inside the session — both are
	// discarded instead.
	returnToPool := true
	defer func() {
		if returnToPool {
			state.pool.Put(vm)
		}
	}()

	sess := &scriptSession{vm: vm, n: n, st: state, closed: make(chan struct{})}
	n.sess = sess

	// Fresh per-run API surface — closures capture this executor so
	// sends land on this message's collector, not a pooled one.
	if err := n.injectGlobals(vm, input); err != nil {
		return nil, err
	}

	prog, err := state.program("func", n.config.Func)
	if err != nil {
		return nil, err
	}

	// Run the script in a goroutine so a wall-clock timeout can
	// interrupt the VM (goja checks the interrupt flag in its loop).
	// The run holds the session mutex: setTimeout callbacks spawned
	// during the run serialize behind it.
	runDone := make(chan error, 1)
	go func() {
		sess.mu.Lock()
		_, err := vm.RunProgram(prog)
		sess.mu.Unlock()
		runDone <- err
	}()

	timeout := n.timeout()
	var runErr error
	if timeout > 0 {
		select {
		case runErr = <-runDone:
		case <-time.After(timeout):
			vm.Interrupt("function timeout")
			runErr = <-runDone
			if runErr == nil {
				runErr = fmt.Errorf("function timed out after %v", timeout)
			}
			// Discard the VM: a script that finished just before the
			// interrupt leaves the flag latched, and goja would abort
			// the NEXT Run on this runtime immediately.
			returnToPool = false
		}
	} else {
		runErr = <-runDone
	}
	sess.endRun()
	// A run that scheduled timers keeps its VM inside the session —
	// callbacks will call back into it until drained or stopped.
	if sess.hasTimers() {
		returnToPool = false
	}
	if runErr != nil {
		return nil, fmt.Errorf("function script error: %w", runErr)
	}
	if n.nodeErr != "" {
		return nil, fmt.Errorf("function node.error: %s", n.nodeErr)
	}

	// __result is the async wrapper's promise. Fulfilled synchronously
	// for ordinary scripts; a rejected promise is a script error; a
	// still-pending one means the script awaits a value no host API
	// can settle (e.g. a user-built promise) — nothing was sent, so the
	// message drops exactly like a script that never returns.
	if result := vm.Get("__result"); result != nil {
		if promise, ok := result.Export().(*goja.Promise); ok {
			switch promise.State() {
			case goja.PromiseStateFulfilled:
				if v := promise.Result(); v != nil && !goja.IsUndefined(v) && !goja.IsNull(v) {
					n.handleSend(v.Export())
				}
			case goja.PromiseStateRejected:
				reason := promise.Result()
				errText := reason.String()
				// The promise rejection is not a thrown goja exception,
				// so the position suffix ("at function:5:20(38)") the
				// RunProgram error path carries is not attached — pull
				// the first frame off the error's JS stack instead.
				if obj, ok := reason.(*goja.Object); ok {
					if stack := obj.Get("stack"); stack != nil && !goja.IsUndefined(stack) {
						lines := strings.SplitN(stack.String(), "\n", 3)
						if len(lines) > 1 {
							if frame := strings.TrimSpace(lines[1]); strings.HasPrefix(frame, "at ") {
								errText += " " + frame
							}
						}
					}
				}
				return nil, fmt.Errorf("function script error: %s", errText)
			}
		} else if !goja.IsUndefined(result) && !goja.IsNull(result) {
			n.handleSend(result.Export())
		}
	}

	return n.dispatch()
}

// SelectPorts restricts outgoing edges to the ports that received a
// first message — consistent with the payload Execute returns (the
// first send on the lowest-indexed port).
func (n *FunctionNode) SelectPorts(input, output nodestore.MessageInterface) []string {
	return n.firstPorts
}

// dispatch resolves the collected sends. When the host exposes the
// running flow, every send is replayed downstream in call order from
// a single goroutine — Node-RED's node.send semantics where each send
// propagates before the next — and Execute returns nil. Hosts without
// a running flow (unit tests) fall back to the legacy split: the
// first send on the lowest-indexed port returns and the rest Emit.
func (n *FunctionNode) dispatch() (nodestore.MessageInterface, error) {
	// Flip asyncMode under sendMu so a timer callback's node.send that
	// raced the end of the run lands either in n.sends (handled here)
	// or the async path — never appended after this read.
	n.sendMu.Lock()
	defer n.sendMu.Unlock()
	n.asyncMode.Store(true)
	if len(n.sends) == 0 {
		return nil, nil // drop
	}

	firstByPort := map[int]int{} // port index → sends index of first msg
	var ret nodestore.MessageInterface
	retPort := int(^uint(0) >> 1) // max int
	for i, s := range n.sends {
		p := s.port
		if p < 0 {
			p = 0 // auto → output_1
		}
		if _, seen := firstByPort[p]; seen {
			continue
		}
		firstByPort[p] = i
		n.firstPorts = append(n.firstPorts, portKey(p))
		if p < retPort {
			retPort = p
			ret = s.msg
		}
	}

	if workflow, ok := n.host.RunningFlow(n.workflowID); ok && workflow != nil {
		// Ordered multi-send: enqueue on the node's shared FIFO worker
		// so sends propagate in call order — both within one Execute
		// and across separate invocations — through the synchronous
		// downstream traversal. A downstream batch/join/sort then sees
		// them in Node-RED's node.send order.
		n.sendQueue().enqueue(sendsOf(n.sends), n, workflow)
		return nil, nil
	}

	for i, s := range n.sends {
		if i == firstByPort[retPort] {
			continue
		}
		p := s.port
		if p < 0 {
			p = 0
		}
		_ = n.host.Emit(n.workflowID, n.GetID(), s.msg, []string{portKey(p)})
	}
	return ret, nil
}

// functionSendQueue serializes a node's asynchronous emissions: sends
// are drained in enqueue order by a single worker, mirroring Node-RED
// where each node.send propagates before the next is processed.
type functionSendQueue struct {
	mu      sync.Mutex
	queue   []functionSend
	running bool
}

// sendsOf copies the collected sends for queued emission.
func sendsOf(sends []functionSend) []functionSend {
	return append([]functionSend(nil), sends...)
}

// enqueue appends sends and starts the drain worker when idle.
func (q *functionSendQueue) enqueue(sends []functionSend, n *FunctionNode, workflow nodestore.WorkflowInterface) {
	q.mu.Lock()
	q.queue = append(q.queue, sends...)
	if q.running {
		q.mu.Unlock()
		return
	}
	q.running = true
	q.mu.Unlock()
	go q.drain(n, workflow)
}

// drain walks the queue FIFO until empty; each send propagates through
// the synchronous downstream traversal before the next is dequeued.
func (q *functionSendQueue) drain(n *FunctionNode, workflow nodestore.WorkflowInterface) {
	ctx := n.host.RunContext(n.workflowID)
	for {
		q.mu.Lock()
		if len(q.queue) == 0 {
			q.running = false
			q.mu.Unlock()
			return
		}
		s := q.queue[0]
		q.queue = q.queue[1:]
		q.mu.Unlock()
		p := s.port
		if p < 0 {
			p = 0
		}
		n.host.ExecuteDownstream(ctx, n.workflowID, n.GetID(), workflow, s.msg, []string{portKey(p)})
	}
}

// sendQueue returns the node's shared send queue — it lives inside the
// single per-node state object (the host state slot holds one value).
func (n *FunctionNode) sendQueue() *functionSendQueue {
	return &n.state().sendQ
}

// handleSend interprets a send argument (node.send call arg or the
// script's return value): an array is a per-port list (index → port,
// null skips the port) where each element may itself be an array of
// messages for that port (Node-RED's [[m1,m2],m3] multi-send); anything
// else is a message for port -1 (auto). After the run's synchronous
// phase ends (asyncMode), sends emit directly through the send queue —
// that is how setTimeout callbacks deliver their messages.
func (n *FunctionNode) handleSend(v interface{}) {
	sends := collectSends(v, n.inputID)
	n.sendMu.Lock()
	async := n.asyncMode.Load()
	if !async {
		n.sends = append(n.sends, sends...)
	}
	n.sendMu.Unlock()
	if !async {
		return
	}
	workflow, ok := n.host.RunningFlow(n.workflowID)
	if !ok || workflow == nil {
		return // no running flow (activation, undeployed) — drop
	}
	n.sendQueue().enqueue(sends, n, workflow)
}

// collectSends expands a send argument into (port, msg) pairs using
// Node-RED's array rules: a top-level array is per-port, and an array
// element that is itself an array sends each element on that port.
func collectSends(v interface{}, parentID string) []functionSend {
	if arr, ok := v.([]interface{}); ok {
		var sends []functionSend
		for i, item := range arr {
			if item == nil {
				continue
			}
			if inner, ok := item.([]interface{}); ok {
				for _, m := range inner {
					if m == nil {
						continue
					}
					sends = append(sends, functionSend{port: i, msg: msgToPayload(m, parentID)})
				}
				continue
			}
			sends = append(sends, functionSend{port: i, msg: msgToPayload(item, parentID)})
		}
		return sends
	}
	return []functionSend{{port: -1, msg: msgToPayload(v, parentID)}}
}

// injectGlobals sets the script-visible globals on the VM: msg, node
// (send/error/done), the Node-RED context scopes (context = node,
// flow, global — all backed by the host ContextStore) and env.
// All closures capture this executor, so they only live for one
// Execute call.
func (n *FunctionNode) injectGlobals(vm *goja.Runtime, input nodestore.MessageInterface) error {
	if input == nil {
		// Lifecycle runs (initialize/finalize) have no incoming
		// message — Node-RED leaves msg undefined there.
		if err := vm.Set("msg", goja.Undefined()); err != nil {
			return err
		}
	} else {
		msg := map[string]interface{}{}
		for k, v := range input.GetMetadata() {
			msg[k] = v
		}
		msg["payload"] = input.GetPayload()
		// Node-RED msg._msgid is the per-instance envelope ID;
		// msgToPayload maps it back on the way out.
		msg["_msgid"] = input.GetID()
		if err := vm.Set("msg", msg); err != nil {
			return err
		}
	}

	nodeObj := map[string]interface{}{
		"send": func(call goja.FunctionCall) goja.Value {
			if len(call.Arguments) > 0 {
				n.handleSend(call.Arguments[0].Export())
			}
			return goja.Undefined()
		},
		"error": func(call goja.FunctionCall) goja.Value {
			if len(call.Arguments) > 0 {
				n.nodeErr = call.Arguments[0].String()
			}
			return goja.Undefined()
		},
		// Scripts are synchronous in goja (no event loop), so node.done
		// has nothing to signal — kept as a no-op for Node-RED script
		// compatibility.
		"done": func(call goja.FunctionCall) goja.Value {
			return goja.Undefined()
		},
		"warn": func(call goja.FunctionCall) goja.Value {
			if len(call.Arguments) > 0 {
				n.host.EmitEvent(n.workflowID, n.GetID(), n.GetID(), "warn", call.Arguments[0].String())
			}
			return goja.Undefined()
		},
		"log": func(call goja.FunctionCall) goja.Value {
			if len(call.Arguments) > 0 {
				n.host.EmitEvent(n.workflowID, n.GetID(), n.GetID(), "info", call.Arguments[0].String())
			}
			return goja.Undefined()
		},
		// node.linkcall(target, msg, options) — Node-RED's function-side
		// link call: resolves a linkIn by id or unique name, injects a
		// (cloned) message and resolves with the linkOut return. Runs
		// synchronously in goja, so `await` settles inside the script
		// run. options.timeout is milliseconds (NR default 5000);
		// options.clone defaults to true.
		"linkcall": func(call goja.FunctionCall) goja.Value {
			v, err := n.linkCall(vm, call.Arguments)
			if err != nil {
				panic(vm.NewGoError(err))
			}
			return v
		},
	}
	if err := vm.Set("node", nodeObj); err != nil {
		return err
	}

	// Node-RED timer surface: goja has no event loop, so timers are
	// host-provided — each callback runs on this session's VM after
	// its delay, serialized with the script run via sess.mu.
	if err := vm.Set("setTimeout", func(call goja.FunctionCall) goja.Value {
		if n.sess == nil || len(call.Arguments) == 0 {
			return goja.Undefined()
		}
		fn, ok := goja.AssertFunction(call.Arguments[0])
		if !ok {
			return goja.Undefined()
		}
		delay := 0.0
		if len(call.Arguments) > 1 {
			delay = call.Arguments[1].ToFloat()
		}
		if delay < 0 {
			delay = 0
		}
		id := n.sess.addTimer(fn, time.Duration(delay*float64(time.Millisecond)))
		return vm.ToValue(id)
	}); err != nil {
		return err
	}
	if err := vm.Set("clearTimeout", func(call goja.FunctionCall) goja.Value {
		if n.sess != nil && len(call.Arguments) > 0 {
			n.sess.clearTimer(call.Arguments[0].ToInteger())
		}
		return goja.Undefined()
	}); err != nil {
		return err
	}
	// console.* is the Node-RED function-script console — routed to
	// the node's event stream (log→info, warn/error→warn so a
	// console.error cannot masquerade as a script error).
	consoleObj := map[string]interface{}{}
	for _, level := range []string{"log", "debug"} {
		consoleObj[level] = n.consoleFunc(vm, "info")
	}
	for _, level := range []string{"warn", "error"} {
		consoleObj[level] = n.consoleFunc(vm, "warn")
	}
	if err := vm.Set("console", consoleObj); err != nil {
		return err
	}

	// Node-RED context scopes: `context` is node-scoped (namespaced by
	// workflow+node), `flow` is shared by all nodes of the workflow
	// only, `global` is shared host-wide. All three go through the
	// host ContextStore so values survive undeploy/redeploy and —
	// with a persistent backend wired in main — process restarts.
	if err := vm.Set("context", n.contextObject(vm, nodebase.ContextScopeNode, n.workflowID+"/"+n.GetID())); err != nil {
		return err
	}
	if err := vm.Set("flow", n.contextObject(vm, nodebase.ContextScopeFlow, n.workflowID)); err != nil {
		return err
	}
	if err := vm.Set("global", n.contextObject(vm, nodebase.ContextScopeGlobal, "")); err != nil {
		return err
	}

	// env.get resolves the Node-RED env scoping chain — group env →
	// subflow overlay → flow env → global-config → process env (see
	// host.NodeEnv for the first four; os.Getenv is the last). The
	// OS fallback fails CLOSED: only a host that explicitly
	// implements OSEnvAllowed() and returns true exposes process
	// env (engine AllowOSEnv option). Hosts without
	// the method get no os.Getenv at all — otherwise a flow author
	// could read arbitrary process secrets via env.get.
	envObj := map[string]interface{}{
		"get": func(call goja.FunctionCall) goja.Value {
			if len(call.Arguments) == 0 {
				return goja.Undefined()
			}
			name := call.Arguments[0].String()
			if v, ok := n.host.NodeEnv(n.workflowID, n.GetID(), name); ok {
				return vm.ToValue(v)
			}
			g, ok := n.host.(interface{ OSEnvAllowed() bool })
			if !ok || !g.OSEnvAllowed() {
				return goja.Undefined()
			}
			return vm.ToValue(os.Getenv(name))
		},
	}
	if err := vm.Set("env", envObj); err != nil {
		return err
	}

	return vm.Set("__result", goja.Undefined())
}

// linkCall implements node.linkcall(target, msg, options): the target
// is a linkIn node id or — when no node has that id — a unique linkIn
// name (Node-RED's static-vs-dynamic rule). The script's msg is cloned
// by default (options.clone === false opts out), injected, and the
// linkOut return is handed back as a JS message object. Errors
// (missing target, timeout) reject like Node-RED's node.linkcall.
func (n *FunctionNode) linkCall(vm *goja.Runtime, args []goja.Value) (goja.Value, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("linkcall target must be a non-empty string")
	}
	target := args[0].String()
	if target == "" || goja.IsUndefined(args[0]) || goja.IsNull(args[0]) {
		return nil, fmt.Errorf("linkcall target must be a non-empty string")
	}
	var argMsg nodestore.MessageInterface
	if len(args) > 1 && args[1] != nil && !goja.IsUndefined(args[1]) && !goja.IsNull(args[1]) {
		exported := args[1].Export()
		if _, ok := exported.(map[string]interface{}); !ok {
			return nil, fmt.Errorf("linkcall msg must be an object")
		}
		argMsg = msgToPayload(exported, n.inputID)
	}
	if argMsg == nil {
		return nil, fmt.Errorf("linkcall msg must be an object")
	}

	timeout := nodebase.DefaultLinkCallFunctionTimeout
	clone := true
	if len(args) > 2 && args[2] != nil && !goja.IsUndefined(args[2]) && !goja.IsNull(args[2]) {
		if opts, ok := args[2].Export().(map[string]interface{}); ok {
			if t, ok := opts["timeout"]; ok {
				if f, err := nodebase.ToFloat(t); err == nil && f > 0 {
					timeout = time.Duration(f * float64(time.Millisecond))
				}
			}
			if c, ok := opts["clone"]; ok {
				if b, ok := c.(bool); ok {
					clone = b
				}
			}
		}
	}

	// A target that is not a live node id is a dynamic (name) lookup —
	// Node-RED resolves the name uniquely within the caller's flow.
	targetID, err := nodebase.ResolveLinkInTarget(n.host, n.workflowID, target, true)
	if err != nil {
		return nil, err
	}
	msg := argMsg
	if clone {
		msg = nodebase.ClonePayload(argMsg)
	}
	response, err := nodebase.LinkCallInto(n.host, n.workflowID, n.GetID(), targetID, msg, timeout)
	if err != nil {
		return nil, err
	}

	out := map[string]interface{}{}
	for k, v := range response.GetMetadata() {
		out[k] = v
	}
	out["payload"] = response.GetPayload()
	out["_msgid"] = response.GetID()
	return vm.ToValue(out), nil
}

// contextObject builds the JS API object for one context scope:
// get/keys read through the host's ContextStore, set writes through
// it (undefined/null deletes the key, matching Node-RED where setting
// undefined removes the value). nodestore.Store failures are reported as error
// events rather than aborting the script — a read miss simply yields
// undefined, same as an unset key.
func (n *FunctionNode) contextObject(vm *goja.Runtime, scope, scopeID string) map[string]interface{} {
	return map[string]interface{}{
		"get": func(call goja.FunctionCall) goja.Value {
			if len(call.Arguments) == 0 {
				return goja.Undefined()
			}
			v, ok, err := n.host.ResolvedContextStore().GetContext(scope, scopeID, call.Arguments[0].String())
			if err != nil {
				n.host.EmitEvent(n.workflowID, n.GetID(), n.GetID(), "error", fmt.Sprintf("%s context get failed: %v", scope, err))
				return goja.Undefined()
			}
			if !ok {
				return goja.Undefined()
			}
			return vm.ToValue(v)
		},
		"set": func(call goja.FunctionCall) goja.Value {
			if len(call.Arguments) < 2 {
				return goja.Undefined()
			}
			key := call.Arguments[0].String()
			arg := call.Arguments[1]
			var err error
			if goja.IsUndefined(arg) || goja.IsNull(arg) {
				err = n.host.ResolvedContextStore().DeleteContext(scope, scopeID, key)
			} else {
				err = n.host.ResolvedContextStore().SetContext(scope, scopeID, key, arg.Export())
			}
			if err != nil {
				n.host.EmitEvent(n.workflowID, n.GetID(), n.GetID(), "error", fmt.Sprintf("%s context set failed: %v", scope, err))
			}
			return goja.Undefined()
		},
		"keys": func(call goja.FunctionCall) goja.Value {
			keys, err := n.host.ResolvedContextStore().ContextKeys(scope, scopeID)
			if err != nil {
				n.host.EmitEvent(n.workflowID, n.GetID(), n.GetID(), "error", fmt.Sprintf("%s context keys failed: %v", scope, err))
				return vm.ToValue([]string{})
			}
			return vm.ToValue(keys)
		},
	}
}

// timeout returns the configured script timeout; 0 (the Node-RED
// default) means no timeout.
func (n *FunctionNode) timeout() time.Duration {
	t := n.config.Timeout
	if t < 0 {
		t = 0
	}
	return time.Duration(t * float64(time.Millisecond))
}

// state returns (creating if needed) the per-node VM pool + compiled program.
func (n *FunctionNode) state() *functionState {
	return n.host.GetOrCreateNodeState(n.workflowID, n.GetID(), func() interface{} {
		s := &functionState{setupDone: make(chan struct{})}
		s.pool.New = func() interface{} { return goja.New() }
		return s
	}).(*functionState)
}

// acquire borrows a VM from the pool (lazily created via pool.New).
func (s *functionState) acquire() *goja.Runtime {
	v := s.pool.Get()
	if v == nil {
		return nil
	}
	return v.(*goja.Runtime)
}

// program returns the compiled script for the given slot
// ("func"|"initialize"|"finalize"), compiling and caching it on first
// use. The body is wrapped so `return msg`, node.send etc. work
// Node-RED style; the result is exposed as the __result global.
func (s *functionState) program(key, source string) (*goja.Program, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if prog, ok := s.progs[key]; ok {
		return prog, nil
	}
	// Node-RED wraps the body in an async function — top-level await
	// (e.g. `await node.linkcall(...)`) is legal there. goja resolves
	// promises whose awaited values are plain results inline, so
	// synchronous scripts still complete inside RunProgram and only
	// the promise-unboxing below changes. Lifecycle scripts get the
	// same signature minus msg (it is set to undefined for them).
	args := "(msg, node, context, flow, global, env)"
	if key != "func" {
		args = "(undefined, node, context, flow, global, env)"
	}
	wrapped := "var __result = (async function (msg, node, context, flow, global, env) {\n" +
		source + "\n})" + args + ";"
	prog, err := goja.Compile("function", wrapped, false)
	if err != nil {
		return nil, fmt.Errorf("function script compile error: %w", err)
	}
	if s.progs == nil {
		s.progs = map[string]*goja.Program{}
	}
	s.progs[key] = prog
	return prog, nil
}

// consoleFunc builds a console.* method — args are stringified and
// joined like the JS console, then emitted as a node event.
func (n *FunctionNode) consoleFunc(vm *goja.Runtime, level string) func(goja.FunctionCall) goja.Value {
	return func(call goja.FunctionCall) goja.Value {
		parts := make([]string, 0, len(call.Arguments))
		for _, a := range call.Arguments {
			parts = append(parts, a.String())
		}
		n.host.EmitEvent(n.workflowID, n.GetID(), n.GetID(), level, strings.Join(parts, " "))
		return goja.Undefined()
	}
}

// emitScriptError surfaces an error thrown inside a timer callback —
// the same shape as a synchronous script error.
func (n *FunctionNode) emitScriptError(err error) {
	n.host.EmitEvent(n.workflowID, n.GetID(), n.GetID(), "error", fmt.Sprintf("function script error: %v", err))
}

// Activate implements nodebase.Activator: runs the Node-RED "On Start"
// (initialize) script on deploy. A returned promise is awaited — the
// script's setTimeout callbacks drive it — so DeployWorkflow blocks
// until async setup settles, matching Node-RED. The state pointer is
// cached for Stop because host state is wiped before stoppers run.
func (n *FunctionNode) Activate(ctx context.Context) error {
	n.st = n.state()
	n.st.setupStarted.Store(true)
	defer n.st.setupOnce.Do(func() { close(n.st.setupDone) })
	if n.config.Initialize == "" {
		return nil
	}
	err := n.runLifecycleScript(ctx, "initialize", n.config.Initialize, true)
	if err != nil {
		n.st.setupErr = err
	}
	return err
}

// Stop implements nodebase.Stopper: cancels pending script timers and
// runs the "On Stop" (finalize) script. It must not re-resolve host
// state — the cached pointer from Activate is the only safe reference.
func (n *FunctionNode) Stop() error {
	st := n.st
	if st == nil {
		return nil
	}
	st.sessMu.Lock()
	sessions := make([]*scriptSession, 0, len(st.sessions))
	for s := range st.sessions {
		sessions = append(sessions, s)
	}
	st.sessMu.Unlock()
	for _, s := range sessions {
		s.close()
	}
	if n.config.Finalize == "" {
		return nil
	}
	return n.runLifecycleScript(context.Background(), "finalize", n.config.Finalize, false)
}

// StopKey dedupes stopper registration: executors are rebuilt per
// message, but only the activation executor should stop the node.
func (n *FunctionNode) StopKey() string {
	return "function:" + n.workflowID + ":" + n.GetID()
}

// runLifecycleScript executes an initialize/finalize script: globals
// are injected with msg undefined, the script runs inside the same
// async wrapper as the body (so top-level await works), and when
// waitPromise is set a pending __result promise is awaited — timer
// callbacks the script scheduled are what settles it.
func (n *FunctionNode) runLifecycleScript(ctx context.Context, key, source string, waitPromise bool) error {
	st := n.st
	vm := st.acquire()
	if vm == nil {
		return fmt.Errorf("function node %s: failed to create JS runtime", n.GetID())
	}
	sess := &scriptSession{vm: vm, n: n, st: st, closed: make(chan struct{})}
	n.sess = sess
	n.asyncMode.Store(true) // lifecycle sends go straight to the queue
	returnToPool := true
	defer func() {
		sess.endRun()
		if sess.hasTimers() {
			returnToPool = false
		}
		if returnToPool {
			st.pool.Put(vm)
		}
	}()

	if err := n.injectGlobals(vm, nil); err != nil {
		return err
	}
	prog, err := st.program(key, source)
	if err != nil {
		return err
	}
	sess.mu.Lock()
	_, runErr := vm.RunProgram(prog)
	result := vm.Get("__result")
	sess.mu.Unlock()
	if runErr != nil {
		return fmt.Errorf("function script error: %w", runErr)
	}
	if !waitPromise || result == nil {
		return nil
	}
	promise, ok := result.Export().(*goja.Promise)
	if !ok {
		return nil
	}
	for {
		sess.mu.Lock()
		state := promise.State()
		sess.mu.Unlock()
		switch state {
		case goja.PromiseStateFulfilled:
			return nil
		case goja.PromiseStateRejected:
			return fmt.Errorf("function script error: %v", promise.Result())
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// portKey maps a zero-based send index to the edge source port key.
func portKey(i int) string {
	return fmt.Sprintf("output_%d", i+1)
}

// msgToPayload converts an exported JS msg back into a nodestore.MessageInterface:
// "payload" becomes Payload, "_msgid" becomes the envelope ID, every
// other key becomes Metadata. A msg that kept the incoming _msgid is
// the same message passed through (Node-RED identity); anything else
// is a new instance — fresh ID with parentMsgId = the incoming ID.
func msgToPayload(v interface{}, parentID string) nodestore.MessageInterface {
	out := nodestore.NewMessage().SetTimestamp(time.Now())
	// nodestore.NewMessage pre-assigns an entity ID, so "new
	// instance" detection keys on whether the script carried an
	// _msgid through — not on GetID() being empty.
	passedThrough := false
	m, ok := v.(map[string]interface{})
	if !ok {
		out.SetPayload(v) // primitive return — carry it as the payload
	} else {
		if id, _ := m["_msgid"].(string); id != "" {
			out.SetID(id)
			passedThrough = true
			delete(m, "_msgid")
		}
		if p, ok := m["payload"]; ok {
			out.SetPayload(p)
			delete(m, "payload")
		}
		if len(m) > 0 {
			out.SetMetadata(m)
		}
	}
	if !passedThrough {
		out.SetID(nodebase.GenerateID())
		if parentID != "" {
			if out.GetMetadata() == nil {
				out.SetMetadata(map[string]interface{}{})
			}
			out.GetMetadata()["parentMsgId"] = parentID
		}
	}
	return out
}

// Validate checks the function node configuration
func (n *FunctionNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	if n.config.Func == "" {
		return fmt.Errorf("function node requires a func script")
	}
	if n.config.Outputs < 0 {
		return fmt.Errorf("invalid outputs %d", n.config.Outputs)
	}
	return nil
}
