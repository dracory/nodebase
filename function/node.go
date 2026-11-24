// Package function contains the function node implementation.
package function

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/dop251/goja"
	"os"
	nodestore "github.com/dracory/nodestore"
	"github.com/dracory/nodebase"
	"sync"
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
}

// functionSend records one message the script pushed out through
// node.send or its return value. Port -1 means "auto" (first output).
type functionSend struct {
	port int
	msg  nodestore.MessageInterface
}

// functionState is the per-node host state. goja VMs are NOT
// goroutine-safe and executors are rebuilt per message, so VMs are
// pooled here and each Execute borrows one, runs the script, then
// returns it. Node-RED context (context/flow/global) is NOT kept here —
// it lives in the host's ContextStore so it survives redeploys and,
// with a persistent backend, process restarts.
type functionState struct {
	pool   sync.Pool // *goja.Runtime
	mu     sync.Mutex
	prog   *goja.Program
	progOk bool
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

	vm := state.acquire()
	if vm == nil {
		return nil, fmt.Errorf("function node %s: failed to create JS runtime", n.GetID())
	}
	// The VM is returned to the pool on success; a timed-out run may
	// carry a stale interrupt flag, so that VM is discarded instead.
	returnToPool := true
	defer func() {
		if returnToPool {
			state.pool.Put(vm)
		}
	}()

	// Fresh per-run API surface — closures capture this executor so
	// sends land on this message's collector, not a pooled one.
	if err := n.injectGlobals(vm, input); err != nil {
		return nil, err
	}

	prog, err := state.program(n.config.Func)
	if err != nil {
		return nil, err
	}

	// Run the script in a goroutine so a wall-clock timeout can
	// interrupt the VM (goja checks the interrupt flag in its loop).
	runDone := make(chan error, 1)
	go func() {
		_, err := vm.RunProgram(prog)
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
	if runErr != nil {
		return nil, fmt.Errorf("function script error: %w", runErr)
	}
	if n.nodeErr != "" {
		return nil, fmt.Errorf("function node.error: %s", n.nodeErr)
	}

	// A non-null return value counts as an implicit send on output_1.
	if result := vm.Get("__result"); result != nil && !goja.IsUndefined(result) && !goja.IsNull(result) {
		n.handleSend(result.Export())
	}

	return n.dispatch()
}

// SelectPorts restricts outgoing edges to the ports that received a
// first message — consistent with the payload Execute returns (the
// first send on the lowest-indexed port).
func (n *FunctionNode) SelectPorts(input, output nodestore.MessageInterface) []string {
	return n.firstPorts
}

// dispatch resolves the collected sends into the host's single-return
// model: the first send on the lowest-indexed port becomes Execute's
// return value (routed by SelectPorts to that port's edges); every
// other send — including additional messages on the same port — is
// emitted fire-and-forget, scoped to its own port, so each port's
// downstream receives its own payload.
func (n *FunctionNode) dispatch() (nodestore.MessageInterface, error) {
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
		if _, seen := firstByPort[p]; !seen {
			firstByPort[p] = i
			n.firstPorts = append(n.firstPorts, portKey(p))
			if p < retPort {
				retPort = p
				ret = s.msg
			}
			continue
		}
		// Extra message on an already-seen port: emit port-scoped.
		_ = n.host.Emit(n.workflowID, n.GetID(), s.msg, []string{portKey(p)})
	}
	// First messages on ports other than the returned one get their own
	// payload emitted port-scoped so they aren't shadowed by the return.
	for p, i := range firstByPort {
		if p != retPort {
			_ = n.host.Emit(n.workflowID, n.GetID(), n.sends[i].msg, []string{portKey(p)})
		}
	}
	return ret, nil
}

// handleSend interprets a send argument (node.send call arg or the
// script's return value): an array is a per-port list (index → port,
// null skips the port); anything else is a message for port -1 (auto).
func (n *FunctionNode) handleSend(v interface{}) {
	if arr, ok := v.([]interface{}); ok {
		for i, item := range arr {
			if item == nil {
				continue
			}
			n.sends = append(n.sends, functionSend{port: i, msg: msgToPayload(item, n.inputID)})
		}
		return
	}
	n.sends = append(n.sends, functionSend{port: -1, msg: msgToPayload(v, n.inputID)})
}

// injectGlobals sets the script-visible globals on the VM: msg, node
// (send/error/done), the Node-RED context scopes (context = node,
// flow, global — all backed by the host ContextStore) and env.
// All closures capture this executor, so they only live for one
// Execute call.
func (n *FunctionNode) injectGlobals(vm *goja.Runtime, input nodestore.MessageInterface) error {
	msg := map[string]interface{}{}
	for k, v := range input.GetMetadata() {
		msg[k] = v
	}
	msg["payload"] = input.GetPayload()
	// Node-RED msg._msgid is the per-instance envelope ID; msgToPayload
	// maps it back on the way out.
	msg["_msgid"] = input.GetID()
	if err := vm.Set("msg", msg); err != nil {
		return err
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
	}
	if err := vm.Set("node", nodeObj); err != nil {
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
	// OS fallback is deliberate (flows read e.g. credentials from
	// env), not an oversight.
	envObj := map[string]interface{}{
		"get": func(call goja.FunctionCall) goja.Value {
			if len(call.Arguments) == 0 {
				return goja.Undefined()
			}
			name := call.Arguments[0].String()
			if v, ok := n.host.NodeEnv(n.workflowID, n.GetID(), name); ok {
				return vm.ToValue(v)
			}
			return vm.ToValue(os.Getenv(name))
		},
	}
	if err := vm.Set("env", envObj); err != nil {
		return err
	}

	return vm.Set("__result", goja.Undefined())
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
		s := &functionState{}
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

// program returns the compiled script, compiling and caching it on
// first use. The body is wrapped so `return msg`, node.send etc. work
// Node-RED style; the result is exposed as the __result global.
func (s *functionState) program(source string) (*goja.Program, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.progOk {
		return s.prog, nil
	}
	wrapped := "var __result = (function (msg, node, context, flow, global, env) {\n" +
		source + "\n})(msg, node, context, flow, global, env);"
	prog, err := goja.Compile("function", wrapped, false)
	if err != nil {
		return nil, fmt.Errorf("function script compile error: %w", err)
	}
	s.prog = prog
	s.progOk = true
	return prog, nil
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
