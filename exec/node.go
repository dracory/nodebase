// Package exec contains the exec node implementation.
package exec

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"io"
	"os"
	oexec "os/exec"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(ExecConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewExecNode(n, c, e), nil
}

// init registers the exec node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "exec",
			Name:        "Exec",
			Description: "Runs a system command (requires the host to allow exec)",
			Icon:        "server",
			Category:    "Actions",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the exec node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"command": map[string]string{
			"type":        "string",
			"title":       "Command",
			"description": "Command line run through the system shell (requires the host to allow exec)",
		},
		"addpay": map[string]string{
			"type":        "string",
			"title":       "Append Property",
			"description": "Name of a message property (e.g. \"payload\") whose value is appended to the command",
		},
		"append": map[string]string{
			"type":        "string",
			"title":       "Extra Arguments",
			"description": "Additional arguments appended after the addpay value",
		},
		"useSpawn": map[string]interface{}{
			"type":        "boolean",
			"title":       "Spawn Mode",
			"description": "Spawn the process and stream stdout/stderr per chunk instead of buffering",
			"default":     false,
		},
		"timer": map[string]interface{}{
			"type":        "number",
			"title":       "Kill Timeout (seconds)",
			"description": "Kill the command after this many seconds; 0 = no timeout",
			"default":     0,
		},
		"oldrc": map[string]interface{}{
			"type":        "boolean",
			"title":       "Legacy Return Code",
			"description": "Emit the bare exit code as the third-output payload instead of {code:...}",
			"default":     false,
		},
		"errorCatch": nodebase.ErrorCatchSchemaField(),
	},
	"required": []string{"command"},
}

// ExecConfig holds configuration for the exec node — the Node-RED
// `exec` equivalent.
type ExecConfig struct {
	// Command is the base command line (trimmed). In buffered mode it
	// runs through the shell; in spawn mode it is split into argv.
	Command string `json:"command"`
	// AddPay is the Node-RED "append msg.X" field: boolean true means
	// "msg.payload", a string names another message property, and
	// false/absent appends nothing. It is normalized into
	// ExecNode.addpayProp at construction.
	AddPay interface{} `json:"addpay"`
	// Append holds extra arguments appended after the addpay value
	// (trimmed).
	Append string `json:"append"`
	// UseSpawn selects spawn mode (stream stdout/stderr chunks to the
	// first two outputs as they arrive). Node-RED serializes it as the
	// string "true"/"false" — DecodeConfig coerces.
	UseSpawn bool `json:"useSpawn"`
	// Timer is a kill timeout in seconds (0 = no timeout).
	Timer float64 `json:"timer"`
	// OldRC selects the legacy third-output payload: the bare exit
	// code instead of the {code:...} object. In buffered mode a
	// successful run then emits nothing on the third output
	// (Node-RED parity).
	OldRC bool `json:"oldrc"`
}

// execState is the per-node run state holding every live child
// process keyed by pid — the Node-RED activeProcesses map. Executors
// are rebuilt per message, so the registry lives in the host state
// slot; the deploy-time executor caches it on Activate so Stop() can
// kill survivors without touching (possibly wiped) host state.
type execState struct {
	mu    sync.Mutex
	procs map[int]*oexec.Cmd
}

func (s *execState) add(c *oexec.Cmd) {
	s.mu.Lock()
	if s.procs == nil {
		s.procs = map[int]*oexec.Cmd{}
	}
	s.procs[c.Process.Pid] = c
	s.mu.Unlock()
}

func (s *execState) remove(pid int) {
	s.mu.Lock()
	delete(s.procs, pid)
	s.mu.Unlock()
}

// kill sends the named signal to pid's process — or to the single
// active process when pid is 0 and exactly one is running (Node-RED
// kill semantics: no pid + several active processes kills none).
func (s *execState) kill(sig string, pid int) {
	s.mu.Lock()
	var targets []*oexec.Cmd
	if pid != 0 {
		if c, ok := s.procs[pid]; ok {
			targets = append(targets, c)
		}
	} else if len(s.procs) == 1 {
		for _, c := range s.procs {
			targets = append(targets, c)
		}
	}
	s.mu.Unlock()
	for _, c := range targets {
		signalProcess(c, sig)
	}
}

func (s *execState) killAll() {
	s.mu.Lock()
	var targets []*oexec.Cmd
	for _, c := range s.procs {
		targets = append(targets, c)
	}
	s.mu.Unlock()
	for _, c := range targets {
		if c.Process != nil {
			_ = c.Process.Kill()
		}
	}
}

// ExecNode runs a system command — the Node-RED `exec` node.
//
// Outputs (Node-RED wiring order):
//   - output_1: stdout (a message per chunk in spawn mode, the whole
//     buffer in exec mode)
//   - output_2: stderr (only emitted while the command produces
//     stderr; spawn mode streams per chunk)
//   - output_3: the return code — {code:N} / {code:null,signal:...} /
//     {code:N,message:"Command failed: ..."} unless oldrc, which emits
//     the bare code (and in buffered mode nothing on success).
//
// SECURITY: arbitrary command execution is gated behind
// NodeHost.ExecAllowed() — the node fails with "exec disabled"
// unless the flag is set (the consuming application wires it in main).
// The addpay value is concatenated into the command line exactly
// like Node-RED does, so even an enabled exec node is only as safe as
// the messages routed into it.
type ExecNode struct {
	*nodebase.BaseNode
	config     *ExecConfig
	host       nodebase.NodeHost
	workflowID string
	// addpayProp is the resolved message-property name whose value is
	// appended VERBATIM to the command line ("" = nothing appended).
	addpayProp string
	// st caches the shared process registry resolved at Activate —
	// Stop() uses it so a wiped state slot cannot strand children.
	st *execState
}

// NewExecNode creates a new exec node
func NewExecNode(node nodestore.NodeInterface, config *ExecConfig, host nodebase.NodeHost) *ExecNode {
	n := &ExecNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
	}
	// Node-RED: addpay === true means "append msg.payload"; a string
	// names that property; anything else appends nothing.
	switch v := config.AddPay.(type) {
	case bool:
		if v {
			n.addpayProp = "payload"
		}
	case string:
		n.addpayProp = v
	}
	return n
}

// shellCommand wraps the command line in the platform shell. Node-RED
// runs exec() through /bin/bash on Linux when it exists (its execOpt
// shell), so bash error wording like "line 1" survives downstream.
func shellCommand(full string) (string, []string) {
	if runtime.GOOS == "windows" {
		return "cmd", []string{"/C", full}
	}
	if runtime.GOOS == "linux" && fileExists("/bin/bash") {
		return "/bin/bash", []string{"-c", full}
	}
	return "sh", []string{"-c", full}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// argvRe is Node-RED's spawn-mode splitter: whitespace-delimited
// tokens, double-quoted spans kept whole (quotes stripped after).
var argvRe = regexp.MustCompile(`[^\s"]+|"[^"]*"`)

func splitArgs(arg string) []string {
	tokens := argvRe.FindAllString(arg, -1)
	for i, t := range tokens {
		if len(t) >= 2 && strings.HasPrefix(t, `"`) && strings.HasSuffix(t, `"`) {
			tokens[i] = t[1 : len(t)-1]
		}
	}
	return tokens
}

// Execute handles a message: a "kill" property signals the active
// process(es) and produces no output; otherwise the command runs —
// buffered through the shell (exec mode) or as a streamed spawn.
func (n *ExecNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	if n.host == nil || !n.host.ExecAllowed() {
		return nil, errors.New("exec disabled: set AllowExec on the host")
	}
	st := n.procState()

	if v, ok := nodebase.LookupProperty(input, "kill"); ok {
		sig, _ := v.(string)
		if sig == "" || !strings.HasPrefix(strings.ToUpper(sig), "SIG") {
			sig = "SIGTERM"
		}
		sig = strings.ToUpper(sig)
		pid := 0
		if pv, ok := nodebase.LookupProperty(input, "pid"); ok {
			if f, err := nodebase.ToFloat(pv); err == nil {
				pid = int(f)
			}
		}
		st.kill(sig, pid)
		return nil, nil
	}

	if strings.TrimSpace(n.config.Command) == "" {
		return nil, errors.New("exec node requires a command")
	}

	arg := strings.TrimSpace(n.config.Command)
	if n.addpayProp != "" {
		if v, ok := nodebase.LookupProperty(input, n.addpayProp); ok {
			arg += " " + fmt.Sprintf("%v", v)
		}
	}
	if ap := strings.TrimSpace(n.config.Append); ap != "" {
		arg += " " + ap
	}

	if n.config.UseSpawn {
		return n.executeSpawn(input, st, arg)
	}
	return n.executeBuffered(ctx, input, st, arg)
}

// executeBuffered is Node-RED's exec() path: run the whole command
// through the shell, then send stdout on output_1, stderr on
// output_2 (only when non-empty) and the return code on output_3.
// The first output is the Execute return value; the other two are
// emitted port-scoped so a missing stderr or a legacy oldrc config
// can leave their outputs silent.
func (n *ExecNode) executeBuffered(ctx context.Context, input nodestore.MessageInterface, st *execState, arg string) (nodestore.MessageInterface, error) {
	if n.config.Timer > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(n.config.Timer*float64(time.Second)))
		defer cancel()
	}

	name, args := shellCommand(arg)
	cmd := oexec.CommandContext(ctx, name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	runErr := cmd.Start()
	if runErr != nil {
		return nil, fmt.Errorf("exec failed: %w", runErr)
	}
	st.add(cmd)
	defer st.remove(cmd.Process.Pid)
	runErr = cmd.Wait()

	var msg2, msg3 nodestore.MessageInterface
	if s := stderr.String(); s != "" {
		msg2 = nodebase.ClonePayload(input)
		msg2.SetPayload(s)
	}
	out := nodebase.ClonePayload(input)
	out.SetPayload(stdout.String())

	// Node's child_process.exec surfaces failures as an error whose
	// code is the exit code (null when signaled) and whose message is
	// "Command failed: <cmd>\n<stderr>" — reproduced verbatim here.
	if runErr != nil {
		rc := map[string]interface{}{}
		code, signal := exitCodeAndSignal(cmd.ProcessState)
		if code < 0 {
			rc["code"] = nil
			if signal != "" {
				rc["signal"] = signal
			}
		} else {
			rc["code"] = code
		}
		rc["message"] = "Command failed: " + arg + "\n" + stderr.String()
		msg3 = nodebase.ClonePayload(input)
		msg3.SetPayload(rc)
	} else if !n.config.OldRC {
		msg3 = nodebase.ClonePayload(input)
		msg3.SetPayload(map[string]interface{}{"code": 0})
	}

	if msg3 != nil {
		setMetadataValue(out, "rc", msg3.GetPayload())
		if msg2 != nil {
			setMetadataValue(msg2, "rc", msg3.GetPayload())
		}
	}

	// The second and third outputs travel on their own port-scoped
	// traversals. Hosts that call Execute detached (unit tests) simply
	// record or drop the emit — errors are ignored like the spawn path.
	if msg2 != nil {
		_ = n.host.Emit(n.workflowID, n.GetID(), msg2, []string{"output_2"})
	}
	if msg3 != nil {
		_ = n.host.Emit(n.workflowID, n.GetID(), msg3, []string{"output_3"})
	}
	return out, nil
}

// executeSpawn is Node-RED's spawn() path: start the process, stream
// every stdout chunk to output_1 and stderr chunk to output_2 as a
// clone of the input message, then emit the exit object on output_3
// when the process closes. Execute returns immediately — the outputs
// all travel via host.Emit on their own traversals.
func (n *ExecNode) executeSpawn(input nodestore.MessageInterface, st *execState, arg string) (nodestore.MessageInterface, error) {
	tokens := splitArgs(arg)
	if len(tokens) == 0 {
		return nil, errors.New("exec node requires a command")
	}
	cmd := oexec.Command(tokens[0], tokens[1:]...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("exec failed: %w", err)
	}
	st.add(cmd)
	pid := cmd.Process.Pid

	if n.config.Timer > 0 {
		time.AfterFunc(time.Duration(n.config.Timer*float64(time.Second)), func() {
			st.kill("SIGKILL", pid)
		})
	}

	stream := func(r io.Reader, port string) {
		buf := make([]byte, 64*1024)
		for {
			nr, err := r.Read(buf)
			if nr > 0 {
				chunk := nodebase.ClonePayload(input)
				chunk.SetPayload(string(buf[:nr]))
				_ = n.host.Emit(n.workflowID, n.GetID(), chunk, []string{port})
			}
			if err != nil {
				return
			}
		}
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); stream(stdout, "output_1") }()
	go func() { defer wg.Done(); stream(stderr, "output_2") }()

	go func() {
		_ = cmd.Wait()
		wg.Wait()
		st.remove(pid)

		code, signal := exitCodeAndSignal(cmd.ProcessState)
		rc := nodebase.ClonePayload(input)
		if n.config.OldRC {
			rc.SetPayload(code)
		} else {
			payload := map[string]interface{}{}
			if code < 0 {
				payload["code"] = nil
				if signal != "" {
					payload["signal"] = signal
				}
			} else {
				payload["code"] = code
			}
			rc.SetPayload(payload)
		}
		_ = n.host.Emit(n.workflowID, n.GetID(), rc, []string{"output_3"})
	}()

	return nil, nil
}

// SelectPorts always routes the returned stdout message to output_1 —
// the stderr and return-code messages are emitted separately on their
// own ports.
func (n *ExecNode) SelectPorts(input, output nodestore.MessageInterface) []string {
	return []string{"output_1"}
}

// Activate implements nodebase.Activator: the exec node is a Stopper
// (it must kill spawned children on undeploy) but activation is not a
// message run — it only resolves the shared process registry so
// Stop() can reach it later.
func (n *ExecNode) Activate(ctx context.Context) error {
	n.procState()
	return nil
}

// Stop implements nodebase.Stopper: kill every child process this
// node still has running (Node-RED kills activeProcesses on close).
func (n *ExecNode) Stop() error {
	if n.st != nil {
		n.st.killAll()
	}
	return nil
}

// StopKey dedupes stopper registration across the per-message
// executor shells of one node.
func (n *ExecNode) StopKey() string {
	return "exec:" + n.workflowID + ":" + n.GetID()
}

// procState returns the shared process registry for this node.
func (n *ExecNode) procState() *execState {
	if n.st != nil {
		return n.st
	}
	if n.host == nil {
		return &execState{}
	}
	n.st = n.host.GetOrCreateNodeState(n.workflowID, n.GetID(), func() interface{} {
		return &execState{}
	}).(*execState)
	return n.st
}

// setMetadataValue writes a metadata key, creating the map when needed.
func setMetadataValue(msg nodestore.MessageInterface, key string, v interface{}) {
	if msg.GetMetadata() == nil {
		msg.SetMetadata(map[string]interface{}{})
	}
	msg.GetMetadata()[key] = v
}

// Validate checks the exec node configuration
func (n *ExecNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	if n.config.Timer < 0 {
		return fmt.Errorf("timer cannot be negative")
	}
	return nil
}
