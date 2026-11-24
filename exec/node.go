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
	"os/exec"
	"runtime"
	"strings"
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
			Description: "Runs a system command (requires GOFLOW_ALLOW_EXEC=1)",
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
			"description": "Command line run through the system shell (requires GOFLOW_ALLOW_EXEC=1)",
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
		"timer": map[string]interface{}{
			"type":        "number",
			"title":       "Kill Timeout (seconds)",
			"description": "Kill the command after this many seconds; 0 = no timeout",
			"default":     0,
		},
		"errorCatch": nodebase.ErrorCatchSchemaField(),
	},
	"required": []string{"command"},
}

// ExecConfig holds configuration for the exec node — the Node-RED
// `exec` equivalent (buffered mode only).
type ExecConfig struct {
	// Command is the base command line to run through the system shell.
	Command string `json:"command"`
	// AddPay names a payload/metadata property whose value is appended
	// VERBATIM to the command line (empty = nothing appended). This is
	// Node-RED parity — the shell expands it, so an addpay fed by
	// untrusted input (e.g. an httpTrigger body) can inject shell
	// syntax. Only enable exec (AllowExec) on trusted flows.
	AddPay string `json:"addpay"`
	// Append holds extra arguments appended after the addpay value.
	Append string `json:"append"`
	// Timer is a kill timeout in seconds (0 = no timeout).
	Timer float64 `json:"timer"`
}

// ExecNode runs a system command and returns stdout/stderr/rc.
//
// SECURITY: arbitrary command execution is gated behind
// WorkflowEngine.ExecAllowed() — the node fails with "exec disabled"
// unless the flag is set (env GOFLOW_ALLOW_EXEC=1 wires it in main).
// The addpay value is concatenated into the shell command line exactly
// like Node-RED does, so even an enabled exec node is only as safe as
// the messages routed into it.
type ExecNode struct {
	*nodebase.BaseNode
	config *ExecConfig
	host   nodebase.NodeHost
	// rc holds the last run's exit code so SelectPorts can route to
	// output_1 (rc 0) or output_3 (non-zero). Executors are rebuilt
	// per message, but Execute and SelectPorts run on the same
	// instance, so an instance field is safe.
	rc int
}

// NewExecNode creates a new exec node
func NewExecNode(node nodestore.NodeInterface, config *ExecConfig, host nodebase.NodeHost) *ExecNode {
	return &ExecNode{
		BaseNode: nodebase.NewBaseNode(node),
		config:   config,
		host:     host,
	}
}

// shellCommand wraps the command line in the platform shell:
// cmd /C on Windows, sh -c elsewhere.
func shellCommand(full string) (string, []string) {
	if runtime.GOOS == "windows" {
		return "cmd", []string{"/C", full}
	}
	return "sh", []string{"-c", full}
}

// Execute builds the full command line (command + addpay-resolved value
// + append), runs it buffered through the shell and returns stdout as
// Data with rc/stderr in Metadata.
func (n *ExecNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	if n.host == nil || !n.host.ExecAllowed() {
		return nil, errors.New("exec disabled: set AllowExec on the host (GOFLOW_ALLOW_EXEC=1)")
	}
	if strings.TrimSpace(n.config.Command) == "" {
		return nil, errors.New("exec node requires a command")
	}

	// Build the full command line
	parts := []string{n.config.Command}
	if n.config.AddPay != "" {
		if v, ok := nodebase.LookupProperty(input, n.config.AddPay); ok {
			parts = append(parts, fmt.Sprintf("%v", v))
		}
	}
	if n.config.Append != "" {
		parts = append(parts, n.config.Append)
	}
	full := strings.Join(parts, " ")

	if n.config.Timer > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(n.config.Timer*float64(time.Second)))
		defer cancel()
	}

	name, args := shellCommand(full)
	cmd := exec.CommandContext(ctx, name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	runErr := cmd.Run()
	n.rc = 0
	if runErr != nil {
		var exitErr *exec.ExitError
		if errors.As(runErr, &exitErr) {
			n.rc = exitErr.ExitCode()
		} else {
			// Could not even start the process (bad shell, etc.)
			return nil, fmt.Errorf("exec failed: %w", runErr)
		}
	}

	metadata := nodebase.CloneMetadata(input)
	metadata["rc"] = n.rc
	metadata["stderr"] = stderr.String()

	data := stdout.String()
	if n.rc != 0 {
		// Failure output: stderr is what matters downstream
		data = stderr.String()
	}
	return nodestore.NewMessage().SetPayload(data).SetMetadata(metadata).SetTimestamp(time.Now()), nil
}

// SelectPorts routes rc 0 to output_1 (stdout); a non-zero exit goes to
// output_3 (Node-RED's "return code" port) carrying stderr + rc.
func (n *ExecNode) SelectPorts(input, output nodestore.MessageInterface) []string {
	if n.rc != 0 {
		return []string{"output_3"}
	}
	return []string{"output_1"}
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
