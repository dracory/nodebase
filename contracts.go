package nodebase

import (
	"context"

	nodestore "github.com/dracory/nodestore"
)

// This file holds the runtime contracts every executor is written
// against. They are the seam between a node pack (this repository's
// built-ins, or any third-party package) and a runner: a runner
// discovers which of these an executor offers; an executor only ever
// sees nodestore entities plus the NodeHost the runner hands it.
// Nothing here may depend on a concrete host.

// NodeExecutor defines the interface that all nodes must implement.
type NodeExecutor interface {
	// ID returns the unique identifier for the node.
	ID() string

	// Type returns the type of the node.
	Type() string

	// Execute performs the node's operation.
	Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error)

	// Validate checks if the node is properly configured.
	Validate() error
}

// PortRouter is implemented by nodes that route their output to specific
// ports (e.g. the switch node). After such a node executes, only edges
// whose SourceKey is in the returned set are followed; other wired
// branches receive no message.
type PortRouter interface {
	// SelectPorts returns the output port keys (e.g. "output_1") that
	// the produced message should be routed to. An empty result means
	// the message dies at this node (Node-RED switch behaviour).
	SelectPorts(input, output nodestore.MessageInterface) []string
}

// MessageExpander is implemented by nodes that turn a single output
// message into a sequence of messages (e.g. the split node). After the
// node executes, each returned payload is enqueued separately on the
// node's selected outgoing edges — routing via PortRouter still applies.
type MessageExpander interface {
	// Expand returns the messages that replace the node's single output.
	// An empty result means no message continues downstream.
	Expand(output nodestore.MessageInterface) []nodestore.MessageInterface
}

// Stopper is implemented by nodes that hold long-lived resources
// (e.g. an HTTP trigger's listener) which must be shut down when the
// workflow stops or restarts.
type Stopper interface {
	Stop() error
}

// Activator is an optional companion to Stopper. When implemented, the
// deploy-time activation pass calls Activate instead of Execute, so
// nodes that ALSO receive wired messages (e.g. queue) can tell the
// activation call apart from a message delivery.
type Activator interface {
	Activate(ctx context.Context) error
}

// StopKeyer lets a Stopper dedupe itself in the runner's per-run
// stopper registry: nodes whose executors are rebuilt per message but
// share one underlying resource (e.g. the queue drainer) return a
// stable key so only the first instance is registered.
type StopKeyer interface {
	// StopKey returns the deduplication key for stopper registration.
	StopKey() string
}

// RunEvent is an extra event an executor may emit after a successful
// run (e.g. the debug node's formatted payload, the log node's
// configured message). The runner appends each returned event under
// the node that produced it.
type RunEvent struct {
	Level   string // info | warn | error | debug
	Message string
}

// RunEventer is implemented by executors that emit extra events after
// a successful Execute. The runner emits each returned event under the
// node that produced it.
type RunEventer interface {
	// PostRunEvents returns the events to append after a successful
	// run. The implementation may also produce side effects (e.g. the
	// debug node's console print).
	PostRunEvents(node nodestore.NodeInterface, out nodestore.MessageInterface) []RunEvent
}
