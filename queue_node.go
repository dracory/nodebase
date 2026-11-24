package nodebase

import (
	"context"
	"fmt"
	"strings"
	"sync"

	nodestore "github.com/dracory/nodestore"
)

// This file holds the executor shells for the queue and checkpoint
// node types. The staging Queue machinery itself is a host service
// (NodeHost.QueueService): executors are rebuilt per message, so the
// real queue lives in the host's per-node state and the shell only
// resolves it through the QueueContract seam.
//
// A checkpoint IS a queue that never drains: every staged item parks
// until a human releases it via one of the node's named routes, and
// the route picks WHICH outgoing edge (output port) the item takes —
// Enfocus Switch's Checkpoint element.

// QueueConfig holds the node.GetData() fields of a queue node.
type QueueConfig struct {
	// Capacity is the max queued items (<=0 unbounded). Alias for
	// inboxCapacity. Kept interface{} so a loosely-typed editor value
	// cannot fail the config decode — the authoritative parse is the
	// host's queue-config resolution.
	Capacity interface{} `json:"capacity"`
	// Policy is the overflow policy: block | drop-newest | drop-oldest |
	// route-to-catch. Alias for inboxPolicy. interface{} for the same
	// loose-typing reason as Capacity.
	Policy interface{} `json:"policy"`
	// Hold starts the queue held: deliveries enqueue but nothing
	// drains. interface{} — truthy parsing accepts bools, strings and
	// numbers.
	Hold interface{} `json:"hold"`
	// Backend selects where items are persisted: "durable" (default,
	// the host's durable inbox store — multi-runner) or "dir" (one
	// file per item under Dir — materialized for OS tooling).
	Backend interface{} `json:"backend"`
	// Dir is the target directory for backend="dir". A local path (or
	// NFS/SMB/SSHFS mount) uses the local adapter; an sftp:// or
	// ftp:// locator stores item files on the remote directory
	// through the Fs seam — remote backends are POLLING.
	Dir string `json:"dir"`
	// Password authenticates remote locators (credential field —
	// masked, stored outside the exported flow document).
	Password string `json:"password"`
	// KeyFile is a local path to an SSH private key for sftp locators
	// (Password doubles as the key passphrase).
	KeyFile string `json:"keyFile"`
	// HostKey pins the expected sftp host public key (authorized_keys
	// or base64 form); empty accepts any key (insecure, logged).
	HostKey string `json:"hostKey"`
}

// CheckpointConfig holds the node.GetData() fields of a checkpoint
// node: the queue's capacity/policy/backend/dir fields plus the
// ordered route labels. There is no `hold` — a checkpoint is always
// held.
type CheckpointConfig struct {
	QueueConfig
	// Routes is the ordered list of route labels; routes[i] labels
	// output_{i+1}. Accepts a JSON array or a comma-separated string.
	Routes interface{} `json:"routes"`
}

// ParseCheckpointRoutes normalizes the loose `routes` config value
// into an ordered label list. Entries may be blank (the port falls
// back to its "output_N" key as the label).
func ParseCheckpointRoutes(v interface{}) []string {
	var raw []interface{}
	switch r := v.(type) {
	case []interface{}:
		raw = r
	case []string:
		out := make([]string, len(r))
		copy(out, r)
		return out
	case string:
		// Comma-separated shorthand: "approve, decline".
		for _, part := range strings.Split(r, ",") {
			raw = append(raw, strings.TrimSpace(part))
		}
	default:
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		s, _ := item.(string)
		out = append(out, strings.TrimSpace(s))
	}
	return out
}

// QueueNode is the executor shell for the queue (and checkpoint) node
// types. It is a Stopper + Activator: activation (deploy) arms the
// shared drainer; message-time Execute stages the payload into the
// queue.
type QueueNode struct {
	*BaseNode
	config     *QueueConfig
	host       NodeHost
	workflowID string
	node       nodestore.NodeInterface
	// checkpoint marks this executor as a checkpoint node: the shared
	// queue never drains on its own (manual) and carries the node's
	// named routes for route-parameterized releases.
	checkpoint bool
	routes     []string
	// qCached holds the shared queue resolved by queue() so Stop can
	// shut down the real drainer after the host wiped node state
	// (stop/redeploy clears state before stopping stoppers —
	// recreating here would leak the live drainer).
	qMu     sync.Mutex
	qCached QueueContract
}

// NewQueueNode creates a new queue node executor shell.
func NewQueueNode(node nodestore.NodeInterface, config *QueueConfig, host NodeHost) *QueueNode {
	return &QueueNode{
		BaseNode:   NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
		node:       node,
	}
}

// NewCheckpointNode creates the executor shell for a checkpoint node.
// It reuses QueueNode wholesale — the difference lives on the shared
// queue (manual + routes).
func NewCheckpointNode(node nodestore.NodeInterface, config *CheckpointConfig, host NodeHost) *QueueNode {
	n := NewQueueNode(node, &config.QueueConfig, host)
	n.BaseNode = NewBaseNode(node)
	n.checkpoint = true
	n.routes = ParseCheckpointRoutes(config.Routes)
	return n
}

// queueService resolves the host's queue capability, reporting a
// descriptive error when the host offers none.
func (n *QueueNode) queueService() (QueueService, error) {
	qs := n.host.QueueService()
	if qs == nil {
		return nil, fmt.Errorf("queue %q requires a host queue service (QueueService() returned nil)", NodeName(n.node))
	}
	return qs, nil
}

// queue returns the shared staging queue for the node, creating it on
// first use.
func (n *QueueNode) queue() (QueueContract, error) {
	qs, err := n.queueService()
	if err != nil {
		return nil, err
	}
	q, err := qs.EnsureQueue(n.node, n.checkpoint, n.routes)
	if err != nil {
		return nil, err
	}
	n.qMu.Lock()
	n.qCached = q
	n.qMu.Unlock()
	return q, nil
}

// Validate enforces the queue's hard durability requirement and
// normalizes policy config early (deploy-time failure, not message-time).
func (n *QueueNode) Validate() error {
	backend, _ := n.config.Backend.(string)
	backend = strings.ToLower(strings.TrimSpace(backend))
	if backend == "" {
		backend = "durable"
	}
	switch backend {
	case "durable":
		qs, err := n.queueService()
		if err != nil {
			return err
		}
		if !qs.HasDurableStore() {
			return fmt.Errorf("queue %q requires a durable inbox store (no durable store configured)", NodeName(n.node))
		}
	case "dir":
		if strings.TrimSpace(n.config.Dir) == "" {
			return fmt.Errorf("queue %q backend \"dir\" requires a dir path", NodeName(n.node))
		}
	default:
		return fmt.Errorf("queue %q has unknown backend %q", NodeName(n.node), backend)
	}
	if p, _ := n.config.Policy.(string); p != "" {
		if _, ok := ParseOverflowPolicy(p); !ok {
			return fmt.Errorf("queue %q has invalid policy %q", NodeName(n.node), p)
		}
	}
	return nil
}

// Activate implements Activator: the deploy-time call arms the shared
// drainer and recovers surviving durable rows. It never stages the
// activation payload. The initial hold state was already resolved
// into the queue at creation (node.GetData()["hold"]).
func (n *QueueNode) Activate(ctx context.Context) error {
	q, err := n.queue()
	if err != nil {
		return err
	}
	q.Arm(ctx)
	return nil
}

// Execute stages the delivered payload into the queue. Output is
// nil — the message was staged, not passed through; the drainer emits
// it downstream later under the hold/release gate.
func (n *QueueNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	q, err := n.queue()
	if err != nil {
		return nil, err
	}
	if err := q.Enqueue(input); err != nil {
		return nil, err
	}
	return nil, nil
}

// Stop implements Stopper: shuts down the shared drainer. Must not
// recreate host state — it may already be wiped, in which case the
// cached queue is the only reference to the live drainer.
func (n *QueueNode) Stop() error {
	n.qMu.Lock()
	q := n.qCached
	n.qMu.Unlock()
	if q == nil {
		return nil
	}
	q.Stop()
	return nil
}

// StopKey dedupes stopper registration: executors are rebuilt per
// message, but all shells of one queue share the queue's drainer.
func (n *QueueNode) StopKey() string {
	return "queue:" + n.workflowID + ":" + n.node.GetID()
}
