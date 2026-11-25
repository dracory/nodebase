package nodebase

import (
	"context"
	"crypto/tls"
	"net/http"

	nodestore "github.com/dracory/nodestore"
)

// NodeHost is the runtime seam between an executor and the runner that
// hosts it. It is the ONLY thing an executor sees of its host: every
// built-in node is written against NodeHost, never against a concrete
// host type, so any runner implementing NodeHost can host this pack
// and any pack written against NodeHost runs on any such runner.
//
// The surface covers the four things executors legitimately need —
// emission back into the graph (Emit, InjectMessage, DeliverSync-style
// helpers), run state (node state slots, running flow, run context),
// host services (HTTP router/authenticator, TLS config resolution,
// context store, file root, exec gate, env lookup) and lifecycle
// correlation (pending responses, queue service).
type NodeHost interface {
	// Emit routes payload out of nodeID's outgoing edges (all of them
	// when ports is nil) inside the running workflow.
	Emit(workflowID, nodeID string, payload nodestore.MessageInterface, ports []string) error

	// EmitEvent appends a timestamped log entry (info|warn|error|debug)
	// for the node — feeds the editor's debug panel and status badges.
	EmitEvent(workflowID, nodeID, nodeName, level, message string)

	// InjectMessage routes an externally-originated payload (e.g. an
	// HTTP trigger request) through the nodes downstream of fromNodeID.
	// It returns a message ID whose response can be awaited via
	// ResolveResponse — used by response-style nodes to reply to the
	// original request.
	InjectMessage(workflowID, fromNodeID string, payload nodestore.MessageInterface) (string, chan nodestore.MessageInterface, error)

	// ResolveResponse delivers payload to the party awaiting the
	// response channel returned by InjectMessage.
	ResolveResponse(msgID string, payload nodestore.MessageInterface) error

	// DiscardPending drops the response correlation registered under
	// msgID (response node fired with nothing waiting, or a timeout).
	DiscardPending(msgID string)

	// LookupNode returns the stored definition of nodeID within the
	// running workflow — used for link targets and config-node
	// references (e.g. tlsConfig).
	LookupNode(workflowID, nodeID string) (nodestore.NodeInterface, bool)

	// RunningFlow returns the expanded in-memory workflow definition
	// the run is executing. ok is false when the workflow is not
	// running on this host.
	RunningFlow(workflowID string) (nodestore.WorkflowInterface, bool)

	// RunContext returns the per-run context cancelled when the
	// workflow stops — long-lived nodes select on it so in-flight
	// work stops promptly on undeploy.
	RunContext(workflowID string) context.Context

	// GetOrCreateNodeState returns the per-run state slot for
	// (workflowID, nodeID), calling factory to create it on first use.
	// Executors are rebuilt per message, so stateful nodes keep their
	// buffers here.
	GetOrCreateNodeState(workflowID, nodeID string, factory func() interface{}) interface{}

	// ExecuteDownstream continues the traversal downstream of
	// fromNodeID on a separate path — for nodes that emit outside the
	// synchronous Execute return (e.g. linkCall's deferred fan-out).
	ExecuteDownstream(ctx context.Context, workflowID, fromNodeID string, workflow nodestore.WorkflowInterface, payload nodestore.MessageInterface, ports []string)

	// NodeEnv resolves an environment variable in node scope
	// (Node-RED env.get: group/component/flow layers, then global env,
	// then the OS environment).
	NodeEnv(workflowID, nodeID, name string) (interface{}, bool)

	// ResolvedContextStore returns the store backing function-node
	// context (node/flow/global scopes): the host's configured store,
	// or a process-local in-memory one when none is wired.
	ResolvedContextStore() ContextStore

	// LoadTLSConfig resolves a tlsConfig node reference (node id
	// within the workflow) into a ready *tls.Config for a client
	// dial.
	LoadTLSConfig(workflowID, ref string) (*tls.Config, error)

	// FileRootPath returns the root directory file nodes are
	// restricted to. Empty means unrestricted.
	FileRootPath() string

	// ExecAllowed reports whether the exec node may run arbitrary
	// system commands on this host.
	ExecAllowed() bool

	// SharedHTTPRouter returns the host's shared-listener seam: an
	// optional router HTTP-serving nodes mount routes on instead of
	// binding a private TCP listener (the Node-RED model where an
	// http-in node names a path served on the host's own port).
	// Nil means no shared router exists.
	SharedHTTPRouter() HTTPRouter

	// HTTPAuthFunc authenticates requests arriving at node-served
	// HTTP listeners (submitIn auth "admin"), returning the
	// authenticated username. Nil means no credentials are
	// configured.
	HTTPAuthFunc() func(r *http.Request) (username string, ok bool)

	// QueueService returns the host's staging-queue service backing
	// the queue and checkpoint nodes. Nil means the host offers no
	// queue service; queue-family nodes then fail activation with an
	// explicit error.
	QueueService() QueueService
}

// QueueService is the host's staging-queue service — a structural
// capability NodeHost exposes so the queue and checkpoint executors
// can stage, hold and release messages without knowing the concrete
// queue implementation (durable table, directory backend, …).
type QueueService interface {
	// EnsureQueue returns the shared staging queue behind node,
	// creating it on first use. checkpoint marks the queue manual
	// (never drains on its own — items leave only via route releases)
	// and routes carries the checkpoint's named release choices in
	// port order.
	EnsureQueue(node nodestore.NodeInterface, checkpoint bool, routes []string) (QueueContract, error)

	// HasDurableStore reports whether a durable inbox store is wired.
	// The queue node's default "durable" backend requires it; dir
	// backends do not.
	HasDurableStore() bool
}

// QueueContract is the executor-facing surface of a host staging
// queue: the queue/checkpoint shells stage and drain through it; the
// inspection API (item listing, hold, per-item release) is a
// host-internal concern and intentionally not part of this contract.
type QueueContract interface {
	// Arm starts the drainer once and recovers surviving durable
	// rows. Idempotent — the deploy-time activation path calls it.
	Arm(ctx context.Context)

	// Enqueue stages a delivered payload under the queue's capacity
	// and overflow policy.
	Enqueue(p nodestore.MessageInterface) error

	// Stop shuts the drainer down (idempotent) and releases the
	// queue's resources.
	Stop()
}
