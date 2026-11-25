package nodebase

import (
	"fmt"
	"time"

	nodestore "github.com/dracory/nodestore"
)

// linkCallStackKey is the Metadata key holding the link call stack.
const LinkCallStackKey = "linkCallStack"

// defaultLinkCallTimeout bounds the wait for a linkOut return when the
// node configures no explicit timeout.
const DefaultLinkCallTimeout = 30 * time.Second

// DefaultLinkCallFunctionTimeout is Node-RED's function-node
// node.linkcall default timeout (5s — shorter than the node's own
// 30s default).
const DefaultLinkCallFunctionTimeout = 5 * time.Second

// ResolveLinkInTarget resolves a link target to a linkIn node id,
// implementing Node-RED's linkcallTargets.getTargetNode: a direct node
// id wins (static choice); otherwise, when dynamic is true, the target
// is treated as a linkIn NAME — unique within the caller's flow first,
// then globally across non-subflow linkIns; zero or many matches fail.
//
// Since this engine folds every Node-RED tab into one workflow, the
// flow-scoped and global lookups are the same scan.
func ResolveLinkInTarget(host NodeHost, workflowID, target string, dynamic bool) (string, error) {
	if node, ok := host.LookupNode(workflowID, target); ok {
		if node.GetType() != "linkIn" {
			return "", fmt.Errorf("target link-in node '%s' not found", target)
		}
		return target, nil
	}
	if !dynamic || target == "" {
		return "", fmt.Errorf("target link-in node '%s' not found", target)
	}
	workflow, ok := host.RunningFlow(workflowID)
	if !ok || workflow == nil {
		return "", fmt.Errorf("target link-in node '%s' not found", target)
	}
	var matches []string
	for _, node := range workflow.GetNodes() {
		if node.GetType() == "linkIn" && NodeName(node) == target {
			matches = append(matches, node.GetID())
		}
	}
	switch len(matches) {
	case 0:
		return "", fmt.Errorf("target link-in node '%s' not found", target)
	case 1:
		return matches[0], nil
	default:
		return "", fmt.Errorf("Multiple link-in nodes named '%s' found", target)
	}
}

// LinkCallInto injects msg into the resolved linkIn target and awaits
// the matching linkOut return — the shared request/response machinery
// behind the linkCall node and the function node's node.linkcall.
// The message's link call stack gets the caller's entry appended
// before injection (mirroring Node-RED's msg._linkSource push).
func LinkCallInto(host NodeHost, workflowID, callerID, target string, msg nodestore.MessageInterface, timeout time.Duration) (nodestore.MessageInterface, error) {
	if msg.GetMetadata() == nil {
		msg.SetMetadata(map[string]interface{}{})
	}
	// The entry's msgId is intentionally left empty — writing it here
	// (before InjectMessage returns) would race the injected traversal,
	// which clones the metadata on a separate goroutine. linkOut's
	// return path falls back to the injected message's stamped "msgId"
	// metadata key, which is the same pending correlation id.
	stack, _ := msg.GetMetadata()[LinkCallStackKey].([]interface{})
	entry := map[string]interface{}{"nodeId": callerID}
	msg.GetMetadata()[LinkCallStackKey] = append(stack, entry)

	msgID, ch, err := host.InjectMessage(workflowID, target, msg)
	if err != nil {
		return nil, fmt.Errorf("linkCall: cannot reach link %q: %w", target, err)
	}
	if timeout <= 0 {
		timeout = DefaultLinkCallTimeout
	}
	select {
	case response := <-ch:
		return response, nil
	case <-time.After(timeout):
		host.DiscardPending(msgID)
		// Node-RED reports link-call timeouts via node.error("timeout").
		return nil, fmt.Errorf("timeout")
	}
}
