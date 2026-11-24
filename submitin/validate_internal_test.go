package submitin

// Internal coverage ported from flowrunner's node_submit_test.go —
// asserts Validate normalizes defaults (path, upload cap).

import (
	nodestore "github.com/dracory/nodestore"
	"testing"
)

func TestSubmitValidateDefaults(t *testing.T) {
	n := NewSubmitNode(nodestore.NewNode("s", "submitIn"), &SubmitConfig{}, nil)
	if err := n.Validate(); err != nil {
		t.Fatalf("empty config must normalize, got %v", err)
	}
	if n.config.Path != "/submit" {
		t.Fatalf("default path = %q", n.config.Path)
	}
	if n.maxSizeBytes() != defaultMaxSubmitBytes {
		t.Fatalf("default maxSize = %d", n.maxSizeBytes())
	}
}
