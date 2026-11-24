// Package checkpoint contains the checkpoint node implementation.
package checkpoint

import (
	_ "embed"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(nodebase.CheckpointConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return nodebase.NewCheckpointNode(n, c, e), nil
}

// init registers the checkpoint node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "checkpoint",
			Name:        "Checkpoint",
			Description: "Holds each message until a human releases it via a named route",
			Icon:        "user-check",
			Category:    "Queue",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the checkpoint node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"routes": map[string]interface{}{
			"type":  "array",
			"title": "Routes",
			"description": "Ordered list of route labels; routes[i] labels output_{i+1}. " +
				"Each route becomes a button in the inspection panel — releasing an item " +
				"via a route delivers it only on that edge",
			"items":   map[string]interface{}{"type": "string"},
			"default": []string{"approve", "decline"},
		},
		"capacity": map[string]interface{}{
			"type":        "number",
			"title":       "Capacity",
			"description": "Maximum held items (0 = unbounded); alias for inboxCapacity",
			"default":     0,
		},
		"policy": map[string]interface{}{
			"type":        "string",
			"title":       "Overflow Policy",
			"enum":        []string{"block", "drop-newest", "drop-oldest", "route-to-catch"},
			"default":     "block",
			"description": "What happens when the checkpoint reaches capacity",
		},
		"backend": map[string]interface{}{
			"type":    "string",
			"title":   "Storage Backend",
			"enum":    []string{"durable", "dir"},
			"default": "durable",
			"description": "durable: the configured durable inbox store (shared, multi-runner). " +
				"dir: one file per held item in a directory, inspectable with OS tools",
		},
		"dir": map[string]interface{}{
			"type":  "string",
			"title": "Directory",
			"description": "Directory the dir backend writes one {seq}-{msgid}.json file per item into. " +
				"May be a local path or an NFS/SMB/SSHFS mount; multi-runner safety is a deployment property, " +
				"the node makes no claim.",
			"showIf": map[string]interface{}{"backend": "dir"},
		},
	},
}
