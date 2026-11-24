// Package queue contains the queue node implementation.
package queue

import (
	_ "embed"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(nodebase.QueueConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return nodebase.NewQueueNode(n, c, e), nil
}

// init registers the queue node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "queue",
			Name:        "Queue",
			Description: "Durable, inspectable staging queue with a hold/release gate",
			Icon:        "inbox",
			Category:    "Queue",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the queue node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"capacity": map[string]interface{}{
			"type":        "number",
			"title":       "Capacity",
			"description": "Maximum queued items (0 = unbounded); alias for inboxCapacity",
			"default":     0,
		},
		"policy": map[string]interface{}{
			"type":        "string",
			"title":       "Overflow Policy",
			"enum":        []string{"block", "drop-newest", "drop-oldest", "route-to-catch"},
			"default":     "block",
			"description": "What happens when the queue reaches capacity",
		},
		"hold": map[string]interface{}{
			"type":        "boolean",
			"title":       "Hold",
			"description": "Start held: deliveries enqueue but nothing drains until released",
			"default":     false,
		},
		"backend": map[string]interface{}{
			"type":    "string",
			"title":   "Storage Backend",
			"enum":    []string{"durable", "dir"},
			"default": "durable",
			"description": "durable: the configured durable inbox store (shared, multi-runner). " +
				"dir: one file per queued item in a directory, inspectable with OS tools",
		},
		"dir": map[string]interface{}{
			"type":  "string",
			"title": "Directory",
			"description": "Directory locator the dir backend writes one {seq}-{msgid}.json file per item into: " +
				"a local path (or NFS/SMB/SSHFS mount), or sftp://user@host/path or ftp://user@host/path. " +
				"Remote backends are POLLING (no fsnotify — the drainer scans on arm). " +
				"Multi-runner safety is a deployment property, the node makes no claim.",
			"showIf": map[string]interface{}{"backend": "dir"},
		},
		"password": map[string]interface{}{
			"type":        "string",
			"title":       "Password",
			"format":      "credential",
			"description": "Remote locator password, or the keyFile passphrase (credential: masked, never exported)",
			"showIf":      map[string]interface{}{"backend": "dir"},
		},
		"keyFile": map[string]interface{}{
			"type":        "string",
			"title":       "SSH Key File",
			"description": "sftp locators: local path to an SSH private key; password doubles as the passphrase",
			"showIf":      map[string]interface{}{"backend": "dir"},
		},
		"hostKey": map[string]interface{}{
			"type":        "string",
			"title":       "Host Key",
			"description": "sftp locators: expected host public key (authorized_keys or base64); empty accepts any key (insecure)",
			"showIf":      map[string]interface{}{"backend": "dir"},
		},
	},
}
