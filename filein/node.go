// Package filein contains the fileIn node implementation.
package filein

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(FileInConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewFileInNode(n, c, e), nil
}

// init registers the fileIn node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "fileIn",
			Name:        "File In",
			Description: "Reads a file into the payload, whole or one message per line",
			Icon:        "folder-open",
			Category:    "Storage",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the fileIn node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"filename": map[string]string{
			"type":        "string",
			"title":       "Filename",
			"description": "Source file path; with filenameType \"msg\" the message's filename property is used instead, with \"flow\"/\"global\" the value is read from that context scope",
		},
		"filenameType": map[string]interface{}{
			"type":    "string",
			"title":   "Filename Source",
			"enum":    []string{"fixed", "msg", "flow", "global"},
			"default": "fixed",
		},
		"format": map[string]interface{}{
			"type":    "string",
			"title":   "Format",
			"enum":    []string{"utf8", "lines", "stream", "binary"},
			"default": "utf8",
			"description": "utf8: one string message; lines: one message per line (parts metadata); " +
				"stream: one message per 64KiB chunk; binary: one []byte message",
		},
		"sendError": map[string]interface{}{
			"type":        "boolean",
			"title":       "Send Error",
			"description": "Emit an error message for a missing file instead of raising a catchable error",
			"default":     false,
		},
		"errorCatch": nodebase.ErrorCatchSchemaField(),
	},
}

// fileStreamChunkSize is the read chunk size for fileIn "stream" format
const fileStreamChunkSize = 64 * 1024

// fileGroupSeq generates unique group ids for fileIn parts metadata
var fileGroupSeq int64

// FileInConfig holds configuration for the file in (read) node — the
// Node-RED `file in` equivalent.
type FileInConfig struct {
	// Filename is the source path (fixed mode); with FilenameType "msg"
	// the message's filename property is used instead
	Filename string `json:"filename"`
	// FilenameType selects the filename source: "fixed" (default) | "msg"
	FilenameType string `json:"filenameType"`
	// Format selects the read mode: "utf8" (default, one string msg) |
	// "lines" (one msg per line with parts metadata) | "stream" (one msg
	// per 64KiB chunk) | "binary" (one []byte msg)
	Format string `json:"format"`
	// SendError emits an error message (Data {error: ...}) for a missing
	// file instead of raising a catchable error
	SendError bool `json:"sendError"`
}

// FileInNode reads a file and emits its content. The "lines" and
// "stream" formats use the MessageExpander hook — Execute stashes the
// chunks on the instance and Expand tags them with parts metadata so a
// downstream join node can rebuild the file.
type FileInNode struct {
	*nodebase.BaseNode
	config *FileInConfig
	host   nodebase.NodeHost
	// chunks holds the pending expansion output; executors are rebuilt
	// per message but Execute/Expand run on the same instance
	chunks []interface{}
	partCh string // delimiter recorded in parts.ch ("lines" only)
}

// Execute reads the resolved file. For single-message formats the content
// lands in Data directly; for "lines"/"stream" it is stashed for Expand.
func (n *FileInNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	filename, err := nodebase.FileFilename(n.config.Filename, n.config.FilenameType, input, n.host, n.GetWorkflowID())
	if err != nil {
		return nil, err
	}

	content, err := os.ReadFile(filename)
	if err != nil {
		if n.config.SendError {
			metadata := nodebase.CloneMetadata(input)
			metadata["filename"] = filename
			return nodestore.NewMessage().SetPayload(map[string]interface{}{"error": err.Error()}).SetMetadata(metadata).SetTimestamp(time.Now()), nil
		}
		return nil, fmt.Errorf("file in node read failed: %w", err)
	}

	metadata := nodebase.CloneMetadata(input)
	metadata["filename"] = filename

	n.chunks = nil
	n.partCh = ""
	switch n.config.Format {
	case "lines":
		// Split on \n keeping the trailing empty line a final newline
		// produces — Node-RED emits it as the last sequence part.
		text := string(content)
		if text != "" {
			for _, line := range strings.Split(text, "\n") {
				n.chunks = append(n.chunks, strings.TrimSuffix(line, "\r"))
			}
		}
		n.partCh = "\n"
		return nodestore.NewMessage().SetMetadata(metadata).SetTimestamp(time.Now()), nil
	case "stream":
		for off := 0; off < len(content); off += fileStreamChunkSize {
			end := off + fileStreamChunkSize
			if end > len(content) {
				end = len(content)
			}
			chunk := make([]byte, end-off)
			copy(chunk, content[off:end])
			n.chunks = append(n.chunks, chunk)
		}
		if len(content) == 0 {
			n.chunks = append(n.chunks, []byte{})
		}
		return nodestore.NewMessage().SetMetadata(metadata).SetTimestamp(time.Now()), nil
	case "binary":
		return nodestore.NewMessage().SetPayload(content).SetMetadata(metadata).SetTimestamp(time.Now()), nil
	default: // "utf8" / ""
		return nodestore.NewMessage().SetPayload(string(content)).SetMetadata(metadata).SetTimestamp(time.Now()), nil
	}
}

// Expand turns the stashed lines/stream chunks into one message per
// part, tagged with parts {id, type, index, count, ch} so a join node
// can reassemble the file.
func (n *FileInNode) Expand(output nodestore.MessageInterface) []nodestore.MessageInterface {
	// Single-message formats (utf8/binary) pass through unexpanded —
	// the host calls Expand for every MessageExpander output.
	if n.config.Format != "lines" && n.config.Format != "stream" {
		return []nodestore.MessageInterface{output}
	}
	if len(n.chunks) == 0 {
		return nil // empty file: nothing to emit
	}
	partType := "stream"
	if n.config.Format == "lines" {
		// Node-RED tags line streams as parts.type "string" with the
		// delimiter in parts.ch so a join reassembles them as text.
		partType = "string"
	}
	groupID := fmt.Sprintf("parts-%d", atomic.AddInt64(&fileGroupSeq, 1))
	count := len(n.chunks)

	out := make([]nodestore.MessageInterface, 0, count)
	for i, chunk := range n.chunks {
		p := nodebase.ClonePayload(output)
		if p.GetMetadata() == nil {
			p.SetMetadata(map[string]interface{}{})
		}
		p.SetPayload(chunk)
		parts := map[string]interface{}{
			"id":    groupID,
			"type":  partType,
			"index": i,
			"count": count,
		}
		if n.partCh != "" {
			parts["ch"] = n.partCh
		}
		if prev, ok := p.GetMetadata()["parts"]; ok {
			parts["parts"] = prev
		}
		p.GetMetadata()["parts"] = parts
		out = append(out, p)
	}
	return out
}

// Validate checks the file in node configuration
func (n *FileInNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	switch n.config.Format {
	case "", "utf8", "lines", "stream", "binary":
	default:
		return fmt.Errorf("invalid format %q", n.config.Format)
	}
	return nil
}

// NewFileInNode creates a new file in (read) node
func NewFileInNode(node nodestore.NodeInterface, config *FileInConfig, host nodebase.NodeHost) *FileInNode {
	return &FileInNode{
		BaseNode: nodebase.NewBaseNode(node),
		config:   config,
		host:     host,
	}
}

// Execute reads the resolved file. For single-message formats the content
// lands in Data directly; for "lines"/"stream" it is stashed for Expand.
