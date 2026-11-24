// Package tail contains the tail node implementation.
package tail

import (
	"context"
	_ "embed"
	"fmt"
	"io"
	"os"
	nodestore "github.com/dracory/nodestore"
	"github.com/dracory/nodebase"
	"strings"
	"sync"
	"time"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(TailConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewTailNode(n, c, e), nil
}

// init registers the tail node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "tail",
			Name:        "Tail",
			Description: "Follows a file and emits newly appended lines",
			Icon:        "file-alt",
			Category:    "Storage",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the tail node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"filename": map[string]string{
			"type":        "string",
			"title":       "Filename",
			"description": "File to follow for appended content",
		},
		"filetype": map[string]interface{}{
			"type":    "string",
			"title":   "File Type",
			"enum":    []string{"text", "binary"},
			"default": "text",
			"description": "text: one message per appended line; " +
				"binary: one []byte message per appended chunk",
		},
		"split": map[string]interface{}{
			"type":        "string",
			"title":       "Separator",
			"description": "Record separator for text mode (default \\n; escapes allowed)",
			"default":     "\\n",
		},
		"interval": map[string]interface{}{
			"type":        "number",
			"title":       "Poll Interval (seconds)",
			"description": "Polling period for new content; 0 = 1 second",
			"default":     1,
		},
		"fromStart": map[string]interface{}{
			"type":        "boolean",
			"title":       "Read From Start",
			"description": "Emit the existing file content first instead of starting at the end",
			"default":     false,
		},
	},
	"required": []string{"filename"},
}

// TailConfig holds configuration for the tail node — the Node-RED
// `tail` equivalent: it follows a growing file and emits newly
// appended content.
type TailConfig struct {
	// Filename is the file to follow.
	Filename string `json:"filename"`
	// Filetype selects the emit mode: "text" (default — one message
	// per appended line, Node-RED semantics) or "binary" (one []byte
	// message per appended chunk).
	Filetype string `json:"filetype"`
	// Split is the record separator for text mode (default "\n";
	// escape sequences like \r\n allowed).
	Split string `json:"split"`
	// Interval is the poll period in seconds (default 1).
	Interval float64 `json:"interval"`
	// FromStart reads the existing file content first instead of
	// starting at the end (Node-RED `tail -f` starts at EOF).
	FromStart bool `json:"fromStart"`
}

// TailNode follows a file's appended content via polling and emits
// each new line (or binary chunk) downstream. Polling keeps the node
// cross-platform without an fsnotify dependency.
type TailNode struct {
	*nodebase.BaseNode
	config     *TailConfig
	host       nodebase.NodeHost
	workflowID string
	stopCh     chan struct{}
	stopOnce   sync.Once
}

// NewTailNode creates a new tail node
func NewTailNode(node nodestore.NodeInterface, config *TailConfig, host nodebase.NodeHost) *TailNode {
	return &TailNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
		stopCh:     make(chan struct{}),
	}
}

// tailState is the live follow position, carried by the polling
// goroutine only.
type tailState struct {
	offset int64
	// pending holds a partial line awaiting its separator.
	pending []byte
}

// Execute resolves the file, opens it and starts the polling goroutine
// from the end (or the start with FromStart).
func (n *TailNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	var root string
	if n.host != nil {
		root = n.host.FileRootPath()
	}
	path, err := nodebase.ResolveFilePath(root, n.config.Filename)
	if err != nil {
		return nil, fmt.Errorf("tail: %w", err)
	}
	st := &tailState{}
	if info, err := os.Stat(path); err == nil && !n.config.FromStart {
		st.offset = info.Size()
	}
	interval := n.config.Interval
	if interval <= 0 {
		interval = 1
	}
	go n.loop(ctx, path, st, time.Duration(interval*float64(time.Second)))
	return nodestore.NewMessage().SetPayload(map[string]interface{}{"status": "tailing", "filename": path}).SetTimestamp(time.Now()), nil
}

// loop polls the file for growth and emits appended content until the
// node is stopped. A truncated/rotated file restarts from offset 0
// (same behaviour as `tail -F` reopening).
func (n *TailNode) loop(ctx context.Context, path string, st *tailState, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			n.drain(path, st)
		case <-n.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

// drain reads any bytes appended since the last poll and emits them
// per the configured filetype.
func (n *TailNode) drain(path string, st *tailState) {
	info, err := os.Stat(path)
	if err != nil {
		return // file may not exist yet; retry next tick
	}
	if info.Size() < st.offset {
		st.offset = 0 // truncated/rotated: follow from the top
	}
	if info.Size() == st.offset {
		return
	}
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	if _, err := f.Seek(st.offset, io.SeekStart); err != nil {
		return
	}
	buf := make([]byte, info.Size()-st.offset)
	read, err := f.Read(buf)
	if read == 0 {
		return
	}
	st.offset += int64(read)
	data := buf[:read]

	if n.config.Filetype == "binary" {
		n.emitData(data, path)
		return
	}
	sep := nodebase.UnescapeTCP(n.config.Split)
	if sep == "" {
		sep = "\n"
	}
	joined := append(st.pending, data...)
	parts := strings.Split(string(joined), sep)
	st.pending = []byte(parts[len(parts)-1])
	for _, line := range parts[:len(parts)-1] {
		n.emitData(strings.TrimSuffix(line, "\r"), path)
	}
}

// emitData pushes one appended record downstream (line string or
// []byte chunk) with the filename in metadata.
func (n *TailNode) emitData(data interface{}, path string) {
	payload := nodestore.NewMessage().SetPayload(data).SetMetadata(map[string]interface{}{
		"filename": path,
	}).SetTimestamp(time.Now())
	_ = n.host.Emit(n.workflowID, n.GetID(), payload, nil)
}

// Stop halts the polling goroutine (idempotent).
func (n *TailNode) Stop() error {
	n.stopOnce.Do(func() { close(n.stopCh) })
	return nil
}

// Validate checks the tail node configuration
func (n *TailNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	if n.config.Filename == "" {
		return fmt.Errorf("tail requires a filename")
	}
	switch n.config.Filetype {
	case "", "text", "binary":
	default:
		return fmt.Errorf("invalid tail filetype %q (text|binary)", n.config.Filetype)
	}
	if n.config.Interval < 0 {
		return fmt.Errorf("tail interval cannot be negative")
	}
	return nil
}
