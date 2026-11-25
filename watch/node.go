// Package watch contains the watch node implementation.
package watch

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(WatchConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	// Node-RED's watch node stores targets in `files` — either a plain
	// path or a JSON array of paths; the node watches the first entry.
	if c.Filename == "" {
		if raw, ok := n.GetData()["files"].(string); ok && raw != "" {
			var arr []string
			if err := json.Unmarshal([]byte(raw), &arr); err == nil && len(arr) > 0 {
				c.Filename = arr[0]
			} else {
				c.Filename = raw
			}
		}
	}
	return NewWatchNode(n, c, e), nil
}

// init registers the watch node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "watch",
			Name:        "Watch",
			Description: "Emits a message for each file change under a watched path",
			Icon:        "eye",
			Category:    "Storage",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the watch node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"filename": map[string]string{
			"type":        "string",
			"title":       "Path",
			"description": "File or directory to watch",
		},
		"recursive": map[string]interface{}{
			"type":        "boolean",
			"title":       "Recursive",
			"description": "Watch subdirectories when Path is a directory",
			"default":     false,
		},
		"interval": map[string]interface{}{
			"type":        "number",
			"title":       "Poll Interval (seconds)",
			"description": "Polling period for change detection; 0 = 1 second",
			"default":     1,
		},
	},
	"required": []string{"filename"},
}

// WatchConfig holds configuration for the watch node — the Node-RED
// `watch` equivalent: a filesystem watcher emitting one message per
// changed path.
type WatchConfig struct {
	// Filename is the file or directory to watch. A directory without
	// Recursive watches its immediate entries only.
	Filename string `json:"filename"`
	// Recursive walks subdirectories when Filename is a directory.
	Recursive bool `json:"recursive"`
	// Interval is the poll period in seconds (default 1). A polling
	// implementation is used so the node is cross-platform without an
	// fsnotify dependency.
	Interval float64 `json:"interval"`
}

// watchEntry is one path's fingerprint in a snapshot.
type watchEntry struct {
	modTime time.Time
	size    int64
	isDir   bool
}

// WatchNode polls the filesystem for changes and emits a message per
// created/modified/deleted path (Stopper lifecycle like other triggers).
type WatchNode struct {
	*nodebase.BaseNode
	config     *WatchConfig
	host       nodebase.NodeHost
	workflowID string
	stopCh     chan struct{}
	stopOnce   sync.Once
}

// NewWatchNode creates a new watch node
func NewWatchNode(node nodestore.NodeInterface, config *WatchConfig, host nodebase.NodeHost) *WatchNode {
	return &WatchNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
		stopCh:     make(chan struct{}),
	}
}

// Execute resolves the watch path, takes the baseline snapshot and
// starts the polling goroutine.
func (n *WatchNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	var root string
	if n.host != nil {
		root = n.host.FileRootPath()
	}
	path, err := nodebase.ResolveFilePath(root, n.config.Filename)
	if err != nil {
		return nil, fmt.Errorf("watch: %w", err)
	}
	snap := n.snapshot(path)
	interval := n.config.Interval
	if interval <= 0 {
		interval = 1
	}
	go n.loop(ctx, path, snap, time.Duration(interval*float64(time.Second)))
	return nodestore.NewMessage().SetPayload(map[string]interface{}{"status": "watching", "filename": path}).SetTimestamp(time.Now()), nil
}

// loop polls the path until the node is stopped, emitting a message
// per added/changed/removed entry on each diff.
func (n *WatchNode) loop(ctx context.Context, path string, snap map[string]watchEntry, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			next := n.snapshot(path)
			for p, cur := range next {
				prev, ok := snap[p]
				switch {
				case !ok:
					n.emitChange(p, "added", cur)
				case !prev.modTime.Equal(cur.modTime) || prev.size != cur.size:
					n.emitChange(p, "changed", cur)
				}
			}
			for p := range snap {
				if _, ok := next[p]; !ok {
					n.emitChange(p, "removed", watchEntry{})
				}
			}
			snap = next
		case <-n.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

// emitChange pushes one change message downstream. Data carries the
// path; Metadata carries filename/event/kind/size (Node-RED emits the
// filename in the payload — kept here for parity).
func (n *WatchNode) emitChange(path, event string, e watchEntry) {
	kind := "file"
	if e.isDir {
		kind = "directory"
	}
	payload := nodestore.NewMessage().SetPayload(path).SetMetadata(map[string]interface{}{
		"filename": path,
		"event":    event,
		"type":     kind,
		"filesize": e.size,
	}).SetTimestamp(time.Now())
	_ = n.host.Emit(n.workflowID, n.GetID(), payload, nil)
}

// snapshot builds the path→entry map for the watched file or directory
// (recursive when configured).
func (n *WatchNode) snapshot(path string) map[string]watchEntry {
	out := map[string]watchEntry{}
	info, err := os.Stat(path)
	if err != nil {
		return out
	}
	if !info.IsDir() {
		out[path] = watchEntry{modTime: info.ModTime(), size: info.Size()}
		return out
	}
	var paths []string
	if n.config.Recursive {
		_ = filepath.Walk(path, func(p string, fi os.FileInfo, err error) error {
			if err == nil {
				paths = append(paths, p)
			}
			return nil
		})
	} else {
		entries, err := os.ReadDir(path)
		if err == nil {
			for _, e := range entries {
				paths = append(paths, filepath.Join(path, e.Name()))
			}
		}
	}
	sort.Strings(paths)
	for _, p := range paths {
		if fi, err := os.Stat(p); err == nil {
			out[p] = watchEntry{modTime: fi.ModTime(), size: fi.Size(), isDir: fi.IsDir()}
		}
	}
	return out
}

// Stop halts the polling goroutine (idempotent).
func (n *WatchNode) Stop() error {
	n.stopOnce.Do(func() { close(n.stopCh) })
	return nil
}

// Validate checks the watch node configuration
func (n *WatchNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	if n.config.Filename == "" {
		return fmt.Errorf("watch requires a filename to watch")
	}
	if n.config.Interval < 0 {
		return fmt.Errorf("watch interval cannot be negative")
	}
	return nil
}
