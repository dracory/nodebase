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
	"strings"
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
			"description": "File or directory to watch; a comma-separated list watches several paths and each entry may be a glob (e.g. \"/log/*.txt\")",
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
	// Filename is the file or directory to watch — a comma-separated
	// list watches several paths and each entry may be a glob pattern
	// (e.g. "/log/*.txt"). A directory without Recursive watches its
	// immediate entries only.
	Filename string `json:"filename"`
	// Files carries Node-RED's serialized watch target: a
	// comma-separated path list or, from older imports, a JSON array
	// of paths. Entries combine with Filename's.
	Files string `json:"files"`
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

// pathSpecs returns the distinct path specs to watch: Filename split
// on commas plus any Node-RED `files` entries (a comma-separated list
// or a legacy JSON array). Each spec may carry glob metacharacters —
// they are expanded on every poll so files created later still match.
func (n *WatchNode) pathSpecs() []string {
	seen := map[string]bool{}
	var specs []string
	add := func(s string) {
		s = strings.TrimSpace(s)
		if s != "" && !seen[s] {
			seen[s] = true
			specs = append(specs, s)
		}
	}
	for _, p := range strings.Split(n.config.Filename, ",") {
		add(p)
	}
	if raw := strings.TrimSpace(n.config.Files); raw != "" {
		var arr []string
		if err := json.Unmarshal([]byte(raw), &arr); err == nil {
			for _, p := range arr {
				add(p)
			}
		} else {
			for _, p := range strings.Split(raw, ",") {
				add(p)
			}
		}
	}
	return specs
}

// Execute resolves the watch specs, takes the baseline snapshot and
// starts the polling goroutine.
func (n *WatchNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	specs := n.pathSpecs()
	if len(specs) == 0 {
		return nil, fmt.Errorf("watch requires a filename to watch")
	}
	var root string
	if n.host != nil {
		root = n.host.FileRootPath()
	}
	resolved := make([]string, 0, len(specs))
	for _, s := range specs {
		p, err := nodebase.ResolveFilePath(root, s)
		if err != nil {
			return nil, fmt.Errorf("watch: %w", err)
		}
		resolved = append(resolved, p)
	}
	snap := n.snapshotSpecs(resolved)
	interval := n.config.Interval
	if interval <= 0 {
		interval = 1
	}
	go n.loop(ctx, resolved, snap, time.Duration(interval*float64(time.Second)))
	return nodestore.NewMessage().SetPayload(map[string]interface{}{"status": "watching", "filename": resolved[0], "paths": resolved}).SetTimestamp(time.Now()), nil
}

// loop polls the specs until the node is stopped, emitting a message
// per added/changed/removed entry on each diff.
func (n *WatchNode) loop(ctx context.Context, specs []string, snap map[string]watchEntry, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			next := n.snapshotSpecs(specs)
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

// snapshotSpecs merges the path→entry maps of every spec, expanding
// glob specs on each call.
func (n *WatchNode) snapshotSpecs(specs []string) map[string]watchEntry {
	out := map[string]watchEntry{}
	for _, spec := range specs {
		matches := []string{spec}
		if strings.ContainsAny(spec, "*?[") {
			if g, err := filepath.Glob(spec); err == nil && len(g) > 0 {
				matches = g
			}
		}
		for _, p := range matches {
			for path, e := range n.snapshot(p) {
				out[path] = e
			}
		}
	}
	return out
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
	if len(n.pathSpecs()) == 0 {
		return fmt.Errorf("watch requires a filename to watch")
	}
	if n.config.Interval < 0 {
		return fmt.Errorf("watch interval cannot be negative")
	}
	return nil
}
