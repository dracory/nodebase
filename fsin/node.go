// Package fsin contains the fsIn node implementation.
package fsin

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(FsInConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewFsInNode(n, c, e), nil
}

// init registers the fsIn node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "fsIn",
			Name:        "FS In",
			Description: "Polls a local/sftp/ftp directory locator and emits one message per new file",
			Icon:        "folder-open",
			Category:    "Storage",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the fsIn node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"location": map[string]interface{}{
			"type":        "string",
			"title":       "Location",
			"description": "Directory locator to poll: a local path, sftp://user@host/path or ftp://user@host/path. Remote polling only — no fsnotify",
		},
		"interval": map[string]interface{}{
			"type":        "number",
			"title":       "Poll Interval (s)",
			"description": "Seconds between directory polls",
			"default":     5,
		},
		"format": map[string]interface{}{
			"type":    "string",
			"title":   "Format",
			"enum":    []string{"utf8", "binary", "path"},
			"default": "utf8",
			"description": "utf8: payload = file contents as string; binary: payload = []byte; " +
				"path: payload = the locator path only (pairs with a downstream reader)",
		},
		"pattern": map[string]interface{}{
			"type":        "string",
			"title":       "Pattern",
			"description": "Optional glob filter on file base names (e.g. \"*.xml\"); empty accepts every file",
		},
		"delete": map[string]interface{}{
			"type":        "boolean",
			"title":       "Delete After Emit",
			"description": "Remove each file after it is emitted (Switch receive-and-consume)",
			"default":     false,
		},
		"password": map[string]interface{}{
			"type":        "string",
			"title":       "Password",
			"format":      "credential",
			"description": "Remote locator password, or the keyFile passphrase (credential: masked, never exported)",
		},
		"keyFile": map[string]interface{}{
			"type":        "string",
			"title":       "SSH Key File",
			"description": "sftp locators: local path to an SSH private key",
		},
		"hostKey": map[string]interface{}{
			"type":        "string",
			"title":       "Host Key",
			"description": "sftp locators: expected host public key (authorized_keys or base64); required unless acceptAnyHostKey is set",
		},
		"acceptAnyHostKey": map[string]interface{}{
			"type":        "boolean",
			"title":       "Accept Any Host Key",
			"description": "sftp locators: skip host-key verification (INSECURE — vulnerable to MITM; prefer pinning hostKey)",
			"default":     false,
		},
	},
	"required": []string{"location"},
}

// FsInConfig holds the fsIn node configuration.
type FsInConfig struct {
	// Location is the directory to poll: a local path or an
	// sftp://user@host/path or ftp://… locator.
	Location string `json:"location"`
	// Interval is the poll period in seconds (default 5).
	Interval float64 `json:"interval"`
	// Format selects the emitted payload: "utf8" (default, file
	// contents as string) | "binary" ([]byte) | "path" (locator path
	// only — pairs with a downstream reader).
	Format string `json:"format"`
	// Delete removes each file after it is emitted (Switch's
	// "leave file" toggle, inverted: receive-and-consume).
	Delete bool `json:"delete"`
	// Pattern, when set, is a glob matched against file base names
	// (e.g. "*.xml"); empty accepts every file.
	Pattern string `json:"pattern"`
	// Credentials for remote locators (see FsCredentials). Password
	// is credential-typed.
	Password         string `json:"password"`
	KeyFile          string `json:"keyFile"`
	HostKey          string `json:"hostKey"`
	AcceptAnyHostKey bool   `json:"acceptAnyHostKey"`
}

// fsSeenEntry fingerprints one polled file so unchanged files are not
// re-emitted every interval.
type fsSeenEntry struct {
	size    int64
	modTime time.Time
	emitted bool
}

// FsInNode polls a directory locator and emits one message per new
// file (Stopper lifecycle like watch/inject).
type FsInNode struct {
	*nodebase.BaseNode
	config     *FsInConfig
	host       nodebase.NodeHost
	workflowID string
	mu         sync.Mutex
	fs         nodebase.Fs
	dir        string
	seen       map[string]fsSeenEntry
	started    bool
	stopCh     chan struct{}
	stopOnce   sync.Once
}

// Execute opens the locator and starts the polling goroutine. The
// returned payload is a status message (like watch); file messages
// flow through host.emit as polls find them.
func (n *FsInNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	fsys, dir, err := nodebase.OpenFs(n.config.Location, n.credentials())
	if err != nil {
		return nil, fmt.Errorf("fsIn: %w", err)
	}
	// Local locators honour the FileRoot sandbox like file/fileIn;
	// remote locators are inherently outside it.
	if _, local := fsys.(nodebase.LocalFs); local && n.host != nil && n.host.FileRootPath() != "" {
		resolved, err := nodebase.ResolveFilePath(n.host.FileRootPath(), dir)
		if err != nil {
			_ = fsys.Close()
			return nil, fmt.Errorf("fsIn: %w", err)
		}
		dir = resolved
	}
	interval := n.config.Interval
	if interval <= 0 {
		interval = 5
	}
	n.mu.Lock()
	if n.started {
		n.mu.Unlock()
		_ = fsys.Close()
		return nodestore.NewMessage().SetPayload(map[string]interface{}{"status": "watching", "location": n.config.Location}).SetTimestamp(time.Now()), nil
	}
	n.fs, n.dir, n.started = fsys, dir, true
	n.mu.Unlock()

	go n.loop(ctx, time.Duration(interval*float64(time.Second)))
	return nodestore.NewMessage().SetPayload(map[string]interface{}{"status": "watching", "location": n.config.Location}).SetTimestamp(time.Now()), nil
}

func (n *FsInNode) credentials() *nodebase.FsCredentials {
	return &nodebase.FsCredentials{
		Password:         n.config.Password,
		KeyFile:          n.config.KeyFile,
		HostKey:          n.config.HostKey,
		AcceptAnyHostKey: n.config.AcceptAnyHostKey,
	}
}

// loop polls the dir until the node is stopped.
func (n *FsInNode) loop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	n.poll() // baseline pass: existing files ARE staged content, emit them
	for {
		select {
		case <-ticker.C:
			n.poll()
		case <-n.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

// poll lists the dir once and emits every unseen/changed file.
func (n *FsInNode) poll() {
	n.mu.Lock()
	fsys, dir := n.fs, n.dir
	n.mu.Unlock()
	if fsys == nil {
		return
	}
	entries, err := fsys.List(dir)
	if err != nil {
		n.host.EmitEvent(n.workflowID, n.GetID(), "fsIn", "warn",
			fmt.Sprintf("fsIn poll of %q failed: %v", n.config.Location, err))
		return
	}
	for _, e := range entries {
		if e.IsDir || !n.matches(e.Name) {
			continue
		}
		n.mu.Lock()
		prev, seen := n.seen[e.Name]
		fresh := !seen || prev.size != e.Size || !prev.modTime.Equal(e.ModTime)
		if fresh {
			n.seen[e.Name] = fsSeenEntry{size: e.Size, modTime: e.ModTime}
		}
		n.mu.Unlock()
		if !fresh {
			continue
		}
		n.emitFile(fsys.Join(dir, e.Name), e)
	}
}

// matches applies the optional glob filter to a base name.
func (n *FsInNode) matches(name string) bool {
	pat := strings.TrimSpace(n.config.Pattern)
	if pat == "" {
		return true
	}
	ok, err := path.Match(pat, name)
	if err != nil {
		return true // invalid glob behaves as no filter
	}
	return ok
}

// emitFile reads one file and pushes it downstream.
func (n *FsInNode) emitFile(fullPath string, e nodebase.FsEntry) {
	n.mu.Lock()
	fsys := n.fs
	n.mu.Unlock()
	if fsys == nil {
		// Stop() may close and nil the fs while a poll is in flight —
		// bail instead of dereferencing the closed handle.
		return
	}
	payload := nodestore.NewMessage().SetMetadata(map[string]interface{}{
		"filename": fullPath,
		"filesize": e.Size,
		"location": n.config.Location,
	}).SetTimestamp(time.Now())
	switch n.config.Format {
	case "path":
		payload.SetPayload(fullPath)
	default:
		raw, err := fsys.ReadFile(fullPath)
		if err != nil {
			n.host.EmitEvent(n.workflowID, n.GetID(), "fsIn", "warn",
				fmt.Sprintf("fsIn read of %q failed: %v", fullPath, err))
			// Leave it un-emitted so a transient read error retries
			// on the next poll (seen was marked pre-read; clear it).
			n.mu.Lock()
			delete(n.seen, e.Name)
			n.mu.Unlock()
			return
		}
		if n.config.Format == "binary" {
			payload.SetPayload(raw)
		} else {
			payload.SetPayload(string(raw))
		}
	}
	if err := n.host.Emit(n.workflowID, n.GetID(), payload, nil); err != nil {
		n.host.EmitEvent(n.workflowID, n.GetID(), "fsIn", "warn",
			fmt.Sprintf("fsIn emit of %q failed: %v", fullPath, err))
		return
	}
	if n.config.Delete {
		if err := fsys.Remove(fullPath); err != nil {
			n.host.EmitEvent(n.workflowID, n.GetID(), "fsIn", "warn",
				fmt.Sprintf("fsIn delete of %q failed: %v", fullPath, err))
		}
	}
}

// Stop shuts the poller and the remote connection down.
func (n *FsInNode) Stop() error {
	n.stopOnce.Do(func() { close(n.stopCh) })
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.fs != nil {
		_ = n.fs.Close()
		n.fs = nil
	}
	return nil
}

// Validate checks the fsIn configuration.
func (n *FsInNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(n.config.Location) == "" {
		return fmt.Errorf("fsIn requires a location")
	}
	// A userinfo password in the locator URI lives in the flow
	// document as plain config — outside the credential-sentinel
	// model — and would leak into exports. Reject it in flows;
	// OpenFs keeps accepting it for direct tooling use.
	if u, err := url.Parse(n.config.Location); err == nil && u.User != nil {
		if _, hasPw := u.User.Password(); hasPw {
			return fmt.Errorf("fsIn: password in the locator URI is not allowed — use the credential \"password\" field")
		}
	}
	switch n.config.Format {
	case "", "utf8", "binary", "path":
	default:
		return fmt.Errorf("invalid format %q", n.config.Format)
	}
	return nil
}

// NewFsInNode creates a new fsIn node.
func NewFsInNode(node nodestore.NodeInterface, config *FsInConfig, host nodebase.NodeHost) *FsInNode {
	return &FsInNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
		seen:       map[string]fsSeenEntry{},
		stopCh:     make(chan struct{}),
	}
}

// Execute opens the locator and starts the polling goroutine. The
// returned payload is a status message (like watch); file messages
// flow through host.emit as polls find them.
