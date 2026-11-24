// Package fsout contains the fsOut node implementation.
package fsout

import (
	"context"
	_ "embed"
	"fmt"
	nodestore "github.com/dracory/nodestore"
	"github.com/dracory/nodebase"
	"strings"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(FsOutConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewFsOutNode(n, c, e), nil
}

// init registers the fsOut node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "fsOut",
			Name:        "FS Out",
			Description: "Writes the payload to a local/sftp/ftp directory locator (Switch-style send)",
			Icon:        "save",
			Category:    "Storage",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the fsOut node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"location": map[string]interface{}{
			"type":        "string",
			"title":       "Location",
			"description": "Target directory locator: a local path, sftp://user@host/path or ftp://user@host/path. The file is written to location/filename",
		},
		"filename": map[string]interface{}{
			"type":        "string",
			"title":       "Filename",
			"description": "Base name to write; with filenameType \"msg\" the message's filename property is used (reduced to its base name)",
		},
		"filenameType": map[string]interface{}{
			"type":    "string",
			"title":   "Filename Source",
			"enum":    []string{"fixed", "msg"},
			"default": "fixed",
		},
		"appendNewline": map[string]interface{}{
			"type":        "boolean",
			"title":       "Append Newline",
			"description": "Append \\n to every written payload",
			"default":     false,
		},
		"overwriteFile": map[string]interface{}{
			"type":    "string",
			"title":   "If File Exists",
			"enum":    []string{"true", "false", "delete"},
			"default": "true",
			"description": "true: overwrite via tmp+rename (atomic on local/sftp); " +
				"false: append; delete: remove first then write fresh",
		},
		"createDir": map[string]interface{}{
			"type":        "boolean",
			"title":       "Create Directory",
			"description": "Create missing locator directories (mkdir -p)",
			"default":     false,
		},
		"encoding": map[string]interface{}{
			"type":        "string",
			"title":       "Encoding",
			"enum":        []string{"utf8", "base64", "hex"},
			"default":     "utf8",
			"description": "base64/hex decode a string payload before writing",
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
			"description": "sftp locators: expected host public key (authorized_keys or base64); empty accepts any key (insecure)",
		},
	},
	"required": []string{"location"},
}

// FsOutConfig holds the fsOut node configuration — the remote-capable
// sibling of FileConfig.
type FsOutConfig struct {
	// Location is the target DIRECTORY locator: a local path or
	// sftp:// / ftp:// URI. The written file lives at
	// location/filename.
	Location string `json:"location"`
	// Filename is the base name to write (fixed mode); with
	// FilenameType "msg" the message's filename property is used —
	// a full msg filename is reduced to its base name so a remote
	// path cannot escape the locator dir.
	Filename     string `json:"filename"`
	FilenameType string `json:"filenameType"`
	// AppendNewline appends \n to every written payload.
	AppendNewline bool `json:"appendNewline"`
	// OverwriteFile: "true" (default) truncates, "false" appends,
	// "delete" removes the file first then writes fresh.
	OverwriteFile string `json:"overwriteFile"`
	// CreateDir creates missing locator directories (mkdir -p).
	CreateDir bool `json:"createDir"`
	// Encoding decodes a string payload before writing (same values
	// as the file node).
	Encoding string `json:"encoding"`
	// Credentials for remote locators; Password is credential-typed.
	Password string `json:"password"`
	KeyFile  string `json:"keyFile"`
	HostKey  string `json:"hostKey"`
}

// FsOutNode writes the payload to a file under a dir locator, then
// passes the message through with the resolved path in Metadata.
// The Fs is opened per message and closed after the write — remote
// delivery is a per-item dial (Switch's send model); volume flows
// should prefer the file node on a mount.
type FsOutNode struct {
	*nodebase.BaseNode
	config *FsOutConfig
	host   nodebase.NodeHost
}

// Execute resolves the target, writes via tmp+rename, and stamps the
// resolved locator path into Metadata["filename"].
func (n *FsOutNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	name := n.config.Filename
	if n.config.FilenameType == "msg" {
		v, ok := nodebase.LookupProperty(input, "filename")
		if !ok {
			return nil, fmt.Errorf("fsOut: filenameType is msg but the message has no filename property")
		}
		name = fmt.Sprintf("%v", v)
	}
	name = baseName(name)
	if name == "" {
		return nil, fmt.Errorf("fsOut: filename is empty")
	}

	fsys, dir, err := nodebase.OpenFs(n.config.Location, &nodebase.FsCredentials{
		Password: n.config.Password,
		KeyFile:  n.config.KeyFile,
		HostKey:  n.config.HostKey,
	})
	if err != nil {
		return nil, fmt.Errorf("fsOut: %w", err)
	}
	defer fsys.Close()
	// Local locators honour the FileRoot sandbox like file/fileIn;
	// remote locators are inherently outside it.
	if _, local := fsys.(nodebase.LocalFs); local && n.host != nil && n.host.FileRootPath() != "" {
		resolved, err := nodebase.ResolveFilePath(n.host.FileRootPath(), dir)
		if err != nil {
			return nil, fmt.Errorf("fsOut: %w", err)
		}
		dir = resolved
	}

	if n.config.CreateDir {
		if err := fsys.MkdirAll(dir); err != nil {
			return nil, fmt.Errorf("fsOut mkdir failed: %w", err)
		}
	}

	content, err := nodebase.EncodeFilePayload(input.GetPayload(), n.config.Encoding)
	if err != nil {
		return nil, err
	}
	if n.config.AppendNewline {
		content = append(content, '\n')
	}

	final := fsys.Join(dir, name)
	if n.config.OverwriteFile == "false" {
		if err := fsys.AppendFile(final, content); err != nil {
			return nil, fmt.Errorf("fsOut append failed: %w", err)
		}
	} else {
		if n.config.OverwriteFile == "delete" {
			if err := fsys.Remove(final); err != nil {
				return nil, fmt.Errorf("fsOut delete failed: %w", err)
			}
		}
		// Atomic-ish publish: write a tmp sibling then rename so a
		// consumer never sees a truncated file. FTP rename is not
		// atomic (documented) but never yields a partial final file.
		tmp := final + ".tmp"
		if err := fsys.WriteFile(tmp, content); err != nil {
			return nil, fmt.Errorf("fsOut write failed: %w", err)
		}
		if err := fsys.Rename(tmp, final); err != nil {
			_ = fsys.Remove(tmp)
			return nil, fmt.Errorf("fsOut commit failed: %w", err)
		}
	}

	metadata := nodebase.CloneMetadata(input)
	metadata["filename"] = final
	return nodestore.NewMessage().SetPayload(input.GetPayload()).SetMetadata(metadata).SetTimestamp(input.GetTimestamp()), nil
}

// baseName reduces a message-supplied filename to its last path
// segment so msg filenames cannot escape the locator directory.
func baseName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	name = strings.ReplaceAll(name, "\\", "/")
	if i := strings.LastIndex(name, "/"); i >= 0 {
		name = name[i+1:]
	}
	return name
}

// Validate checks the fsOut configuration.
func (n *FsOutNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(n.config.Location) == "" {
		return fmt.Errorf("fsOut requires a location")
	}
	switch n.config.OverwriteFile {
	case "", "true", "false", "delete":
	default:
		return fmt.Errorf("invalid overwriteFile %q", n.config.OverwriteFile)
	}
	switch n.config.Encoding {
	case "", "utf8", "none", "base64", "hex":
	default:
		return fmt.Errorf("invalid encoding %q", n.config.Encoding)
	}
	return nil
}

// NewFsOutNode creates a new fsOut node.
func NewFsOutNode(node nodestore.NodeInterface, config *FsOutConfig, host nodebase.NodeHost) *FsOutNode {
	return &FsOutNode{
		BaseNode: nodebase.NewBaseNode(node),
		config:   config,
		host:     host,
	}
}

// Execute resolves the target, writes via tmp+rename, and stamps the
// resolved locator path into Metadata["filename"].
