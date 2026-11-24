// Package file contains the file node implementation.
package file

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"os"
	"path/filepath"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(FileConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewFileNode(n, c, e), nil
}

// init registers the file node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "file",
			Name:        "File",
			Description: "Writes the payload to a file (optionally restricted by GOFLOW_FILE_ROOT)",
			Icon:        "save",
			Category:    "Storage",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the file node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"filename": map[string]string{
			"type":        "string",
			"title":       "Filename",
			"description": "Target file path; with filenameType \"msg\" the message's filename property is used instead",
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
			"description": "true: overwrite (truncate); false: append to the file; " +
				"delete: remove the file first, then write fresh",
		},
		"createDir": map[string]interface{}{
			"type":        "boolean",
			"title":       "Create Directory",
			"description": "Create missing parent directories (mkdir -p)",
			"default":     false,
		},
		"encoding": map[string]interface{}{
			"type":        "string",
			"title":       "Encoding",
			"enum":        []string{"utf8", "base64", "hex"},
			"default":     "utf8",
			"description": "base64/hex decode a string payload before writing",
		},
		"errorCatch": nodebase.ErrorCatchSchemaField(),
	},
}

// FileConfig holds configuration for the file (write) node — the
// Node-RED `file` equivalent.
type FileConfig struct {
	// Filename is the target path (fixed mode); with FilenameType "msg"
	// the message's filename property is used instead
	Filename string `json:"filename"`
	// FilenameType selects the filename source: "fixed" (default) | "msg"
	FilenameType string `json:"filenameType"`
	// AppendNewline appends \n to every written payload
	AppendNewline bool `json:"appendNewline"`
	// OverwriteFile: "true" (default) truncates, "false" appends,
	// "delete" removes the file first then writes fresh
	OverwriteFile string `json:"overwriteFile"`
	// CreateDir creates missing parent directories (mkdir -p)
	CreateDir bool `json:"createDir"`
	// Encoding decodes a string payload before writing:
	// "utf8" (default) | "base64" | "hex"
	Encoding string `json:"encoding"`
}

// FileNode writes the message payload to a file, then passes the
// message through with the resolved filename in Metadata.
type FileNode struct {
	*nodebase.BaseNode
	config *FileConfig
	host   nodebase.NodeHost
}

// Execute writes the payload to the resolved filename and returns the
// input message with Metadata["filename"] set.
func (n *FileNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	filename, err := nodebase.FileFilename(n.config.Filename, n.config.FilenameType, input, n.host)
	if err != nil {
		return nil, err
	}

	content, err := n.encode(input)
	if err != nil {
		return nil, err
	}
	if n.config.AppendNewline {
		content = append(content, '\n')
	}

	if n.config.CreateDir {
		if dir := filepath.Dir(filename); dir != "" {
			if err := os.MkdirAll(dir, 0755); err != nil {
				return nil, fmt.Errorf("file node mkdir failed: %w", err)
			}
		}
	}

	if n.config.OverwriteFile == "delete" {
		// Remove the file first; a missing file is not an error
		if err := os.Remove(filename); err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("file node delete failed: %w", err)
		}
	}

	flags := os.O_WRONLY | os.O_CREATE
	if n.config.OverwriteFile == "false" {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC // "true" (default) and "delete" write fresh
	}

	f, err := os.OpenFile(filename, flags, 0644)
	if err != nil {
		return nil, fmt.Errorf("file node open failed: %w", err)
	}
	defer f.Close()
	if _, err := f.Write(content); err != nil {
		return nil, fmt.Errorf("file node write failed: %w", err)
	}

	metadata := nodebase.CloneMetadata(input)
	metadata["filename"] = filename
	return nodestore.NewMessage().SetPayload(input.GetPayload()).SetMetadata(metadata).SetTimestamp(input.GetTimestamp()), nil
}

// encode converts the message Data into bytes per the configured
// encoding.
func (n *FileNode) encode(input nodestore.MessageInterface) ([]byte, error) {
	return nodebase.EncodeFilePayload(input.GetPayload(), n.config.Encoding)
}

// Validate checks the file node configuration
func (n *FileNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
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

// NewFileNode creates a new file (write) node
func NewFileNode(node nodestore.NodeInterface, config *FileConfig, host nodebase.NodeHost) *FileNode {
	return &FileNode{
		BaseNode: nodebase.NewBaseNode(node),
		config:   config,
		host:     host,
	}
}

// Execute writes the payload to the resolved filename and returns the
// input message with Metadata["filename"] set.
