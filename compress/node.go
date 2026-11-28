// Package compress contains the compress node: whole-message gzip,
// zlib, or raw flate compression and decompression using only the Go
// standard library.
package compress

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"context"
	_ "embed"
	"fmt"
	"io"
	"strings"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := &Config{Operation: "compress", Algorithm: "gzip", Property: "payload"}
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	if err := validateConfig(c); err != nil {
		return nil, err
	}
	return NewNode(n, c), nil
}

func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "compress",
			Name:        "Compress",
			Description: "Compresses or decompresses a whole message property with gzip, zlib, or raw flate",
			Icon:        "archive",
			Category:    "Transform",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"operation": map[string]interface{}{
			"type": "string", "title": "Operation", "enum": []string{"compress", "decompress"}, "default": "compress",
		},
		"algorithm": map[string]interface{}{
			"type": "string", "title": "Algorithm", "enum": []string{"gzip", "zlib", "flate", "deflate"}, "default": "gzip",
			"description": "flate and deflate are aliases for raw DEFLATE data",
		},
		"property": map[string]interface{}{
			"type": "string", "title": "Property", "default": "payload",
			"description": "Message property to process as one whole byte sequence; payload, dotted payload-map paths, or msg.* metadata paths",
		},
	},
}

// Config holds the compress node configuration.
type Config struct {
	Operation string `json:"operation"`
	Algorithm string `json:"algorithm"`
	Property  string `json:"property"`
}

// Node performs whole-message compression or decompression.
type Node struct {
	*nodebase.BaseNode
	cfg *Config
}

// NewNode creates a compress node.
func NewNode(node nodestore.NodeInterface, cfg *Config) *Node {
	return &Node{BaseNode: nodebase.NewBaseNode(node), cfg: cfg}
}

func validateConfig(c *Config) error {
	if c == nil {
		return fmt.Errorf("compress: missing configuration")
	}
	if c.Operation != "compress" && c.Operation != "decompress" {
		return fmt.Errorf("compress: unsupported operation %q", c.Operation)
	}
	switch normalizedAlgorithm(c.Algorithm) {
	case "gzip", "zlib", "flate":
	default:
		return fmt.Errorf("compress: unsupported algorithm %q", c.Algorithm)
	}
	if strings.TrimSpace(c.Property) == "" {
		return fmt.Errorf("compress: property is required")
	}
	return nil
}

func normalizedAlgorithm(algorithm string) string {
	if algorithm == "deflate" {
		return "flate"
	}
	if algorithm == "" {
		return "gzip"
	}
	return algorithm
}

// Execute transforms the configured property as one complete byte sequence.
func (n *Node) Execute(ctx context.Context, in nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	if err := validateConfig(n.cfg); err != nil {
		return nil, err
	}
	value, ok := nodebase.LookupMessageProperty(in, n.cfg.Property)
	if !ok {
		return nil, fmt.Errorf("compress: property %q not found", n.cfg.Property)
	}
	raw, err := nodebase.PayloadBytes(value)
	if err != nil {
		return nil, fmt.Errorf("compress: unsupported payload type %T: %w", value, err)
	}
	algorithm := normalizedAlgorithm(n.cfg.Algorithm)
	var out []byte
	if n.cfg.Operation == "compress" {
		out, err = compressBytes(algorithm, raw)
	} else {
		out, err = decompressBytes(algorithm, raw)
	}
	if err != nil {
		return nil, err
	}
	if err := nodebase.SetMessageProperty(in, n.cfg.Property, out); err != nil {
		return nil, err
	}
	metadata := in.GetMetadata()
	if metadata == nil {
		metadata = map[string]interface{}{}
	}
	metadata["compression"] = map[string]interface{}{
		"operation": n.cfg.Operation, "algorithm": algorithm,
		"property": n.cfg.Property, "originalSize": len(raw), "resultSize": len(out),
	}
	in.SetMetadata(metadata)
	return in, nil
}

func compressBytes(algorithm string, raw []byte) ([]byte, error) {
	var buf bytes.Buffer
	var w io.WriteCloser
	var err error
	switch algorithm {
	case "gzip":
		w = gzip.NewWriter(&buf)
	case "zlib":
		w = zlib.NewWriter(&buf)
	case "flate":
		w, err = flate.NewWriter(&buf, flate.DefaultCompression)
		if err != nil {
			return nil, fmt.Errorf("compress: %s compression setup failed: %w", algorithm, err)
		}
	default:
		return nil, fmt.Errorf("compress: unsupported compression algorithm %q", algorithm)
	}
	if w == nil {
		return nil, fmt.Errorf("compress: %s compression setup failed", algorithm)
	}
	if _, err := w.Write(raw); err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("compress: %s compression failed: %w", algorithm, err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("compress: %s compression failed: %w", algorithm, err)
	}
	return buf.Bytes(), nil
}

func decompressBytes(algorithm string, raw []byte) ([]byte, error) {
	var r io.ReadCloser
	var err error
	switch algorithm {
	case "gzip":
		r, err = gzip.NewReader(bytes.NewReader(raw))
	case "zlib":
		r, err = zlib.NewReader(bytes.NewReader(raw))
	case "flate":
		r = flate.NewReader(bytes.NewReader(raw))
	}
	if err != nil {
		return nil, fmt.Errorf("compress: invalid %s input: %w", algorithm, err)
	}
	defer r.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("compress: invalid %s input: %w", algorithm, err)
	}
	return out, nil
}
