package nodebase

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	nodestore "github.com/dracory/nodestore"
	"strings"
)

// File-path/payload helpers shared by the filesystem nodes (file,
// fileIn, fsIn, fsOut, watch, tail).

// resolveFilePath applies the host's FileRoot sandbox. When FileRoot is
// empty the path passes through unchanged (unrestricted — document at
// deploy time via GOFLOW_FILE_ROOT). When set, the cleaned absolute path
// must stay under the root; ".." escapes are rejected.
func ResolveFilePath(root, name string) (string, error) {
	if name == "" {
		return "", errors.New("filename is empty")
	}
	if root == "" {
		return name, nil
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("invalid file root: %w", err)
	}
	pathAbs, err := filepath.Abs(name)
	if err != nil {
		return "", fmt.Errorf("invalid filename %q: %w", name, err)
	}
	rel, err := filepath.Rel(rootAbs, pathAbs)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("filename %q escapes file root %q", name, root)
	}
	return pathAbs, nil
}

// encodeFilePayload converts message data into bytes per encoding:
// strings/[]byte pass through, other values are JSON-marshalled;
// base64/hex decode a string payload first. Shared by the file and
// fsOut nodes.
func EncodeFilePayload(data interface{}, encoding string) ([]byte, error) {
	var raw []byte
	switch v := data.(type) {
	case string:
		raw = []byte(v)
	case []byte:
		raw = v
	default:
		marshalled, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("file node cannot marshal payload: %w", err)
		}
		raw = marshalled
	}

	switch encoding {
	case "base64":
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(raw)))
		if err != nil {
			return nil, fmt.Errorf("file node base64 decode failed: %w", err)
		}
		return decoded, nil
	case "hex":
		decoded, err := hex.DecodeString(strings.TrimSpace(string(raw)))
		if err != nil {
			return nil, fmt.Errorf("file node hex decode failed: %w", err)
		}
		return decoded, nil
	default: // "utf8" / "none" / empty
		return raw, nil
	}
}

// fileFilename resolves the node's target filename. With filenameType
// "msg" the name comes from msg.Payload["filename"] or
// msg.Metadata["filename"] (via lookupProperty); otherwise the fixed
// configured filename is used. The host FileRoot restriction applies.
func FileFilename(configured, filenameType string, input nodestore.MessageInterface, host NodeHost) (string, error) {
	name := configured
	if filenameType == "msg" {
		v, ok := LookupProperty(input, "filename")
		if !ok {
			return "", errors.New("filenameType is msg but the message has no filename property")
		}
		name = fmt.Sprintf("%v", v)
	}
	var root string
	if host != nil {
		root = host.FileRootPath()
	}
	return ResolveFilePath(root, name)
}
