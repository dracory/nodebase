package nodebase

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	nodestore "github.com/dracory/nodestore"
	"path/filepath"
	"strings"
)

// File-path/payload helpers shared by the filesystem nodes (file,
// fileIn, fsIn, fsOut, watch, tail).

// resolveFilePath applies the host's FileRoot sandbox. When FileRoot is
// empty the path passes through unchanged (unrestricted — document at
// deploy time by the consuming application). When set, the cleaned absolute path
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
	// Resolve symlinks before the containment check: a symlink inside
	// the root pointing outside must not be a clean escape (CWE-59).
	// EvalSymlinks fails when the target does not exist yet (create
	// mode), so fall back to the parent directory's resolution.
	if resolved, err := filepath.EvalSymlinks(rootAbs); err == nil {
		rootAbs = resolved
	}
	if resolved, err := filepath.EvalSymlinks(pathAbs); err == nil {
		pathAbs = resolved
	} else if resolved, err := filepath.EvalSymlinks(filepath.Dir(pathAbs)); err == nil {
		pathAbs = filepath.Join(resolved, filepath.Base(pathAbs))
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

// fileFilename resolves the node's target filename per the filename
// source (see FileFilenameValue), then applies the host FileRoot
// restriction. workflowID scopes "flow" context lookups; an empty
// workflowID leaves the flow namespace unscoped.
func FileFilename(configured, filenameType string, input nodestore.MessageInterface, host NodeHost, workflowID string) (string, error) {
	name, err := FileFilenameValue(configured, filenameType, input, host, workflowID)
	if err != nil {
		return "", err
	}
	var root string
	if host != nil {
		root = host.FileRootPath()
	}
	return ResolveFilePath(root, name)
}

// FileFilenameValue resolves the raw filename string a filename
// typedInput produces, before any FileRoot sandboxing. "msg" reads the
// message's filename property (via lookupProperty); "flow"/"global"
// read the configured value as a context key through the host's
// context store; anything else ("fixed", "", NR's "str") is the fixed
// configured filename itself.
func FileFilenameValue(configured, filenameType string, input nodestore.MessageInterface, host NodeHost, workflowID string) (string, error) {
	switch filenameType {
	case "msg":
		v, ok := LookupProperty(input, "filename")
		if !ok {
			return "", errors.New("filenameType is msg but the message has no filename property")
		}
		return fmt.Sprintf("%v", v), nil
	case ContextScopeFlow, ContextScopeGlobal:
		rc := &RuleContext{Host: host, WorkflowID: workflowID}
		v, ok := rc.ContextGet(filenameType, configured)
		if !ok {
			return "", fmt.Errorf("filenameType is %s but %q is not set in %s context", filenameType, configured, filenameType)
		}
		return fmt.Sprintf("%v", v), nil
	default:
		return configured, nil
	}
}
