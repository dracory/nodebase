package nodebase

import "github.com/dracory/nodestore"

// CredentialSentinel is the marker stored in node.Data in place of a
// real credential value (the same convention Node-RED's editor uses,
// "__PWRD__"). Stored workflows and editor payloads carry the sentinel
// where a secret exists; the actual value lives in server-side
// credential storage and is merged back into node.Data at deploy time.
// Exports must strip sentinel values and credential keys entirely.
//
// The canonical registry lives in nodestore so flow documents and the
// runner share one credential-key set; these are thin aliases.
const CredentialSentinel = nodestore.CredentialSentinel

// RegisterCredentialKeys marks config keys of a node type as
// credentials — the same treatment built-in types get: the editor
// masks them, storage moves them out of the flow document behind the
// CredentialSentinel marker, and exports strip them. Node packs call
// this alongside RegisterNodeType (typically from init()). Re-calling
// replaces the key list for the type.
func RegisterCredentialKeys(typeID string, keys []string) {
	nodestore.RegisterCredentialKeys(typeID, keys)
}

// CredentialKeys returns the config keys of a node type that are
// treated as credentials (masked in the editor, stored outside the
// exported flow document, stripped from exports).
func CredentialKeys(nodeType string) []string {
	return nodestore.CredentialKeys(nodeType)
}

// IsCredentialKey reports whether key is a credential field on the
// given node type.
func IsCredentialKey(nodeType, key string) bool {
	return nodestore.IsCredentialKey(nodeType, key)
}

// IsCredentialSentinel reports whether v is the sentinel string left in
// place of a stored credential value.
func IsCredentialSentinel(v interface{}) bool {
	return nodestore.IsCredentialSentinel(v)
}
