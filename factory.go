package nodebase

import (
	"encoding/json"
	"fmt"

	nodestore "github.com/dracory/nodestore"
)

// DecodeConfig maps the node's stored Data map onto the typed config
// struct via JSON — the decoding every built-in factory applies.
func DecodeConfig(node nodestore.NodeInterface, cfg interface{}) error {
	raw, err := json.Marshal(node.GetData())
	if err != nil {
		return fmt.Errorf("invalid %s node data: %w", node.GetType(), err)
	}
	if err := json.Unmarshal(raw, cfg); err != nil {
		return fmt.Errorf("invalid %s config: %w", node.GetType(), err)
	}
	return nil
}
