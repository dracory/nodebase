package nodebase

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	nodestore "github.com/dracory/nodestore"
)

// This file holds the node type registry — the equivalent of
// Node-RED's RED.nodes.registerType(name, ctor). Both the built-in
// node folders (nodes/<type>) and third-party packs register through
// it; any runner then deploys registered types through the same
// machinery and editor integrations serve palette metadata and JSON
// schemas straight from the registrations.

// NodeFactory builds a NodeExecutor for one workflow node instance.
// It is the stable public contract for node registration. The
// signature is frozen.
//
// node carries the stored definition (ID, Type, WorkflowID, Data
// config map); host is the runtime seam the node executes against —
// the runner passes itself in, so long-lived nodes can emit messages,
// consult LookupNode, or use host services (context store, HTTP
// router, TLS config, queue service) without ever holding a concrete
// host type.
type NodeFactory func(node nodestore.NodeInterface, host NodeHost) (NodeExecutor, error)

// NodeDescriptor is the editor-facing metadata a registration carries:
// what the palette shows for the type. Schema is the JSON schema
// served to editors — including the editor extensions
// "format":"credential" and "showIf". Set groups registrations
// Node-RED nodeSet-style ("core" for built-ins, the pack name for
// third-party) so the palette can group and a future enable/disable
// can operate per set. EditorHTML optionally carries the node's
// Node-RED-style editor bundle (config dialog markup + help text);
// the built-in folders embed theirs from editor.html.
type NodeDescriptor struct {
	Type        string                 `json:"type"`
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Icon        string                 `json:"icon"`
	Category    string                 `json:"category"`
	Set         string                 `json:"set,omitempty"`
	Schema      map[string]interface{} `json:"schema,omitempty"`
	EditorHTML  string                 `json:"editorHtml,omitempty"`
}

// NodeRegistration bundles a type's factory with its editor
// descriptor — the single registration unit for both the runtime
// (deploy) and the palette (nodeadmin).
type NodeRegistration struct {
	Descriptor NodeDescriptor
	Factory    NodeFactory // nil for palette-only registrations
}

// nodeFactories is the process-wide type→registration registry
// populated via RegisterNode/RegisterNodeType (typically from init()).
var nodeFactories = struct {
	sync.RWMutex
	m map[string]NodeRegistration
}{m: map[string]NodeRegistration{}}

// registerNodeIn writes reg into a registry map. upsert=false enforces
// the no-shadowing rule: registering an already-registered typeID is
// an error.
func registerNodeIn(m map[string]NodeRegistration, reg NodeRegistration, upsert bool) error {
	typeID := reg.Descriptor.Type
	if typeID == "" {
		return errors.New("node type id cannot be empty")
	}
	if reg.Descriptor.Set == "" {
		reg.Descriptor.Set = "core"
	}
	if _, exists := m[typeID]; exists && !upsert {
		return fmt.Errorf("node type %q is already registered", typeID)
	}
	m[typeID] = reg
	return nil
}

// RegisterNode adds reg to the global node registry — the path both
// built-ins and node packs take, so every host can deploy the type
// and editors can show it in the palette. Registering an
// already-registered typeID is an error (use UpsertNode to
// deliberately replace).
func RegisterNode(reg NodeRegistration) error {
	nodeFactories.Lock()
	defer nodeFactories.Unlock()
	return registerNodeIn(nodeFactories.m, reg, false)
}

// MustRegisterNode is RegisterNode for built-in registration: a
// duplicate typeID is a programming error and panics.
func MustRegisterNode(reg NodeRegistration) {
	if err := RegisterNode(reg); err != nil {
		panic(err)
	}
}

// UpsertNode registers or replaces a registration — the compatibility
// path for metadata-only palette entries which historically replaced
// existing entries.
func UpsertNode(reg NodeRegistration) error {
	nodeFactories.Lock()
	defer nodeFactories.Unlock()
	return registerNodeIn(nodeFactories.m, reg, true)
}

// RegisterNodeType registers factory under typeID in the global node
// type registry — the minimal pack path equivalent to
// RegisterNode with an empty descriptor. Registering an
// already-registered typeID is an error.
func RegisterNodeType(typeID string, factory NodeFactory) error {
	if factory == nil {
		return fmt.Errorf("node factory for %q cannot be nil", typeID)
	}
	return RegisterNode(NodeRegistration{
		Descriptor: NodeDescriptor{Type: typeID, Name: typeID},
		Factory:    factory,
	})
}

// RegisteredNodes returns a snapshot of every globally registered
// node type, sorted by type id — what editors serve through
// /api/node-fetch-all.
func RegisteredNodes() []NodeRegistration {
	nodeFactories.RLock()
	defer nodeFactories.RUnlock()
	out := make([]NodeRegistration, 0, len(nodeFactories.m))
	for _, reg := range nodeFactories.m {
		out = append(out, reg)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Descriptor.Type < out[j].Descriptor.Type })
	return out
}

// RegisteredNode returns the registration for typeID, if any.
func RegisteredNode(typeID string) (NodeRegistration, bool) {
	nodeFactories.RLock()
	defer nodeFactories.RUnlock()
	reg, ok := nodeFactories.m[typeID]
	return reg, ok
}

// NodeSchemaFor returns the JSON schema registered for typeID, if any.
func NodeSchemaFor(typeID string) (map[string]interface{}, bool) {
	nodeFactories.RLock()
	defer nodeFactories.RUnlock()
	reg, ok := nodeFactories.m[typeID]
	if !ok || reg.Descriptor.Schema == nil {
		return nil, false
	}
	return reg.Descriptor.Schema, true
}
