package nodebase

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	nodestore "github.com/dracory/nodestore"
)

// This file holds the manifest half of the dashboard contract (see
// uishare.go for the state-push half): a per-type describer registry
// plus the workflow → widget-manifest collector a dashboard hub uses
// to answer "what renders on this flow's dashboard".
//
// The contract is engine-agnostic: describers read the stored node
// definition (nodestore.NodeInterface), never a runner type, so the
// registry travels with the nodebase. The 7 core ui* types register
// their describers in uimanifest_widgets.go; a third-party widget
// node type is a describer + node type + schema, no hub changes.

// UIWidgetManifest is the manifest entry for one dashboard widget
// node instance: everything the dashboard page needs to render it.
//
// Page/Group/Label/Order come from the node's layout fields (see
// UIWidgetLayout); Config carries type-specific settings the
// widget's client-side renderer understands (e.g. min/max for a
// gauge).
type UIWidgetManifest struct {
	ID     string                 `json:"id"`
	Type   string                 `json:"type"`
	Page   string                 `json:"page"`
	Group  string                 `json:"group,omitempty"`
	Label  string                 `json:"label,omitempty"`
	Order  int                    `json:"order,omitempty"`
	Config map[string]interface{} `json:"config,omitempty"`
}

// UIWidgetDescriber extracts a widget's manifest entry from its
// stored node definition. Registered per node type via
// RegisterUIWidget — the "small internal interface" widget node
// types implement so the hub stays type-agnostic.
//
// Implementations read layout fields through UIWidgetLayout and add
// type-specific keys to Config.
type UIWidgetDescriber func(node nodestore.NodeInterface) UIWidgetManifest

// uiDescribers is the process-wide node-type → describer registry,
// populated by widget node packs (typically from init()).
var uiDescribers = struct {
	sync.RWMutex
	m map[string]UIWidgetDescriber
}{m: map[string]UIWidgetDescriber{}}

// RegisterUIWidget registers describer under typeID in the global
// dashboard widget registry. Re-registering replaces the previous
// describer. Returns an error for empty typeID/nil describer.
func RegisterUIWidget(typeID string, describer UIWidgetDescriber) error {
	if typeID == "" {
		return errors.New("dashboard widget type id cannot be empty")
	}
	if describer == nil {
		return fmt.Errorf("dashboard widget describer for %q cannot be nil", typeID)
	}
	uiDescribers.Lock()
	defer uiDescribers.Unlock()
	uiDescribers.m[typeID] = describer
	return nil
}

// uiWidgetDescriberFor resolves the describer registered for typeID.
func uiWidgetDescriberFor(typeID string) (UIWidgetDescriber, bool) {
	uiDescribers.RLock()
	defer uiDescribers.RUnlock()
	d, ok := uiDescribers.m[typeID]
	return d, ok
}

// IsUIWidget reports whether typeID is a registered dashboard widget
// node type. The editor/API uses it to group widget types in the
// palette (convention: NodeType.Category "Dashboard", ui* naming).
func IsUIWidget(typeID string) bool {
	_, ok := uiWidgetDescriberFor(typeID)
	return ok
}

// UIWidgetLayout pulls the shared layout fields every widget node
// carries (page, group, label, order) out of node.GetData(). Widget
// describers call it and then fill Type/Config.
//
// Convention (documented for widget implementers): data keys "page"
// (string, default "default"), "group" (string, optional section
// inside the page), "label" (string, falls back to "name" then the
// node id), "order" (number, optional sort hint within the group).
func UIWidgetLayout(node nodestore.NodeInterface) UIWidgetManifest {
	w := UIWidgetManifest{
		ID:   node.GetID(),
		Type: node.GetType(),
		Page: "default",
	}
	data := node.GetData()
	if v, ok := data["page"].(string); ok && v != "" {
		w.Page = v
	}
	if v, ok := data["group"].(string); ok {
		w.Group = v
	}
	if v, ok := data["label"].(string); ok && v != "" {
		w.Label = v
	} else if v, ok := data["name"].(string); ok && v != "" {
		w.Label = v
	} else {
		w.Label = node.GetID()
	}
	if v, ok := UINumberValue(data["order"]); ok {
		w.Order = int(v)
	}
	return w
}

// CollectUIWidgets builds the manifest for a deployed workflow:
// every enabled node whose type has a registered describer, sorted
// by page → group → order → id for a stable render order.
func CollectUIWidgets(workflow nodestore.WorkflowInterface) []UIWidgetManifest {
	var widgets []UIWidgetManifest
	for _, node := range workflow.GetNodes() {
		if WorkflowNodeDisabled(workflow, node) {
			continue
		}
		describer, ok := uiWidgetDescriberFor(node.GetType())
		if !ok {
			continue
		}
		widgets = append(widgets, describer(node))
	}
	sort.SliceStable(widgets, func(i, j int) bool {
		a, b := widgets[i], widgets[j]
		if a.Page != b.Page {
			return a.Page < b.Page
		}
		if a.Group != b.Group {
			return a.Group < b.Group
		}
		if a.Order != b.Order {
			return a.Order < b.Order
		}
		return a.ID < b.ID
	})
	return widgets
}

// WorkflowNodeDisabled reports whether node is inert for a deploy —
// the runner-agnostic disabled predicate so optional packages (the
// dashboard hub) that collect node subsets of a deployed workflow
// honor the same semantics the engine applies.
//
// A node is disabled when its own "d"/"disabled" data flag is set
// (Node-RED parity), when its child flow (data.childFlow — an id under the
// id-keyed model, a name in legacy documents) is listed in
// workflow.GetView()["disabledFlows"] or flagged enabled:false in
// workflow.GetView()["childFlows"], or when a view group containing it
// (workflow.GetView()["groups"] entry with disabled:true) is
// disabled. noderunner's deploy-time predicate delegates here so the
// semantics stay single-sourced in the nodebase.
func WorkflowNodeDisabled(workflow nodestore.WorkflowInterface, node nodestore.NodeInterface) bool {
	if uiTruthyFlag(node.GetData()["d"]) || uiTruthyFlag(node.GetData()["disabled"]) {
		return true
	}
	if workflow == nil || workflow.GetView() == nil {
		return false
	}
	if mod := StringOf(node.GetData()["childFlow"]); mod != "" {
		// data.childFlow and view.disabledFlows hold child flow ids under
		// the id-keyed model; legacy documents hold names. Both forms
		// resolve through view.childFlows.
		for _, m := range uiArrayOf(workflow.GetView()["disabledFlows"]) {
			if uiFlowRefMatches(workflow, m, mod) {
				return true
			}
		}
		// A child flow entity flagged enabled:false disables every node
		// on it (same state as a disabledFlows listing).
		if uiFlowEntityDisabled(workflow, mod) {
			return true
		}
	}
	for _, g := range uiArrayOf(workflow.GetView()["groups"]) {
		gm, ok := g.(map[string]interface{})
		if !ok || !uiTruthyFlag(gm["disabled"]) {
			continue
		}
		for _, id := range uiArrayOf(gm["nodeIds"]) {
			if StringOf(id) == node.GetID() {
				return true
			}
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Disabled-predicate helpers (private copies of the small view-model
// helpers so the predicate needs no runner imports).
// ---------------------------------------------------------------------------

func uiArrayOf(v any) []any {
	arr, _ := v.([]any)
	return arr
}

func uiTruthyFlag(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		switch strings.ToLower(strings.TrimSpace(t)) {
		case "true", "1", "yes":
			return true
		}
	}
	return false
}

// uiFlowLabel resolves a child flow reference (id or label) to its
// label per view.childFlows, so a name-keyed disabledFlows entry
// matches a node carrying the child flow id and vice versa.
func uiFlowLabel(w nodestore.WorkflowInterface, ref string) string {
	if w == nil || w.GetView() == nil || ref == "" {
		return ref
	}
	for _, m := range uiArrayOf(w.GetView()["childFlows"]) {
		mm, ok := m.(map[string]any)
		if !ok {
			continue
		}
		if StringOf(mm["id"]) == ref {
			if label := StringOf(mm["label"]); label != "" {
				return label
			}
		}
	}
	return ref
}

func uiFlowRefMatches(w nodestore.WorkflowInterface, a, b any) bool {
	ra, rb := StringOf(a), StringOf(b)
	if ra == "" || rb == "" {
		return false
	}
	return ra == rb || uiFlowLabel(w, ra) == uiFlowLabel(w, rb)
}

// uiFlowEntityDisabled reports whether the view.childFlows entity
// matching ref (by id or label) is flagged enabled:false.
func uiFlowEntityDisabled(w nodestore.WorkflowInterface, ref string) bool {
	if w == nil || w.GetView() == nil || ref == "" {
		return false
	}
	for _, m := range uiArrayOf(w.GetView()["childFlows"]) {
		mm, ok := m.(map[string]interface{})
		if !ok {
			continue
		}
		if enabled, isBool := mm["enabled"].(bool); isBool && !enabled {
			if StringOf(mm["id"]) == ref || StringOf(mm["label"]) == ref {
				return true
			}
		}
	}
	return false
}
