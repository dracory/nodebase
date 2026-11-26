package builtin

import (
	"testing"

	"github.com/dracory/nodebase"
)

// TestPortedTypesRegistered asserts the nine types ported from the
// retired flowdashboard/nodepacks packs (cutover P3) resolve from the
// registry with a factory, palette metadata, schema and editor HTML —
// everything the nodeadmin palette serves.
// TestAITypesRegistered asserts the ai-* nodes resolve from the
// registry with a factory, palette metadata, schema and editor HTML.
func TestAITypesRegistered(t *testing.T) {
	for _, typeID := range []string{
		"ai-provider", "ai-request", "ai-agent", "ai-tool",
		"ai-tool-return", "ai-memory", "ai-approval",
	} {
		reg, ok := nodebase.RegisteredNode(typeID)
		if !ok {
			t.Fatalf("type %q not registered", typeID)
		}
		if reg.Factory == nil {
			t.Fatalf("type %q has no factory", typeID)
		}
		if reg.Descriptor.Schema == nil {
			t.Fatalf("type %q has no schema", typeID)
		}
		if reg.Descriptor.EditorHTML == "" {
			t.Fatalf("type %q has no editor HTML", typeID)
		}
	}
}

func TestPortedTypesRegistered(t *testing.T) {
	for _, typeID := range []string{
		"uiText", "uiGauge", "uiChart", "uiNotification",
		"uiButton", "uiTextInput", "uiForm",
		"pdf", "docToText", "compress", "hash",
	} {
		reg, ok := nodebase.RegisteredNode(typeID)
		if !ok {
			t.Fatalf("%s not registered", typeID)
		}
		if reg.Factory == nil {
			t.Fatalf("%s registered without a factory", typeID)
		}
		d := reg.Descriptor
		if d.Name == "" || d.Icon == "" || d.Category == "" {
			t.Fatalf("%s descriptor incomplete: %+v", typeID, d)
		}
		if d.Schema == nil {
			t.Fatalf("%s registered without a schema", typeID)
		}
		if d.EditorHTML == "" {
			t.Fatalf("%s registered without editor HTML", typeID)
		}
	}
}
