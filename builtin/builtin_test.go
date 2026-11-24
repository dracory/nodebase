package builtin

import (
	"testing"

	"github.com/dracory/nodebase"
)

// TestPortedTypesRegistered asserts the nine types ported from the
// retired flowdashboard/nodepacks packs (cutover P3) resolve from the
// registry with a factory, palette metadata, schema and editor HTML —
// everything the nodeadmin palette serves.
func TestPortedTypesRegistered(t *testing.T) {
	for _, typeID := range []string{
		"uiText", "uiGauge", "uiChart", "uiNotification",
		"uiButton", "uiTextInput", "uiForm",
		"pdf", "docToText",
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
