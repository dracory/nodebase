package uuid

import (
	"context"
	"strings"
	"testing"
	stdlibuuid "uuid"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

func testNode(version, target string) *Node {
	return NewNode(nodestore.NewNode("u1", "uuid"), &Config{Version: version, Target: target})
}

func testNodeWithFormat(version, target, format string) *Node {
	return NewNode(nodestore.NewNode("u1", "uuid"), &Config{Version: version, Target: target, Format: format})
}

func executeOne(t *testing.T, node *Node, in nodestore.MessageInterface) nodestore.MessageInterface {
	t.Helper()
	out, err := node.Execute(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func parseTargetUUID(t *testing.T, value any) stdlibuuid.UUID {
	t.Helper()
	s, ok := value.(string)
	if !ok {
		t.Fatalf("UUID output is %T, want string", value)
	}
	if len(s) != 36 || s != strings.ToLower(s) {
		t.Fatalf("UUID output %q is not canonical lowercase string form", s)
	}
	parsed, err := stdlibuuid.Parse(s)
	if err != nil {
		t.Fatalf("UUID output does not parse: %v", err)
	}
	return parsed
}

func assertRFCVariantAndVersion(t *testing.T, id stdlibuuid.UUID, version byte) {
	t.Helper()
	if got := id[6] >> 4; got != version {
		t.Fatalf("UUID version bits = %d, want %d (%s)", got, version, id)
	}
	if got := id[8] & 0xc0; got != 0x80 {
		t.Fatalf("UUID variant bits = %#x, want RFC 9562 variant 0x80 (%s)", got, id)
	}
}

func TestUUIDV4Output(t *testing.T) {
	out := executeOne(t, testNode("v4", "payload"), nodestore.NewMessage().SetPayload("input"))
	id := parseTargetUUID(t, out.GetPayload())
	assertRFCVariantAndVersion(t, id, 4)
}

func TestUUIDV7Output(t *testing.T) {
	out := executeOne(t, testNode("v7", "payload"), nodestore.NewMessage().SetPayload("input"))
	id := parseTargetUUID(t, out.GetPayload())
	assertRFCVariantAndVersion(t, id, 7)
}

func TestUUIDSuccessiveExecutionsAreDistinct(t *testing.T) {
	n := testNode("v7", "payload")
	seen := map[string]bool{}
	for i := 0; i < 20; i++ {
		out := executeOne(t, n, nodestore.NewMessage())
		s, _ := out.GetPayload().(string)
		if seen[s] {
			t.Fatalf("UUID %q generated twice", s)
		}
		seen[s] = true
	}
}

func TestUUIDDefaultFactoryConfig(t *testing.T) {
	executor, err := factory(nodestore.NewNode("u-default", "uuid").SetData(map[string]any{}), nil)
	if err != nil {
		t.Fatal(err)
	}
	n, ok := executor.(*Node)
	if !ok {
		t.Fatalf("factory returned %T, want *Node", executor)
	}
	if n.cfg.Version != "v7" || n.cfg.Target != "payload" || n.cfg.Format != "canonical" {
		t.Fatalf("default config = %+v, want v7/payload/canonical", n.cfg)
	}
}

func TestUUIDTargetsAndMessagePreservation(t *testing.T) {
	in := nodestore.NewMessage().SetPayload("original").SetMetadata(map[string]any{"keep": true})
	out := executeOne(t, testNode("v4", "payload"), in)
	if out != in {
		t.Fatal("UUID node should preserve and return the input message object")
	}
	if out.GetMetadata()["keep"] != true {
		t.Fatalf("unrelated metadata lost: %#v", out.GetMetadata())
	}
	parseTargetUUID(t, out.GetPayload())

	metaIn := nodestore.NewMessage().SetPayload("unchanged")
	metaOut := executeOne(t, testNode("v7", "msg.identifiers.correlation"), metaIn)
	if metaOut.GetPayload() != "unchanged" {
		t.Fatalf("metadata target changed payload: %#v", metaOut.GetPayload())
	}
	identifiers, ok := metaOut.GetMetadata()["identifiers"].(map[string]interface{})
	if !ok {
		t.Fatalf("nested metadata missing: %#v", metaOut.GetMetadata())
	}
	parseTargetUUID(t, identifiers["correlation"])
}

func TestUUIDHexFormatOutput(t *testing.T) {
	out := executeOne(t, testNodeWithFormat("v7", "payload", "hex"), nodestore.NewMessage().SetPayload("input"))
	s, ok := out.GetPayload().(string)
	if !ok {
		t.Fatalf("UUID output is %T, want string", out.GetPayload())
	}
	if len(s) != 32 {
		t.Fatalf("hex UUID output %q is %d characters, want 32", s, len(s))
	}
	if strings.Contains(s, "-") {
		t.Fatalf("hex UUID output %q contains dashes", s)
	}
	if s != strings.ToLower(s) {
		t.Fatalf("hex UUID output %q is not lowercase", s)
	}
	// The stdlib parser accepts the bare 32-character hex form; the bits
	// underneath the formatting are still a version-correct RFC 9562 UUID.
	parsed, err := stdlibuuid.Parse(s)
	if err != nil {
		t.Fatalf("hex UUID output does not parse: %v", err)
	}
	assertRFCVariantAndVersion(t, parsed, 7)
}

func TestUUIDEmptyFormatIsCanonical(t *testing.T) {
	// Configurations stored before the format field existed carry no
	// value; they must keep producing the canonical hyphenated form.
	out := executeOne(t, testNodeWithFormat("v7", "payload", ""), nodestore.NewMessage())
	parseTargetUUID(t, out.GetPayload())
}

func TestUUIDValidationErrors(t *testing.T) {
	for _, cfg := range []*Config{
		{Version: "v5", Target: "payload"},
		{Version: "v7", Target: ""},
		{Version: "v7", Target: "   "},
	} {
		if _, err := testNode(cfg.Version, cfg.Target).Execute(context.Background(), nodestore.NewMessage()); err == nil {
			t.Fatalf("config %+v should fail validation", cfg)
		}
	}
	bad := nodestore.NewNode("u-bad", "uuid").SetData(map[string]any{"version": "v5"})
	if _, err := factory(bad, nil); err == nil || !strings.Contains(err.Error(), "unsupported version") {
		t.Fatalf("factory should reject unsupported version: %v", err)
	}
	if _, err := testNodeWithFormat("v7", "payload", "base64").Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("unsupported format should fail validation")
	}
	badFormat := nodestore.NewNode("u-badfmt", "uuid").SetData(map[string]any{"format": "base64"})
	if _, err := factory(badFormat, nil); err == nil || !strings.Contains(err.Error(), "unsupported format") {
		t.Fatalf("factory should reject unsupported format: %v", err)
	}
}

func TestUUIDSelfRegistration(t *testing.T) {
	reg, ok := nodebase.RegisteredNode("uuid")
	if !ok {
		t.Fatal("uuid type not registered")
	}
	if reg.Factory == nil {
		t.Fatal("uuid registration has no factory")
	}
	if reg.Descriptor.Name != "UUID" || reg.Descriptor.Icon == "" || reg.Descriptor.Category == "" {
		t.Fatalf("uuid descriptor incomplete: %+v", reg.Descriptor)
	}
	if reg.Descriptor.Schema == nil {
		t.Fatal("uuid registration has no schema")
	}
	if reg.Descriptor.EditorHTML == "" {
		t.Fatal("uuid registration has no editor HTML")
	}
}
