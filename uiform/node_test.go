package uiform

import "testing"

func TestSchemaFieldsUseRows(t *testing.T) {
	props := schema["properties"].(map[string]interface{})
	fields := props["fields"].(map[string]interface{})
	if fields["type"] != "array" {
		t.Fatalf("fields should be an array, got %#v", fields["type"])
	}
	items := fields["items"].(map[string]interface{})
	itemProps, ok := items["properties"].(map[string]interface{})
	if !ok {
		t.Fatalf("fields should use item row properties, got %#v", items)
	}
	for _, key := range []string{"name", "label", "type", "options", "value"} {
		if _, ok := itemProps[key]; !ok {
			t.Fatalf("field row missing %q", key)
		}
	}
	value := itemProps["value"].(map[string]interface{})
	if value["format"] != "typedinput" || value["typeField"] != "valueType" {
		t.Fatalf("initial value should be typed input: %#v", value)
	}
}
