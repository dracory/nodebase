package nodebase

import "testing"

func TestUIFormFieldsNormalizeEditorRows(t *testing.T) {
	fields := UIFormFields(map[string]interface{}{
		"fields": []interface{}{
			map[string]interface{}{
				"name":    "colour",
				"label":   "Colour",
				"type":    "select",
				"options": "red, green ,blue",
				"value":   "green",
			},
			map[string]interface{}{
				"name":      "enabled",
				"type":      "checkbox",
				"value":     "true",
				"valueType": "bool",
			},
			map[string]interface{}{
				"name":      "count",
				"type":      "number",
				"value":     "42",
				"valueType": "num",
			},
		},
	})
	if len(fields) != 3 {
		t.Fatalf("normalized field count wrong: %#v", fields)
	}
	if len(fields[0].Options) != 3 || fields[0].Options[1] != "green" {
		t.Fatalf("comma-separated options wrong: %#v", fields[0].Options)
	}
	if fields[1].Value != true {
		t.Fatalf("bool typed value wrong: %#v", fields[1].Value)
	}
	if fields[2].Value != 42.0 {
		t.Fatalf("numeric typed value wrong: %#v", fields[2].Value)
	}
}
