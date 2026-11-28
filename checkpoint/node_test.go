package checkpoint

import "testing"

func TestSchemaOptionsExplainCheckpointChoices(t *testing.T) {
	props := schema["properties"].(map[string]interface{})

	policy := props["policy"].(map[string]interface{})
	policyOptions, ok := policy["options"].([]map[string]interface{})
	if !ok || len(policyOptions) != 4 {
		t.Fatalf("policy should expose four rich options, got %#v", policy["options"])
	}
	backend := props["backend"].(map[string]interface{})
	backendOptions, ok := backend["options"].([]map[string]interface{})
	if !ok || len(backendOptions) != 2 {
		t.Fatalf("backend should expose two rich options, got %#v", backend["options"])
	}
	if backendOptions[1]["title"] != "Directory files" || backendOptions[1]["description"] == "" {
		t.Fatalf("directory backend should be explained: %#v", backendOptions[1])
	}
}
