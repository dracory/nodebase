package queue

import "testing"

func TestSchemaOptionsExplainQueueChoices(t *testing.T) {
	props := schema["properties"].(map[string]interface{})

	policy := props["policy"].(map[string]interface{})
	policyOptions, ok := policy["options"].([]map[string]interface{})
	if !ok || len(policyOptions) != 4 {
		t.Fatalf("policy should expose four rich options, got %#v", policy["options"])
	}
	if policyOptions[0]["title"] != "Block (backpressure)" || policyOptions[0]["description"] == "" {
		t.Fatalf("block policy should explain backpressure: %#v", policyOptions[0])
	}
	if enum, ok := policy["enum"].([]string); !ok || len(enum) != 4 {
		t.Fatalf("policy should retain enum values for schema consumers: %#v", policy["enum"])
	}

	backend := props["backend"].(map[string]interface{})
	backendOptions, ok := backend["options"].([]map[string]interface{})
	if !ok || len(backendOptions) != 2 {
		t.Fatalf("backend should expose two rich options, got %#v", backend["options"])
	}
	if backendOptions[0]["title"] != "Durable inbox store" || backendOptions[0]["description"] == "" {
		t.Fatalf("durable backend should be explained: %#v", backendOptions[0])
	}
}
