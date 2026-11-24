package mqttin

// Internal coverage ported from flowrunner's node_mqtt_test.go
// (subscriptionTopic is unexported on MQTTInConfig).

import "testing"

// TestMQTTSharedSubscriptionTopic checks the effective filter built from
// topic + shareGroup without needing a broker.
func TestMQTTSharedSubscriptionTopic(t *testing.T) {
	cases := []struct {
		name  string
		topic string
		group string
		want  string
	}{
		{"plain", "a/b", "", "a/b"},
		{"shared", "a/b", "workers", "$share/workers/a/b"},
		{"shared wildcard", "a/#", "g", "$share/g/a/#"},
		{"raw share passthrough", "$share/g/a/b", "", "$share/g/a/b"},
		{"raw share wins over group", "$share/g/a/b", "other", "$share/g/a/b"},
		{"group whitespace trimmed", "a/b", " g ", "$share/g/a/b"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &MQTTInConfig{Topic: tc.topic, ShareGroup: tc.group}
			if got := c.subscriptionTopic(); got != tc.want {
				t.Fatalf("subscriptionTopic()=%q want %q", got, tc.want)
			}
		})
	}
}
