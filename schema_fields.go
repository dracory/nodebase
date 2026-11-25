package nodebase

// errorCatchSchemaField returns the shared "errorCatch" schema field
// for the external-facing built-ins that support the opt-in error
// output port (task error_catch_port): when enabled the
// node gains a dedicated "error" port after its normal outputs; a
// node failure emits msg.error={text,node,full} on it when wired,
// else falls through to the catch/problem-queue ladder.
func ErrorCatchSchemaField() map[string]interface{} {
	return map[string]interface{}{
		"type":        "boolean",
		"title":       "Error Catch Port",
		"description": "Adds a dedicated \"error\" output port; a node failure emits msg.error={text,node,full} on it when wired, else the catch/problem-queue ladder applies",
		"default":     false,
	}
}
