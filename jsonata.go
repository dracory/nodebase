package nodebase

import (
	"strings"

	nodestore "github.com/dracory/nodestore"
	jsonata "github.com/xiatechs/jsonata-go"
)

// JSONata expression evaluation (Node-RED's "jsonata" value types:
// change tot:"jsonata", switch jsonata_exp / propertyType "jsonata",
// inject payloadType "jsonata", debug targetType "jsonata").
//
// The library is wrapped behind this package-level seam — node
// packages call CompileJSONata/EvalJSONata/EvalJSONataCtx and never
// import the underlying implementation, so it stays swappable.

// CompileJSONata validates a JSONata expression without evaluating
// it; the error is non-nil exactly when the expression fails to parse.
// Deploy-time checks use it so unsupported syntax fails loudly.
func CompileJSONata(expr string) error {
	_, err := jsonata.Compile(strings.TrimSpace(expr))
	return err
}

// EvalJSONata evaluates expr against the whole message, Node-RED
// semantics: the message is a flat object where "payload" is the
// payload and every metadata key (topic, parts, …) is a root key.
// An expression that resolves to "undefined" (a missing path) returns
// (nil, nil) — matching upstream where undefined is not an error.
func EvalJSONata(expr string, msg nodestore.MessageInterface) (interface{}, error) {
	return evalJSONata(expr, msg, nil)
}

// EvalJSONataCtx is EvalJSONata with the Node-RED context functions
// bound through rc: $flowContext("key"), $globalContext("key") and
// $env("NAME"). A nil rc leaves them unregistered — an expression
// calling them then fails at evaluation, as it would upstream without
// a runtime.
func EvalJSONataCtx(expr string, msg nodestore.MessageInterface, rc *RuleContext) (interface{}, error) {
	return evalJSONata(expr, msg, rc)
}

func evalJSONata(expr string, msg nodestore.MessageInterface, rc *RuleContext) (interface{}, error) {
	e, err := jsonata.Compile(strings.TrimSpace(expr))
	if err != nil {
		return nil, err
	}
	if rc != nil {
		exts := map[string]jsonata.Extension{
			"flowContext":   {Func: func(key string) (interface{}, error) { v, _ := rc.ContextGet(ContextScopeFlow, key); return v, nil }},
			"globalContext": {Func: func(key string) (interface{}, error) { v, _ := rc.ContextGet(ContextScopeGlobal, key); return v, nil }},
			"env":           {Func: func(key string) (interface{}, error) { v, _ := rc.Env(key); return v, nil }},
		}
		if err := e.RegisterExts(exts); err != nil {
			return nil, err
		}
	}
	var data map[string]interface{}
	if msg != nil {
		data = MessageMap(msg)
	}
	v, err := e.Eval(data)
	if err != nil {
		if err == jsonata.ErrUndefined {
			return nil, nil
		}
		return nil, err
	}
	return v, nil
}
