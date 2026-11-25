package nodebase

// Unit tests for the JSONata seam (jsonata.go): expression compile,
// evaluation against the Node-RED message shape, and the
// $flowContext/$globalContext/$env bindings through RuleContext.

import (
	"testing"

	nodestore "github.com/dracory/nodestore"
)

func TestCompileJSONata(t *testing.T) {
	if err := CompileJSONata(`payload & ", World!"`); err != nil {
		t.Fatalf("valid expression should compile: %v", err)
	}
	if err := CompileJSONata(`payload & (`); err == nil {
		t.Fatal("invalid expression should fail to compile")
	}
	// Surrounding whitespace is tolerated (imports carry stray tabs).
	if err := CompileJSONata("payload\t"); err != nil {
		t.Fatalf("trailing whitespace should compile: %v", err)
	}
}

func TestEvalJSONata(t *testing.T) {
	msg := nodestore.NewMessage().
		SetPayload("Hello").
		SetMetadata(map[string]interface{}{"topic": "Sample"})

	// Concatenation across payload and metadata keys.
	v, err := EvalJSONata(`"[" & topic & "] " & payload`, msg)
	if err != nil || v != "[Sample] Hello" {
		t.Fatalf("concat wrong: %v (%v)", v, err)
	}

	// Numeric comparison on a numeric payload.
	num := nodestore.NewMessage().SetPayload(float64(8))
	v, err = EvalJSONata(`(payload % 2) = 0`, num)
	if err != nil || v != true {
		t.Fatalf("comparison wrong: %v (%v)", v, err)
	}

	// A missing path is undefined → (nil, nil), not an error.
	v, err = EvalJSONata(`missing.path`, msg)
	if err != nil || v != nil {
		t.Fatalf("undefined should be nil, nil: %v (%v)", v, err)
	}

	// Escape sequences follow JSONata/JS semantics (\b is backspace).
	v, err = EvalJSONata(`"a\bb\n"`, msg)
	if err != nil || v != "a\bb\n" {
		t.Fatalf("escapes wrong: %q (%v)", v, err)
	}

	// A syntax error surfaces as an error.
	if _, err := EvalJSONata(`payload & (`, msg); err == nil {
		t.Fatal("invalid expression should error")
	}

	// A nil message evaluates against an empty object.
	v, err = EvalJSONata(`"literal"`, nil)
	if err != nil || v != "literal" {
		t.Fatalf("nil message wrong: %v (%v)", v, err)
	}
}

func TestEvalJSONataCtx(t *testing.T) {
	store := NewMemoryContextStore()
	host := &stubHost{
		store: store,
		env:   map[string]interface{}{"GREETING": "hi"},
	}
	rc := &RuleContext{Host: host, WorkflowID: "wf1", NodeID: "n1"}
	msg := nodestore.NewMessage()

	// Unset context key is undefined; undefined + 1 evaluates to an
	// error, matching upstream (undefined arithmetic fails).
	if _, err := EvalJSONataCtx(`$flowContext("count") + 1`, msg, rc); err == nil {
		t.Fatal("undefined context operand should error")
	}
	if err := store.SetContext(ContextScopeFlow, "wf1", "count", float64(41)); err != nil {
		t.Fatal(err)
	}
	v, err := EvalJSONataCtx(`$flowContext("count") + 1`, msg, rc)
	if err != nil || v != float64(42) {
		t.Fatalf("flowContext wrong: %v (%v)", v, err)
	}
	if err := store.SetContext(ContextScopeGlobal, "", "name", "g"); err != nil {
		t.Fatal(err)
	}
	v, err = EvalJSONataCtx(`$globalContext("name") & "-" & $env("GREETING")`, msg, rc)
	if err != nil || v != "g-hi" {
		t.Fatalf("globalContext/env wrong: %v (%v)", v, err)
	}

	// Without a RuleContext the functions are unregistered and the
	// expression fails at evaluation.
	if _, err := EvalJSONata(`$flowContext("count")`, msg); err == nil {
		t.Fatal("unregistered context function should error")
	}
}
