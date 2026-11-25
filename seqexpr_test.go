package nodebase

import (
	"testing"
)

// TestEvalSeqExprComparisons covers the comparison operators the
// sequence-expression subset added for switch-value expressions
// ((payload % 2) = 0 style): strict equality without string↔number
// coercion and undefined propagation.
func TestEvalSeqExprComparisons(t *testing.T) {
	ctx := map[string]interface{}{
		"payload": 8.0,
		"topic":   "b",
	}
	cases := []struct {
		src  string
		want interface{}
	}{
		{"(payload % 2) = 0", true},
		{"(payload % 2) = 1", false},
		{"(payload % 2) != 0", false},
		{"payload = 8", true},
		{"payload = \"8\"", false}, // strict — no string↔number coercion
		{"topic = \"b\"", true},
		{"payload > 4", true},
		{"payload >= 8", true},
		{"payload < 8", false},
		{"payload <= 8", true},
		{"topic > \"a\"", true},
		{"topic < \"a\"", false},
		{"true = true", true},
		{"payload % 3 = 2", true},
	}
	for _, c := range cases {
		got, err := EvalSeqExpr(c.src, ctx, nil)
		if err != nil {
			t.Fatalf("%s: %v", c.src, err)
		}
		if got != c.want {
			t.Fatalf("%s: got %v want %v", c.src, got, c.want)
		}
	}
	// An undefined operand makes the comparison undefined (nil).
	for _, src := range []string{"missing = 1", "1 = missing", "missing < 1", "1 < \"x\""} {
		got, err := EvalSeqExpr(src, ctx, nil)
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		if got != nil {
			t.Fatalf("%s: got %v want undefined", src, got)
		}
	}
	// Bad syntax still fails loudly at compile time.
	for _, src := range []string{"payload == 1", "1 <", "payload <> 1"} {
		if err := CompileSeqExpr(src); err == nil {
			t.Fatalf("%s: expected compile error", src)
		}
	}
}
