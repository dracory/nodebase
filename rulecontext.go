package nodebase

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// RuleContext carries the host seam and node identity a change or
// switch rule needs to resolve environment variables and flow/global
// context at runtime — the same seam the function node's
// env.get/flow.get/global.get use. A nil *RuleContext disables those
// resolutions: context reads miss, context writes are skipped and env
// lookups fail, matching the behaviour of a host-less executor.
type RuleContext struct {
	Host       NodeHost
	WorkflowID string
	NodeID     string
}

// contextScopeID maps a rule scope ("flow"/"global") onto the scope ID
// convention ContextStore uses: flow context namespaces by workflow,
// global is engine-wide (empty scope ID).
func (rc *RuleContext) contextScopeID(scope string) (string, bool) {
	switch scope {
	case ContextScopeFlow:
		return rc.WorkflowID, true
	case ContextScopeGlobal:
		return "", true
	}
	return "", false
}

// ContextGet reads a key from flow/global context; ok is false when no
// host store is wired or the key is absent.
func (rc *RuleContext) ContextGet(scope, key string) (interface{}, bool) {
	if rc == nil || rc.Host == nil {
		return nil, false
	}
	scopeID, ok := rc.contextScopeID(scope)
	if !ok {
		return nil, false
	}
	store := rc.Host.ResolvedContextStore()
	if store == nil {
		return nil, false
	}
	v, found, err := store.GetContext(scope, scopeID, key)
	if err != nil || !found {
		return nil, false
	}
	return v, true
}

// ContextSet writes a key into flow/global context; a no-op when the
// host offers no store.
func (rc *RuleContext) ContextSet(scope, key string, value interface{}) {
	if rc == nil || rc.Host == nil {
		return
	}
	scopeID, ok := rc.contextScopeID(scope)
	if !ok {
		return
	}
	if store := rc.Host.ResolvedContextStore(); store != nil {
		_ = store.SetContext(scope, scopeID, key, value)
	}
}

// ContextDelete removes a key from flow/global context.
func (rc *RuleContext) ContextDelete(scope, key string) {
	if rc == nil || rc.Host == nil {
		return
	}
	scopeID, ok := rc.contextScopeID(scope)
	if !ok {
		return
	}
	if store := rc.Host.ResolvedContextStore(); store != nil {
		_ = store.DeleteContext(scope, scopeID, key)
	}
}

// ContextAvailable reports whether the host actually provides a
// context store — deploy-time checks use it to fail loud on rules that
// would otherwise silently no-op.
func (rc *RuleContext) ContextAvailable(scope string) bool {
	if rc == nil || rc.Host == nil {
		return false
	}
	if _, ok := rc.contextScopeID(scope); !ok {
		return false
	}
	return rc.Host.ResolvedContextStore() != nil
}

// Env resolves an environment variable the way env.get does: the
// host's layered node env first, then the process environment when
// the host opts in via OSEnvAllowed.
func (rc *RuleContext) Env(name string) (interface{}, bool) {
	if rc == nil || rc.Host == nil {
		return nil, false
	}
	if v, ok := rc.Host.NodeEnv(rc.WorkflowID, rc.NodeID, name); ok {
		return v, true
	}
	g, ok := rc.Host.(interface{ OSEnvAllowed() bool })
	if !ok || !g.OSEnvAllowed() {
		return nil, false
	}
	return os.LookupEnv(name)
}

// EvalRuleExpr evaluates the small expression subset rules accept for
// value type "jsonata": numeric and string literals, parentheses, the
// arithmetic operators + - * / %, and the context functions
// $flowContext("key"), $globalContext("key") and $env("NAME").
// Anything else — variables, & concatenation, comparisons, the wider
// JSONata language — fails to compile so deploy still errors loudly on
// unsupported expressions. An operand that resolves to "undefined"
// (a missing context key) makes the whole expression undefined (nil),
// matching upstream semantics where undefined propagates.
func EvalRuleExpr(src string, rc *RuleContext) (interface{}, error) {
	fn, err := compileRuleExpr(src)
	if err != nil {
		return nil, err
	}
	v, _ := fn(rc)
	return v, nil
}

// CompileRuleExpr validates an expression at deploy time without
// evaluating it; the error is non-nil exactly when EvalRuleExpr would
// fail to compile.
func CompileRuleExpr(src string) error {
	_, err := compileRuleExpr(src)
	return err
}

// ruleExprFunc is a compiled expression: it returns the value and
// whether the result is defined (undefined propagates nil).
type ruleExprFunc func(rc *RuleContext) (interface{}, bool)

func compileRuleExpr(src string) (ruleExprFunc, error) {
	p := &ruleExprParser{src: strings.TrimSpace(src)}
	fn, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	p.skipSpace()
	if p.pos != len(p.src) {
		return nil, fmt.Errorf("unexpected trailing input %q", p.src[p.pos:])
	}
	return fn, nil
}

// ruleExprParser is a recursive-descent parser over the supported
// expression subset.
type ruleExprParser struct {
	src string
	pos int
}

func (p *ruleExprParser) skipSpace() {
	for p.pos < len(p.src) && (p.src[p.pos] == ' ' || p.src[p.pos] == '\t' || p.src[p.pos] == '\n' || p.src[p.pos] == '\r') {
		p.pos++
	}
}

func (p *ruleExprParser) parseExpr() (ruleExprFunc, error) {
	left, err := p.parseTerm()
	if err != nil {
		return nil, err
	}
	for {
		p.skipSpace()
		if p.pos >= len(p.src) || (p.src[p.pos] != '+' && p.src[p.pos] != '-') {
			return left, nil
		}
		op := p.src[p.pos]
		p.pos++
		right, err := p.parseTerm()
		if err != nil {
			return nil, err
		}
		left = ruleExprArithmetic(left, right, op)
	}
}

func (p *ruleExprParser) parseTerm() (ruleExprFunc, error) {
	left, err := p.parseFactor()
	if err != nil {
		return nil, err
	}
	for {
		p.skipSpace()
		if p.pos >= len(p.src) || (p.src[p.pos] != '*' && p.src[p.pos] != '/' && p.src[p.pos] != '%') {
			return left, nil
		}
		op := p.src[p.pos]
		p.pos++
		right, err := p.parseFactor()
		if err != nil {
			return nil, err
		}
		left = ruleExprArithmetic(left, right, op)
	}
}

// ruleExprArithmetic combines two operands numerically; an undefined
// operand makes the result undefined.
func ruleExprArithmetic(a, b ruleExprFunc, op byte) ruleExprFunc {
	return func(rc *RuleContext) (interface{}, bool) {
		av, ok := a(rc)
		if !ok {
			return nil, false
		}
		bv, ok := b(rc)
		if !ok {
			return nil, false
		}
		af, err1 := ToFloat(av)
		bf, err2 := ToFloat(bv)
		if err1 != nil || err2 != nil {
			return nil, false
		}
		switch op {
		case '+':
			return af + bf, true
		case '-':
			return af - bf, true
		case '*':
			return af * bf, true
		case '/':
			return af / bf, true
		case '%':
			if bf == 0 {
				return nil, false
			}
			return float64(int64(af) % int64(bf)), true
		}
		return nil, false
	}
}

func (p *ruleExprParser) parseFactor() (ruleExprFunc, error) {
	p.skipSpace()
	if p.pos >= len(p.src) {
		return nil, fmt.Errorf("unexpected end of expression")
	}
	c := p.src[p.pos]
	switch {
	case c == '(':
		p.pos++
		fn, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		p.skipSpace()
		if p.pos >= len(p.src) || p.src[p.pos] != ')' {
			return nil, fmt.Errorf("missing closing parenthesis")
		}
		p.pos++
		return fn, nil
	case c == '-':
		p.pos++
		fn, err := p.parseFactor()
		if err != nil {
			return nil, err
		}
		return func(rc *RuleContext) (interface{}, bool) {
			v, ok := fn(rc)
			if !ok {
				return nil, false
			}
			f, err := ToFloat(v)
			if err != nil {
				return nil, false
			}
			return -f, true
		}, nil
	case c == '"' || c == '\'':
		return p.parseString()
	case c == '$':
		return p.parseFunc()
	case c >= '0' && c <= '9' || c == '.':
		return p.parseNumber()
	}
	return nil, fmt.Errorf("unsupported syntax at %q", p.src[p.pos:])
}

// parseNumber reads a numeric literal (int or float).
func (p *ruleExprParser) parseNumber() (ruleExprFunc, error) {
	start := p.pos
	for p.pos < len(p.src) && (p.src[p.pos] >= '0' && p.src[p.pos] <= '9' || p.src[p.pos] == '.') {
		p.pos++
	}
	f, err := strconv.ParseFloat(p.src[start:p.pos], 64)
	if err != nil {
		return nil, fmt.Errorf("invalid number %q", p.src[start:p.pos])
	}
	return func(rc *RuleContext) (interface{}, bool) { return f, true }, nil
}

// parseString reads a single- or double-quoted literal (no escapes
// beyond the quote character itself).
func (p *ruleExprParser) parseString() (ruleExprFunc, error) {
	quote := p.src[p.pos]
	p.pos++
	start := p.pos
	for p.pos < len(p.src) && p.src[p.pos] != quote {
		if p.src[p.pos] == '\\' && p.pos+1 < len(p.src) {
			p.pos++
		}
		p.pos++
	}
	if p.pos >= len(p.src) {
		return nil, fmt.Errorf("unterminated string")
	}
	s := p.src[start:p.pos]
	p.pos++
	return func(rc *RuleContext) (interface{}, bool) { return s, true }, nil
}

// parseFunc reads a $name("arg") context/env function call.
func (p *ruleExprParser) parseFunc() (ruleExprFunc, error) {
	p.pos++ // consume '$'
	start := p.pos
	for p.pos < len(p.src) && (p.src[p.pos] >= 'a' && p.src[p.pos] <= 'z' || p.src[p.pos] >= 'A' && p.src[p.pos] <= 'Z') {
		p.pos++
	}
	name := p.src[start:p.pos]
	var scope string
	switch name {
	case "flowContext":
		scope = ContextScopeFlow
	case "globalContext":
		scope = ContextScopeGlobal
	case "env":
		scope = "env"
	default:
		return nil, fmt.Errorf("unsupported function $%s", name)
	}
	p.skipSpace()
	if p.pos >= len(p.src) || p.src[p.pos] != '(' {
		return nil, fmt.Errorf("$%s must be called with a quoted argument", name)
	}
	p.pos++
	p.skipSpace()
	if p.pos >= len(p.src) || (p.src[p.pos] != '"' && p.src[p.pos] != '\'') {
		return nil, fmt.Errorf("$%s argument must be a quoted name", name)
	}
	argFn, err := p.parseString()
	if err != nil {
		return nil, err
	}
	p.skipSpace()
	if p.pos >= len(p.src) || p.src[p.pos] != ')' {
		return nil, fmt.Errorf("$%s missing closing parenthesis", name)
	}
	p.pos++
	if scope == "env" {
		return func(rc *RuleContext) (interface{}, bool) {
			key, _ := argFn(rc)
			name, _ := key.(string)
			return rc.Env(name)
		}, nil
	}
	return func(rc *RuleContext) (interface{}, bool) {
		key, _ := argFn(rc)
		k, _ := key.(string)
		return rc.ContextGet(scope, k)
	}, nil
}
