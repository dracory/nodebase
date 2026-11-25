package nodebase

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// Sequence-expression support: a deliberately small subset of JSONata
// used by the sequence nodes (join reduce expressions, sort key
// expressions). Supported syntax: numeric/string/boolean/null
// literals, dotted paths resolved against a context object, the
// sequence variables ($A, $I, $N — bound by the caller via vars), the
// functions $string, $substring and $append, the "&" concatenation
// operator, the arithmetic operators + - * / %, the comparison
// operators = != < <= > >= (strict — no string↔number coercion),
// unary minus, parentheses, and array/object literals.
// Anything else fails at
// compile time so deployment errors loudly rather than diverging
// silently.

// CompileSeqExpr validates a sequence expression without evaluating it;
// the error is non-nil exactly when evaluation would fail to parse.
func CompileSeqExpr(src string) error {
	_, err := compileSeqExpr(src)
	return err
}

// EvalSeqExpr evaluates src against ctx — a map[string]interface{}
// whose keys form the root scope (for message evaluation: "payload",
// "topic", "parts", …). vars binds the $-variables ($A accumulator,
// $I index, $N count). A path that does not resolve evaluates to
// "undefined" (nil), matching JSONata semantics.
func EvalSeqExpr(src string, ctx interface{}, vars map[string]interface{}) (interface{}, error) {
	fn, err := compileSeqExpr(src)
	if err != nil {
		return nil, err
	}
	v, _ := fn(ctx, vars)
	return v, nil
}

// seqExprFunc is a compiled expression: it returns the value and
// whether it is defined (undefined propagates through operators).
type seqExprFunc func(ctx interface{}, vars map[string]interface{}) (interface{}, bool)

func compileSeqExpr(src string) (seqExprFunc, error) {
	p := &seqExprParser{src: strings.TrimSpace(src)}
	fn, err := p.parseCompare()
	if err != nil {
		return nil, err
	}
	p.skipSpace()
	if p.pos != len(p.src) {
		return nil, fmt.Errorf("unexpected trailing input %q", p.src[p.pos:])
	}
	return fn, nil
}

// seqExprParser is a recursive-descent parser over the sequence
// expression subset.
type seqExprParser struct {
	src string
	pos int
}

func (p *seqExprParser) skipSpace() {
	for p.pos < len(p.src) && (p.src[p.pos] == ' ' || p.src[p.pos] == '\t' || p.src[p.pos] == '\n' || p.src[p.pos] == '\r') {
		p.pos++
	}
}

// parseCompare parses a comparison: <concat> (=|!=|<|<=|>|>=) <concat>.
// Comparisons are non-associative; an undefined operand makes the
// whole comparison undefined.
func (p *seqExprParser) parseCompare() (seqExprFunc, error) {
	left, err := p.parseConcat()
	if err != nil {
		return nil, err
	}
	p.skipSpace()
	op := ""
	if p.pos < len(p.src) {
		switch p.src[p.pos] {
		case '=':
			op = "="
			p.pos++
		case '!':
			if p.pos+1 < len(p.src) && p.src[p.pos+1] == '=' {
				op = "!="
				p.pos += 2
			}
		case '<':
			if p.pos+1 < len(p.src) && p.src[p.pos+1] == '=' {
				op = "<="
				p.pos += 2
			} else {
				op = "<"
				p.pos++
			}
		case '>':
			if p.pos+1 < len(p.src) && p.src[p.pos+1] == '=' {
				op = ">="
				p.pos += 2
			} else {
				op = ">"
				p.pos++
			}
		}
	}
	if op == "" {
		return left, nil
	}
	right, err := p.parseConcat()
	if err != nil {
		return nil, err
	}
	l, r := left, right
	return func(ctx interface{}, vars map[string]interface{}) (interface{}, bool) {
		av, ok := l(ctx, vars)
		if !ok {
			return nil, false
		}
		bv, ok := r(ctx, vars)
		if !ok {
			return nil, false
		}
		switch op {
		case "=", "!=":
			eq := seqExprEqual(av, bv)
			if op == "!=" {
				eq = !eq
			}
			return eq, true
		}
		cmp, ok := seqExprOrder(av, bv)
		if !ok {
			return nil, false
		}
		switch op {
		case "<":
			return cmp < 0, true
		case "<=":
			return cmp <= 0, true
		case ">":
			return cmp > 0, true
		case ">=":
			return cmp >= 0, true
		}
		return nil, false
	}, nil
}

// seqExprEqual is the strict equality the sequence-expression subset
// uses for "=" — no string↔number coercion: booleans compare to
// booleans, strings to strings, numerics numerically, composites
// structurally.
func seqExprEqual(a, b interface{}) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if ab, ok := a.(bool); ok {
		bb, ok2 := b.(bool)
		return ok2 && ab == bb
	}
	if _, ok := b.(bool); ok {
		return false
	}
	if as, ok := a.(string); ok {
		bs, ok2 := b.(string)
		return ok2 && as == bs
	}
	if _, ok := b.(string); ok {
		return false
	}
	if af, err := ToFloat(a); err == nil {
		if bf, err2 := ToFloat(b); err2 == nil {
			return af == bf
		}
		return false
	}
	return reflect.DeepEqual(a, b)
}

// seqExprOrder compares two values for the ordering operators:
// strings lexically, numerics numerically; any other pairing is not
// comparable (ok is false — the comparison evaluates undefined).
func seqExprOrder(a, b interface{}) (int, bool) {
	if as, ok := a.(string); ok {
		bs, ok2 := b.(string)
		if !ok2 {
			return 0, false
		}
		switch {
		case as < bs:
			return -1, true
		case as > bs:
			return 1, true
		}
		return 0, true
	}
	if !seqExprIsNumber(a) || !seqExprIsNumber(b) {
		return 0, false
	}
	af, _ := ToFloat(a)
	bf, _ := ToFloat(b)
	switch {
	case af < bf:
		return -1, true
	case af > bf:
		return 1, true
	}
	return 0, true
}

// seqExprIsNumber reports whether v is a genuine JSON numeric type —
// strings and booleans never count for ordering.
func seqExprIsNumber(v interface{}) bool {
	switch v.(type) {
	case float64, float32, int, int64, int32, json.Number:
		return true
	}
	return false
}

// parseConcat parses "&"-joined additions (lowest precedence).
func (p *seqExprParser) parseConcat() (seqExprFunc, error) {
	left, err := p.parseAdd()
	if err != nil {
		return nil, err
	}
	for {
		p.skipSpace()
		if p.pos >= len(p.src) || p.src[p.pos] != '&' {
			return left, nil
		}
		p.pos++
		right, err := p.parseAdd()
		if err != nil {
			return nil, err
		}
		l, r := left, right
		left = func(ctx interface{}, vars map[string]interface{}) (interface{}, bool) {
			av, ok := l(ctx, vars)
			if !ok {
				return nil, false
			}
			bv, ok := r(ctx, vars)
			if !ok {
				return nil, false
			}
			return JSString(av) + JSString(bv), true
		}
	}
}

// parseAdd parses + and - terms.
func (p *seqExprParser) parseAdd() (seqExprFunc, error) {
	left, err := p.parseMul()
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
		right, err := p.parseMul()
		if err != nil {
			return nil, err
		}
		left = seqExprArithmetic(left, right, op)
	}
}

// parseMul parses * / % factors.
func (p *seqExprParser) parseMul() (seqExprFunc, error) {
	left, err := p.parseUnary()
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
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left = seqExprArithmetic(left, right, op)
	}
}

// seqExprArithmetic combines two operands numerically; an undefined or
// non-numeric operand makes the result undefined.
func seqExprArithmetic(a, b seqExprFunc, op byte) seqExprFunc {
	return func(ctx interface{}, vars map[string]interface{}) (interface{}, bool) {
		av, ok := a(ctx, vars)
		if !ok {
			return nil, false
		}
		bv, ok := b(ctx, vars)
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

// parseUnary parses unary minus and primaries.
func (p *seqExprParser) parseUnary() (seqExprFunc, error) {
	p.skipSpace()
	if p.pos < len(p.src) && p.src[p.pos] == '-' {
		p.pos++
		fn, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return func(ctx interface{}, vars map[string]interface{}) (interface{}, bool) {
			v, ok := fn(ctx, vars)
			if !ok {
				return nil, false
			}
			f, err := ToFloat(v)
			if err != nil {
				return nil, false
			}
			return -f, true
		}, nil
	}
	return p.parsePrimary()
}

func (p *seqExprParser) parsePrimary() (seqExprFunc, error) {
	p.skipSpace()
	if p.pos >= len(p.src) {
		return nil, fmt.Errorf("unexpected end of expression")
	}
	c := p.src[p.pos]
	switch {
	case c == '(':
		p.pos++
		fn, err := p.parseCompare()
		if err != nil {
			return nil, err
		}
		p.skipSpace()
		if p.pos >= len(p.src) || p.src[p.pos] != ')' {
			return nil, fmt.Errorf("missing closing parenthesis")
		}
		p.pos++
		return fn, nil
	case c == '"' || c == '\'':
		return p.parseString()
	case c == '$':
		return p.parseDollar()
	case c == '[':
		return p.parseArray()
	case c == '{':
		return p.parseObject()
	case c >= '0' && c <= '9' || c == '.':
		return p.parseNumber()
	case isIdentStart(c):
		return p.parsePathOrKeyword()
	}
	return nil, fmt.Errorf("unsupported syntax at %q", p.src[p.pos:])
}

func isIdentStart(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_'
}

func isIdentChar(c byte) bool {
	return isIdentStart(c) || c >= '0' && c <= '9'
}

// parseNumber reads a numeric literal.
func (p *seqExprParser) parseNumber() (seqExprFunc, error) {
	start := p.pos
	for p.pos < len(p.src) && (p.src[p.pos] >= '0' && p.src[p.pos] <= '9' || p.src[p.pos] == '.') {
		p.pos++
	}
	f, err := strconv.ParseFloat(p.src[start:p.pos], 64)
	if err != nil {
		return nil, fmt.Errorf("invalid number %q", p.src[start:p.pos])
	}
	return func(ctx interface{}, vars map[string]interface{}) (interface{}, bool) { return f, true }, nil
}

// parseString reads a quoted literal; the only escapes honored are
// \" and \' (a backslash escapes the following character verbatim).
func (p *seqExprParser) parseString() (seqExprFunc, error) {
	quote := p.src[p.pos]
	p.pos++
	var sb strings.Builder
	for p.pos < len(p.src) {
		c := p.src[p.pos]
		if c == '\\' && p.pos+1 < len(p.src) {
			switch p.src[p.pos+1] {
			case 'n':
				sb.WriteByte('\n')
			case 't':
				sb.WriteByte('\t')
			case 'r':
				sb.WriteByte('\r')
			default:
				sb.WriteByte(p.src[p.pos+1])
			}
			p.pos += 2
			continue
		}
		if c == quote {
			p.pos++
			s := sb.String()
			return func(ctx interface{}, vars map[string]interface{}) (interface{}, bool) { return s, true }, nil
		}
		sb.WriteByte(c)
		p.pos++
	}
	return nil, fmt.Errorf("unterminated string")
}

// parseArray reads an array literal [expr, …].
func (p *seqExprParser) parseArray() (seqExprFunc, error) {
	p.pos++ // consume '['
	var elems []seqExprFunc
	for {
		p.skipSpace()
		if p.pos < len(p.src) && p.src[p.pos] == ']' {
			p.pos++
			break
		}
		fn, err := p.parseConcat()
		if err != nil {
			return nil, err
		}
		elems = append(elems, fn)
		p.skipSpace()
		if p.pos >= len(p.src) {
			return nil, fmt.Errorf("unterminated array literal")
		}
		if p.src[p.pos] == ',' {
			p.pos++
			continue
		}
		if p.src[p.pos] == ']' {
			p.pos++
			break
		}
		return nil, fmt.Errorf("expected ',' or ']' at %q", p.src[p.pos:])
	}
	return func(ctx interface{}, vars map[string]interface{}) (interface{}, bool) {
		out := make([]interface{}, 0, len(elems))
		for _, fn := range elems {
			v, ok := fn(ctx, vars)
			if !ok {
				v = nil
			}
			out = append(out, v)
		}
		return out, true
	}, nil
}

// parseObject reads an object literal {"key": expr, …}. Keys may be
// quoted or bare identifiers.
func (p *seqExprParser) parseObject() (seqExprFunc, error) {
	p.pos++ // consume '{'
	type kv struct {
		key string
		fn  seqExprFunc
	}
	var pairs []kv
	for {
		p.skipSpace()
		if p.pos < len(p.src) && p.src[p.pos] == '}' {
			p.pos++
			break
		}
		var key string
		if p.pos < len(p.src) && (p.src[p.pos] == '"' || p.src[p.pos] == '\'') {
			kfn, err := p.parseString()
			if err != nil {
				return nil, err
			}
			kv0, _ := kfn(nil, nil)
			key, _ = kv0.(string)
		} else {
			start := p.pos
			for p.pos < len(p.src) && isIdentChar(p.src[p.pos]) {
				p.pos++
			}
			key = p.src[start:p.pos]
		}
		if key == "" {
			return nil, fmt.Errorf("expected object key at %q", p.src[p.pos:])
		}
		p.skipSpace()
		if p.pos >= len(p.src) || p.src[p.pos] != ':' {
			return nil, fmt.Errorf("expected ':' after object key %q", key)
		}
		p.pos++
		fn, err := p.parseConcat()
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, kv{key: key, fn: fn})
		p.skipSpace()
		if p.pos >= len(p.src) {
			return nil, fmt.Errorf("unterminated object literal")
		}
		if p.src[p.pos] == ',' {
			p.pos++
			continue
		}
		if p.src[p.pos] == '}' {
			p.pos++
			break
		}
		return nil, fmt.Errorf("expected ',' or '}' at %q", p.src[p.pos:])
	}
	return func(ctx interface{}, vars map[string]interface{}) (interface{}, bool) {
		out := make(map[string]interface{}, len(pairs))
		for _, pair := range pairs {
			v, ok := pair.fn(ctx, vars)
			if !ok {
				continue // JSONata drops keys whose value is undefined
			}
			out[pair.key] = v
		}
		return out, true
	}, nil
}

// parseDollar reads $-references: a bound variable ($A, $I, $N) or a
// function call ($string(…), $substring(…), $append(…)).
func (p *seqExprParser) parseDollar() (seqExprFunc, error) {
	p.pos++ // consume '$'
	start := p.pos
	for p.pos < len(p.src) && isIdentChar(p.src[p.pos]) {
		p.pos++
	}
	name := p.src[start:p.pos]
	if name == "" {
		return nil, fmt.Errorf("expected identifier after '$'")
	}
	p.skipSpace()
	if p.pos >= len(p.src) || p.src[p.pos] != '(' {
		// Bare variable reference: resolve from vars.
		return func(ctx interface{}, vars map[string]interface{}) (interface{}, bool) {
			v, ok := vars[name]
			return v, ok
		}, nil
	}
	// Function call.
	p.pos++
	var args []seqExprFunc
	for {
		p.skipSpace()
		if p.pos < len(p.src) && p.src[p.pos] == ')' {
			p.pos++
			break
		}
		fn, err := p.parseConcat()
		if err != nil {
			return nil, err
		}
		args = append(args, fn)
		p.skipSpace()
		if p.pos >= len(p.src) {
			return nil, fmt.Errorf("unterminated call to $%s", name)
		}
		if p.src[p.pos] == ',' {
			p.pos++
			continue
		}
		if p.src[p.pos] == ')' {
			p.pos++
			break
		}
		return nil, fmt.Errorf("expected ',' or ')' at %q", p.src[p.pos:])
	}
	switch name {
	case "string":
		if len(args) != 1 {
			return nil, fmt.Errorf("$string takes exactly one argument")
		}
		return func(ctx interface{}, vars map[string]interface{}) (interface{}, bool) {
			v, ok := args[0](ctx, vars)
			if !ok {
				return nil, false
			}
			return JSString(v), true
		}, nil
	case "substring":
		if len(args) < 2 || len(args) > 3 {
			return nil, fmt.Errorf("$substring takes two or three arguments")
		}
		return func(ctx interface{}, vars map[string]interface{}) (interface{}, bool) {
			sv, ok := args[0](ctx, vars)
			if !ok {
				return nil, false
			}
			iv, ok := args[1](ctx, vars)
			if !ok {
				return nil, false
			}
			startF, err := ToFloat(iv)
			if err != nil {
				return nil, false
			}
			runes := []rune(JSString(sv))
			start := int(startF)
			if start < 0 {
				start = len(runes) + start
			}
			if start < 0 {
				start = 0
			}
			if start > len(runes) {
				start = len(runes)
			}
			end := len(runes)
			if len(args) == 3 {
				lv, ok := args[2](ctx, vars)
				if !ok {
					return nil, false
				}
				lf, err := ToFloat(lv)
				if err != nil {
					return nil, false
				}
				end = start + int(lf)
				if end < 0 {
					end = 0
				}
				if end > len(runes) {
					end = len(runes)
				}
			}
			if end < start {
				end = start
			}
			return string(runes[start:end]), true
		}, nil
	case "append":
		if len(args) != 2 {
			return nil, fmt.Errorf("$append takes exactly two arguments")
		}
		return func(ctx interface{}, vars map[string]interface{}) (interface{}, bool) {
			av, ok := args[0](ctx, vars)
			if !ok {
				return nil, false
			}
			bv, ok := args[1](ctx, vars)
			if !ok {
				return nil, false
			}
			var out []interface{}
			if arr, isArr := av.([]interface{}); isArr {
				out = append(out, arr...)
			} else if av != nil {
				out = append(out, av)
			}
			if arr, isArr := bv.([]interface{}); isArr {
				out = append(out, arr...)
			} else if bv != nil {
				out = append(out, bv)
			}
			return out, true
		}, nil
	}
	return nil, fmt.Errorf("unsupported function $%s", name)
}

// parsePathOrKeyword reads true/false/null literals or a dotted
// path resolved against the context object.
func (p *seqExprParser) parsePathOrKeyword() (seqExprFunc, error) {
	var segs []string
	for {
		start := p.pos
		for p.pos < len(p.src) && isIdentChar(p.src[p.pos]) {
			p.pos++
		}
		segs = append(segs, p.src[start:p.pos])
		if p.pos < len(p.src) && p.src[p.pos] == '.' {
			p.pos++
			continue
		}
		break
	}
	if len(segs) == 1 {
		switch segs[0] {
		case "true":
			return func(ctx interface{}, vars map[string]interface{}) (interface{}, bool) { return true, true }, nil
		case "false":
			return func(ctx interface{}, vars map[string]interface{}) (interface{}, bool) { return false, true }, nil
		case "null":
			return func(ctx interface{}, vars map[string]interface{}) (interface{}, bool) { return nil, true }, nil
		}
	}
	return func(ctx interface{}, vars map[string]interface{}) (interface{}, bool) {
		cur := ctx
		for _, seg := range segs {
			m, ok := cur.(map[string]interface{})
			if !ok {
				return nil, false
			}
			v, ok := m[seg]
			if !ok {
				return nil, false
			}
			cur = v
		}
		return cur, true
	}, nil
}
