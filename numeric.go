package nodebase

import (
	"math"
	"strconv"
	"strings"

	"github.com/spf13/cast"
)

// ToFloat parses a payload value as a float64
func ToFloat(value interface{}) (float64, error) {
	return cast.ToFloat64E(value)
}

// ParseNumeric coerces a value to a number with JS Number() semantics.
// Strings are trimmed (whitespace + BOM) and honour 0x/0o/0b radix
// prefixes before delegating to cast; everything else defers to
// cast.ToFloat64E. The second return value is false when the value
// cannot be coerced (NaN).
func ParseNumeric(value interface{}) (float64, bool) {
	if s, ok := value.(string); ok {
		return ParseNumericString(s)
	}
	f, err := cast.ToFloat64E(value)
	if err != nil || math.IsNaN(f) {
		return 0, false
	}
	return f, true
}

// ParseNumericString parses a string like JS Number(): trims whitespace
// and a leading BOM, honours 0x/0o/0b radix prefixes, otherwise defers
// to standard float parsing.
func ParseNumericString(s string) (float64, bool) {
	s = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(s), "\ufeff"))
	if s == "" {
		return 0, false
	}
	lower := strings.ToLower(s)
	negative := false
	if strings.HasPrefix(lower, "-") {
		negative = true
		lower = lower[1:]
	} else if strings.HasPrefix(lower, "+") {
		lower = lower[1:]
	}
	parse := func(digits string, base int) (float64, bool) {
		if digits == "" {
			return 0, false
		}
		n, err := strconv.ParseInt(digits, base, 64)
		if err != nil {
			return 0, false
		}
		f := float64(n)
		if negative {
			f = -f
		}
		return f, true
	}
	switch {
	case strings.HasPrefix(lower, "0x"):
		return parse(lower[2:], 16)
	case strings.HasPrefix(lower, "0o"):
		return parse(lower[2:], 8)
	case strings.HasPrefix(lower, "0b"):
		return parse(lower[2:], 2)
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(f) {
		return 0, false
	}
	return f, true
}
