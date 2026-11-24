package nodebase

import (
	"sort"
	"strings"
	"sync/atomic"
)

// splitGroupSeq generates unique group ids for each split operation.
var groupSeq int64

// sortedKeys returns the map keys in deterministic (sorted) order
func SortedKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// unescapeDelim converts literal escape sequences ("\n", "\t", "\r",
// "\0", "\\") stored in the config into real characters
func UnescapeDelim(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' || i+1 >= len(s) {
			b.WriteByte(s[i])
			continue
		}
		i++
		switch s[i] {
		case 'n':
			b.WriteByte('\n')
		case 't':
			b.WriteByte('\t')
		case 'r':
			b.WriteByte('\r')
		case '0':
			b.WriteByte(0)
		case '\\':
			b.WriteByte('\\')
		default:
			b.WriteByte('\\')
			b.WriteByte(s[i])
		}
	}
	return b.String()
}

// NextGroupSeq returns the next group sequence number — unique ids
// for split-family operations (split, csv) that fan one message out
// into a numbered group.
func NextGroupSeq() int64 {
	return atomic.AddInt64(&groupSeq, 1)
}
