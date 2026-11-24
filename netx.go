package nodebase

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	nodestore "github.com/dracory/nodestore"
	"time"
)

// Network-node helpers shared across the tcp/udp/http folders.

// TCPPayloadBytes converts the message Data into bytes to send:
// strings pass through, []byte as-is, anything else as JSON with a
// trailing newline.
func TCPPayloadBytes(input nodestore.MessageInterface) ([]byte, error) {
	if input == nil || input.GetPayload() == nil {
		return nil, errors.New("tcpOut requires message data")
	}
	switch v := input.GetPayload().(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	default:
		raw, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("tcpOut cannot serialize data: %w", err)
		}
		return append(raw, '\n'), nil
	}
}

// UnescapeTCP decodes the common escape sequences config fields accept
// (same convention as the split node's delimiter).
func UnescapeTCP(s string) string {
	r := strings.NewReplacer(`\n`, "\n", `\r`, "\r", `\t`, "\t", `\0`, "\x00")
	return r.Replace(s)
}

// MuxPattern converts a Node-RED-style path with ":name" segments into
// a Go 1.22 ServeMux pattern with "{name}" wildcards, returning the
// pattern and the parameter names in order.
func MuxPattern(path string) (string, []string) {
	segments := strings.Split(path, "/")
	var params []string
	for i, seg := range segments {
		if strings.HasPrefix(seg, ":") && len(seg) > 1 {
			params = append(params, seg[1:])
			segments[i] = "{" + seg[1:] + "}"
		}
	}
	return strings.Join(segments, "/"), params
}

// ReadHeaderTimeout guards against slowloris clients (stalled headers).
// A package var so tests can shorten it.
var ReadHeaderTimeout = 10 * time.Second
