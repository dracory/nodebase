package nodebase

import (
	nodestore "github.com/dracory/nodestore"
)

// JSONSource resolves the source value JSON-family nodes read from
// the message (payload or a dotted payload path).

// jsonSource resolves the value to convert. When property is "payload"
// (or empty) the whole Data is used; otherwise the dotted path is read
// inside the data map. Returns the value plus whether it lives on Data.
func JSONSource(input nodestore.MessageInterface, property string) (interface{}, bool) {
	if property == "payload" {
		return input.GetPayload(), true
	}
	if m, ok := input.GetPayload().(map[string]interface{}); ok {
		if v, found := GetPath(m, property); found {
			return v, false
		}
	}
	return nil, false
}
