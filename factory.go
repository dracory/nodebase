package nodebase

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	nodestore "github.com/dracory/nodestore"
)

// DecodeConfig maps the node's stored Data map onto the typed config
// struct via JSON — the decoding every built-in factory applies.
func DecodeConfig(node nodestore.NodeInterface, cfg interface{}) error {
	// Node-RED serializes some numeric/boolean config fields as strings
	// ("30", "false") — coerce them to the struct's declared types so
	// imported flows decode the same way editor-produced ones do.
	data := node.GetData()
	coerceConfigStrings(data, reflect.TypeOf(cfg))
	raw, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("invalid %s node data: %w", node.GetType(), err)
	}
	if err := json.Unmarshal(raw, cfg); err != nil {
		return fmt.Errorf("invalid %s config: %w", node.GetType(), err)
	}
	return nil
}

// coerceConfigStrings mutates data in place, converting string values to
// bools/numbers where the target cfg struct declares bool/number fields.
// Node-RED serializes editor fields as strings ("true", "30", "1880") that
// would otherwise fail strict JSON decoding of imported flows.
func coerceConfigStrings(data map[string]any, cfgType reflect.Type) {
	if cfgType == nil || cfgType.Kind() != reflect.Pointer || len(data) == 0 {
		return
	}
	t := cfgType.Elem()
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return
	}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if name == "-" {
			continue
		}
		if name == "" {
			name = f.Name
		}
		v, ok := data[name]
		if !ok {
			continue
		}
		if s, isStr := v.(string); isStr {
			if s == "" && f.Type.Kind() != reflect.String {
				// Node-RED serializes unset numeric/bool editor fields
				// as "" — drop the key so the struct gets its zero value.
				delete(data, name)
				continue
			}
			switch f.Type.Kind() {
			case reflect.Bool:
				if b, err := strconv.ParseBool(s); err == nil {
					data[name] = b
				}
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				if n, err := strconv.ParseInt(s, 10, 64); err == nil {
					data[name] = n
				}
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				if n, err := strconv.ParseUint(s, 10, 64); err == nil {
					data[name] = n
				}
			case reflect.Float32, reflect.Float64:
				if n, err := strconv.ParseFloat(s, 64); err == nil {
					data[name] = n
				}
			}
			continue
		}
		// Reverse direction: a bool in the data for a string field is
		// Node-RED's "checkbox vs value" dual-use (e.g. exec addpay) —
		// drop the key rather than fail or inject "true"/"false".
		if _, isBool := v.(bool); isBool && f.Type.Kind() == reflect.String {
			delete(data, name)
			continue
		}
		if f.Type.Kind() == reflect.Slice || f.Type.Kind() == reflect.Struct {
			coerceConfigStringsChild(v, f.Type)
		}
	}
}

// coerceConfigStringsChild recurses into slices and nested structs so
// string-typed fields inside arrays of rule objects also coerce.
func coerceConfigStringsChild(v any, t reflect.Type) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Slice:
		elem := t.Elem()
		arr, ok := v.([]any)
		if !ok {
			return
		}
		for _, item := range arr {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			coerceConfigStrings(m, reflect.PointerTo(elem))
		}
	case reflect.Struct:
		if m, ok := v.(map[string]any); ok {
			coerceConfigStrings(m, reflect.PointerTo(t))
		}
	}
}
