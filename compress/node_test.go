package compress

import (
	"bytes"
	"context"
	"strings"
	"testing"

	nodestore "github.com/dracory/nodestore"
)

func testNode(operation, algorithm, property string) *Node {
	return NewNode(nodestore.NewNode("c1", "compress"), &Config{
		Operation: operation, Algorithm: algorithm, Property: property,
	})
}

func TestCompressRoundTripAlgorithms(t *testing.T) {
	for _, algorithm := range []string{"gzip", "zlib", "flate", "deflate"} {
		t.Run(algorithm, func(t *testing.T) {
			ctx := context.Background()
			original := []byte{0, 1, 2, 0xff, 'h', 'e', 'l', 'l', 'o'}
			compressedMsg, err := testNode("compress", algorithm, "payload").Execute(ctx,
				nodestore.NewMessage().SetPayload(original))
			if err != nil {
				t.Fatalf("compress: %v", err)
			}
			compressed, ok := compressedMsg.GetPayload().([]byte)
			if !ok || len(compressed) == 0 || bytes.Equal(compressed, original) {
				t.Fatalf("compressed payload wrong: %#v (%T)", compressedMsg.GetPayload(), compressedMsg.GetPayload())
			}
			meta := compressedMsg.GetMetadata()["compression"].(map[string]interface{})
			if meta["algorithm"] != "flate" && algorithm == "deflate" {
				t.Fatalf("deflate must normalize to flate metadata: %v", meta)
			}
			if meta["originalSize"] != len(original) || meta["resultSize"] != len(compressed) {
				t.Fatalf("size metadata wrong: %v", meta)
			}

			decompressedMsg, err := testNode("decompress", algorithm, "payload").Execute(ctx, compressedMsg)
			if err != nil {
				t.Fatalf("decompress: %v", err)
			}
			decompressed, ok := decompressedMsg.GetPayload().([]byte)
			if !ok || !bytes.Equal(decompressed, original) {
				t.Fatalf("round trip = %#v (%T), want original bytes", decompressedMsg.GetPayload(), decompressedMsg.GetPayload())
			}
		})
	}
}

func TestCompressConfiguredNestedProperty(t *testing.T) {
	ctx := context.Background()
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{
		"file": map[string]interface{}{"content": "hello"},
	})
	out, err := testNode("compress", "gzip", "file.content").Execute(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	nested := out.GetPayload().(map[string]interface{})["file"].(map[string]interface{})
	if _, ok := nested["content"].([]byte); !ok {
		t.Fatalf("nested output is %T", nested["content"])
	}
	back, err := testNode("decompress", "gzip", "file.content").Execute(ctx, out)
	if err != nil {
		t.Fatal(err)
	}
	got := back.GetPayload().(map[string]interface{})["file"].(map[string]interface{})["content"].([]byte)
	if string(got) != "hello" {
		t.Fatalf("nested round trip wrong: %q", got)
	}
}

func TestDecompressInvalidInputErrors(t *testing.T) {
	_, err := testNode("decompress", "gzip", "payload").Execute(context.Background(),
		nodestore.NewMessage().SetPayload([]byte("not gzip")))
	if err == nil || !strings.Contains(err.Error(), "invalid gzip input") {
		t.Fatalf("expected invalid gzip input error, got %v", err)
	}
}

func TestCompressUnsupportedInputAndConfigErrors(t *testing.T) {
	_, err := testNode("compress", "gzip", "payload").Execute(context.Background(),
		nodestore.NewMessage().SetPayload(func() {}))
	if err == nil || !strings.Contains(err.Error(), "unsupported payload type") {
		t.Fatalf("expected unsupported payload error, got %v", err)
	}
	if _, err := testNode("compress", "brotli", "payload").Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("unsupported algorithm should error")
	}
	if _, err := testNode("missing", "gzip", "payload").Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("unsupported operation should error")
	}
	if _, err := testNode("compress", "gzip", "missing").Execute(context.Background(), nodestore.NewMessage().SetPayload("x")); err == nil {
		t.Fatal("missing property should error")
	}
}

func TestFactoryValidation(t *testing.T) {
	node := nodestore.NewNode("c1", "compress").SetData(map[string]interface{}{
		"operation": "compress", "algorithm": "snappy", "property": "payload",
	})
	if _, err := factory(node, nil); err == nil || !strings.Contains(err.Error(), "unsupported algorithm") {
		t.Fatalf("factory should reject unsupported algorithm: %v", err)
	}
}
