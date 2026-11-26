package hash

import (
	"context"
	"strings"
	"testing"

	nodestore "github.com/dracory/nodestore"
)

func testNode(algorithm, source, target, encoding string) *Node {
	return NewNode(nodestore.NewNode("h1", "hash"), &Config{
		Algorithm: algorithm, Source: source, Target: target, Encoding: encoding,
	})
}

func TestHashKnownVectors(t *testing.T) {
	vectors := map[string]map[string]string{
		"sha256": {
			"hex":    "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
			"base64": "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=",
		},
		"sha1": {
			"hex":    "a9993e364706816aba3e25717850c26c9cd0d89d",
			"base64": "qZk+NkcGgWq6PiVxeFDCbJzQ2J0=",
		},
		"sha512": {
			"hex":    "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f",
			"base64": "3a81oZNherrMQXNJriBBMRLm+k6JqX6iCp7u5ktV05ohkpkqJ0/BqDa6PCOj/uu9RU1EI2Q86A4qmslPpUyknw==",
		},
		"md5": {
			"hex":    "900150983cd24fb0d6963f7d28e17f72",
			"base64": "kAFQmDzST7DWlj99KOF/cg==",
		},
	}
	for algorithm, encodings := range vectors {
		for encoding, want := range encodings {
			t.Run(algorithm+"/"+encoding, func(t *testing.T) {
				out, err := testNode(algorithm, "payload", "payload", encoding).Execute(context.Background(),
					nodestore.NewMessage().SetPayload("abc"))
				if err != nil {
					t.Fatal(err)
				}
				if out.GetPayload() != want {
					t.Fatalf("hash = %q, want %q", out.GetPayload(), want)
				}
			})
		}
	}
}

func TestHashPayloadAndMetadataTargets(t *testing.T) {
	ctx := context.Background()
	payloadOut, err := testNode("sha256", "payload", "payload", "hex").Execute(ctx,
		nodestore.NewMessage().SetPayload([]byte("abc")))
	if err != nil {
		t.Fatal(err)
	}
	if payloadOut.GetPayload() != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" {
		t.Fatalf("payload target wrong: %#v", payloadOut.GetPayload())
	}

	metaOut, err := testNode("sha256", "payload", "msg.attributes.digest", "base64").Execute(ctx,
		nodestore.NewMessage().SetPayload([]byte("abc")))
	if err != nil {
		t.Fatal(err)
	}
	attrs := metaOut.GetMetadata()["attributes"].(map[string]interface{})
	if attrs["digest"] != "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=" {
		t.Fatalf("metadata target wrong: %#v", attrs)
	}
	if metaOut.GetPayload().([]byte)[0] != 'a' {
		t.Fatal("metadata target must not replace payload")
	}
}

func TestHashNestedSourceAndDeterministicJSON(t *testing.T) {
	ctx := context.Background()
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{
		"file": map[string]interface{}{"content": "abc"},
	})
	out, err := testNode("sha256", "payload.file.content", "msg.digest", "hex").Execute(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	if out.GetMetadata()["digest"] != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" {
		t.Fatalf("nested source hash wrong: %#v", out.GetMetadata()["digest"])
	}

	objectOut, err := testNode("sha256", "payload", "payload", "hex").Execute(ctx,
		nodestore.NewMessage().SetPayload(map[string]interface{}{"b": []interface{}{true, "x"}, "a": 1}))
	if err != nil {
		t.Fatal(err)
	}
	if objectOut.GetPayload() != "63e8063d9dc6f0fd5a24b4706818a165fd57c3531b74466cf5dea62bff09b0b6" {
		t.Fatalf("canonical JSON hash wrong: %#v", objectOut.GetPayload())
	}
}

func TestHashValidationErrors(t *testing.T) {
	if _, err := testNode("sha256", "missing", "payload", "hex").Execute(context.Background(), nodestore.NewMessage().SetPayload("x")); err == nil {
		t.Fatal("missing source should error")
	}
	_, err := testNode("sha256", "payload", "payload", "hex").Execute(context.Background(),
		nodestore.NewMessage().SetPayload(func() {}))
	if err == nil || !strings.Contains(err.Error(), "unsupported source type") {
		t.Fatalf("unsupported source should error: %v", err)
	}
	if _, err := testNode("sha3", "payload", "payload", "hex").Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("unsupported algorithm should error")
	}
	if _, err := testNode("sha256", "payload", "payload", "binary").Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("unsupported encoding should error")
	}
}

func TestFactoryValidation(t *testing.T) {
	node := nodestore.NewNode("h1", "hash").SetData(map[string]interface{}{
		"algorithm": "sha256", "source": "payload", "target": "msg.hash", "encoding": "base32",
	})
	if _, err := factory(node, nil); err == nil || !strings.Contains(err.Error(), "unsupported output encoding") {
		t.Fatalf("factory should reject unsupported encoding: %v", err)
	}
}
