package comment

// Pack-local unit tests for the comment node — a metadata-only
// annotation whose only contract is pass-through + valid deploy.

import (
	"context"
	"testing"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

func TestCommentNode(t *testing.T) {
	n := NewCommentNode(nodestore.NewNode("c1", "comment"), &CommentConfig{Info: "note"})
	in := nodestore.NewMessage().SetPayload("x")
	out, err := n.Execute(context.Background(), in)
	if err != nil || out != in {
		t.Fatal("comment should pass through")
	}
	if err := n.Validate(); err != nil {
		t.Fatalf("comment should always validate: %v", err)
	}
}

func TestCommentFactoryDecodesInfo(t *testing.T) {
	reg, ok := nodebase.RegisteredNode("comment")
	if !ok {
		t.Fatal("comment not registered")
	}
	executor, err := reg.Factory(nodestore.NewNode("c2", "comment").SetData(map[string]interface{}{
		"info": "hello canvas",
	}), nil)
	if err != nil {
		t.Fatalf("factory failed: %v", err)
	}
	node, ok := executor.(*CommentNode)
	if !ok || node.config.Info != "hello canvas" {
		t.Fatalf("info not decoded: %+v", node)
	}
}
