package mailout

// Pack-local unit tests for the mailOut node — RFC822 rendering,
// recipient/subject merge semantics and port defaults. The SMTP
// transport itself needs a server (integration gap); its dial error
// path is still verified.

import (
	"context"
	"strings"
	"testing"

	nodestore "github.com/dracory/nodestore"
)

func newNode(cfg *MailOutConfig) *MailOutNode {
	return NewMailOutNode(nodestore.NewNode("m1", "mailOut").SetWorkflowID("wf"), cfg)
}

func TestPortOrDefault(t *testing.T) {
	if (&MailOutConfig{Port: 2525}).portOrDefault() != 2525 {
		t.Fatal("explicit port not honored")
	}
	if (&MailOutConfig{Security: "tls"}).portOrDefault() != 465 {
		t.Fatal("tls default wrong")
	}
	if (&MailOutConfig{Security: "plain"}).portOrDefault() != 25 {
		t.Fatal("plain default wrong")
	}
	if (&MailOutConfig{}).portOrDefault() != 587 {
		t.Fatal("starttls default wrong")
	}
}

func TestSplitAndOverrideAddrs(t *testing.T) {
	got := splitAddrs(" a@x, b@y ; ,c@z ")
	if len(got) != 3 || got[0] != "a@x" || got[2] != "c@z" {
		t.Fatalf("splitAddrs wrong: %v", got)
	}
	var dst []string
	overrideAddrs(&dst, "")
	if dst != nil {
		t.Fatal("empty override must not clobber")
	}
	dst = []string{"orig@x"}
	overrideAddrs(&dst, "new@x, other@y")
	if len(dst) != 2 || dst[0] != "new@x" {
		t.Fatalf("string override wrong: %v", dst)
	}
	overrideAddrs(&dst, []interface{}{"list@x", 42})
	if len(dst) != 1 || dst[0] != "list@x" {
		t.Fatalf("list override wrong: %v", dst)
	}
	overrideAddrs(&dst, []interface{}{})
	if dst[0] != "list@x" {
		t.Fatal("empty list override must not clobber")
	}
}

func TestMessageFieldsMerge(t *testing.T) {
	n := newNode(&MailOutConfig{
		Host: "smtp.x", From: "me@x", To: "a@x", Subject: "cfg subj",
	})
	// config-only
	m := n.messageFields(nodestore.NewMessage().SetPayload("hi"))
	if m.from != "me@x" || len(m.recipients) != 1 || m.subject != "cfg subj" || string(m.body) != "hi" {
		t.Fatalf("config merge wrong: %+v", m)
	}
	// message overrides
	in := nodestore.NewMessage().SetPayload("body").SetMetadata(map[string]interface{}{
		"topic": "msg subj",
		"to":    "b@x",
		"mail":  map[string]interface{}{"cc": "c@x", "subject": "mail subj"},
	})
	m = n.messageFields(in)
	if m.subject != "msg subj" || m.to[0] != "b@x" || m.cc[0] != "c@x" {
		t.Fatalf("message override wrong: %+v", m)
	}
	if len(m.recipients) != 2 {
		t.Fatalf("recipients union wrong: %v", m.recipients)
	}
	// non-string payload serializes
	m = n.messageFields(nodestore.NewMessage().SetPayload(map[string]interface{}{"k": 1}))
	if string(m.body) != `{"k":1}` {
		t.Fatalf("json body wrong: %q", m.body)
	}
	// nil input still works
	m = n.messageFields(nil)
	if len(m.recipients) != 1 {
		t.Fatal("nil input merge wrong")
	}
}

func TestBuildRFC822(t *testing.T) {
	raw, err := buildRFC822(&mailMessage{
		from:    "me@x",
		to:      []string{"a@x", "b@y"},
		cc:      []string{"c@z"},
		subject: "Grüße",
		body:    []byte("<p>hi</p>"),
		html:    true,
	})
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	s := string(raw)
	for _, want := range []string{
		"From: me@x", "To: a@x, b@y", "Cc: c@z",
		"MIME-Version: 1.0", "text/html", "<p>hi</p>",
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("message missing %q:\n%s", want, s)
		}
	}
	// non-ASCII subject is RFC2047-encoded
	if !strings.Contains(s, "Subject: =?utf-8?") {
		t.Fatalf("subject not encoded: %s", s)
	}
	// no Bcc header — bcc recipients get the mail but stay hidden
	raw2, _ := buildRFC822(&mailMessage{
		from: "me@x", bcc: []string{"hidden@x"}, subject: "s", body: []byte("b"),
	})
	if strings.Contains(string(raw2), "Bcc:") {
		t.Fatal("Bcc header must not appear")
	}
	if !strings.Contains(string(raw2), "text/plain") {
		t.Fatal("plain content type missing")
	}
}

func TestMailOutExecuteErrors(t *testing.T) {
	// no recipients at all
	n := newNode(&MailOutConfig{Host: "smtp.x", From: "me@x"})
	if _, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("x")); err == nil {
		t.Fatal("missing recipients should fail")
	}
	// no from
	n2 := newNode(&MailOutConfig{Host: "smtp.x", To: "a@x"})
	if _, err := n2.Execute(context.Background(), nodestore.NewMessage().SetPayload("x")); err == nil {
		t.Fatal("missing from should fail")
	}
	// unreachable server surfaces the dial error
	n3 := newNode(&MailOutConfig{Host: "127.0.0.1", Port: 1, Security: "plain", From: "me@x", To: "a@x"})
	if _, err := n3.Execute(context.Background(), nodestore.NewMessage().SetPayload("x")); err == nil {
		t.Fatal("unreachable smtp should fail")
	}
}
