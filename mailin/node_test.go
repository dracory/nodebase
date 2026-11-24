package mailin

// Pack-local unit tests for the mailIn node — config defaults,
// RFC822 body parsing (plain, multipart, attachments) and
// validation. The IMAP transport needs a server (integration gap).

import (
	"strings"
	"testing"
	"time"

	nodestore "github.com/dracory/nodestore"
)

func newNode(cfg *MailInConfig) *MailInNode {
	return NewMailInNode(nodestore.NewNode("m1", "mailIn").SetWorkflowID("wf"), cfg, nil)
}

func TestConfigDefaults(t *testing.T) {
	c := &MailInConfig{}
	if !c.markSeen() {
		t.Fatal("markSeen should default true")
	}
	off := false
	c.MarkSeen = &off
	if c.markSeen() {
		t.Fatal("explicit markSeen=false not honored")
	}
	c = &MailInConfig{}
	if c.mailboxOrDefault() != "INBOX" {
		t.Fatal("mailbox default wrong")
	}
	c.Mailbox = "Archive"
	if c.mailboxOrDefault() != "Archive" {
		t.Fatal("mailbox override wrong")
	}
	c = &MailInConfig{}
	if c.pollInterval() != 30*time.Second {
		t.Fatal("poll default wrong")
	}
	c.PollSeconds = 5
	if c.pollInterval() != 5*time.Second {
		t.Fatal("poll override wrong")
	}
}

func TestParseMailBodyPlain(t *testing.T) {
	raw := "From: a@x\r\nSubject: hi\r\nContent-Type: text/plain\r\n\r\nplain body"
	body, atts := parseMailBody(strings.NewReader(raw))
	if strings.TrimSpace(body) != "plain body" {
		t.Fatalf("plain body wrong: %q", body)
	}
	if len(atts) != 0 {
		t.Fatalf("unexpected attachments: %v", atts)
	}
}

func TestParseMailBodyMultipart(t *testing.T) {
	raw := "From: a@x\r\nContent-Type: multipart/mixed; boundary=BB\r\n\r\n" +
		"--BB\r\nContent-Type: text/plain\r\n\r\nplain part\r\n" +
		"--BB\r\nContent-Type: text/html\r\n\r\n<b>html</b>\r\n" +
		"--BB\r\nContent-Type: application/pdf\r\n" +
		"Content-Disposition: attachment; filename=\"doc.pdf\"\r\n\r\nPDFDATA\r\n" +
		"--BB--\r\n"
	body, atts := parseMailBody(strings.NewReader(raw))
	if strings.TrimSpace(body) != "plain part" {
		t.Fatalf("multipart body wrong: %q", body)
	}
	if len(atts) != 1 {
		t.Fatalf("attachments wrong: %v", atts)
	}
	att := atts[0].(map[string]interface{})
	if att["name"] != "doc.pdf" || att["size"].(int) != len("PDFDATA") {
		t.Fatalf("attachment meta wrong: %v", att)
	}
}

func TestParseMailBodyHTMLFallback(t *testing.T) {
	raw := "From: a@x\r\nContent-Type: multipart/alternative; boundary=BB\r\n\r\n" +
		"--BB\r\nContent-Type: text/html\r\n\r\n<b>only html</b>\r\n--BB--\r\n"
	body, _ := parseMailBody(strings.NewReader(raw))
	if !strings.Contains(body, "only html") {
		t.Fatalf("html fallback wrong: %q", body)
	}
}

func TestParseMailBodyGarbage(t *testing.T) {
	body, _ := parseMailBody(strings.NewReader("not a mail \x00\x01"))
	if body != "not a mail \x00\x01" {
		t.Fatalf("raw fallback wrong: %q", body)
	}
}

func TestMailInValidate(t *testing.T) {
	if err := newNode(&MailInConfig{Host: "imap.x", Username: "u"}).Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if err := newNode(&MailInConfig{Username: "u"}).Validate(); err == nil {
		t.Fatal("missing host should fail")
	}
	if err := newNode(&MailInConfig{Host: "imap.x"}).Validate(); err == nil {
		t.Fatal("missing username should fail")
	}
	if err := newNode(&MailInConfig{Host: "h", Username: "u", Security: "bogus"}).Validate(); err == nil {
		t.Fatal("bad security should fail")
	}
}

func TestMailInStopKey(t *testing.T) {
	n := newNode(&MailInConfig{})
	if n.StopKey() == "" {
		t.Fatal("StopKey missing")
	}
}
