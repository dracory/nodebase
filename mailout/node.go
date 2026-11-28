// Package mailout contains the mailOut node implementation.
package mailout

import (
	"context"
	"crypto/tls"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"mime"
	"net"
	"net/smtp"
	"strconv"
	"strings"
	"time"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(MailOutConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewMailOutNode(n, c), nil
}

// init registers the mailOut node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "mailOut",
			Name:        "Mail Out",
			Description: "Sends the message as an e-mail via SMTP",
			Icon:        "paper-plane",
			Category:    "Network",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the mailOut node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"host": map[string]string{
			"type":        "string",
			"title":       "Host",
			"description": "SMTP server host",
		},
		"port": map[string]interface{}{
			"type":        "integer",
			"title":       "Port",
			"description": "SMTP port (0 = default for security: 465 tls, 587 starttls, 25 plain)",
			"default":     0,
		},
		"security": map[string]interface{}{
			"type":    "string",
			"title":   "Security",
			"enum":    []string{"tls", "starttls", "plain"},
			"default": "starttls",
		},
		"from": map[string]string{
			"type":        "string",
			"title":       "From",
			"description": "Sender address; the message's mail.from metadata overrides it",
		},
		"to": map[string]string{
			"type":        "string",
			"title":       "To",
			"description": "Comma-separated recipients; the message's to or mail.to metadata overrides it",
		},
		"cc": map[string]string{
			"type":        "string",
			"title":       "Cc",
			"description": "Comma-separated cc recipients; message cc/mail.cc metadata overrides",
		},
		"bcc": map[string]string{
			"type":        "string",
			"title":       "Bcc",
			"description": "Comma-separated bcc recipients; message bcc/mail.bcc metadata overrides",
		},
		"subject": map[string]string{
			"type":        "string",
			"title":       "Subject",
			"description": "Mail subject; the message's topic or mail.subject metadata overrides it",
		},
		"html": map[string]interface{}{
			"type":        "boolean",
			"title":       "HTML Body",
			"description": "Send the payload as text/html instead of text/plain",
			"default":     false,
		},
		"username": map[string]interface{}{
			"type":        "string",
			"format":      "credential",
			"title":       "Username",
			"description": "Optional SMTP-AUTH username (credential: masked, never exported)",
		},
		"password": map[string]interface{}{
			"type":        "string",
			"format":      "credential",
			"title":       "Password",
			"description": "Optional SMTP-AUTH password (credential: masked, never exported)",
		},
		"errorCatch": nodebase.ErrorCatchSchemaField(),
	},
	"required": []string{"host", "from"},
}

// MailOutConfig holds configuration for the mailOut node.
type MailOutConfig struct {
	// Host is the SMTP server host.
	Host string `json:"host"`
	// Port is the SMTP server port. Zero picks the default for the
	// security mode: 465 (tls), 587 (starttls), 25 (plain).
	Port int `json:"port"`
	// Security selects the transport: "tls" (implicit TLS on connect),
	// "starttls" (upgrade after connect, the default) or "plain".
	Security string `json:"security"`
	// From is the envelope/header sender address.
	From string `json:"from"`
	// To/Cc/Bcc are the default recipients; message metadata overrides.
	To  string `json:"to"`
	Cc  string `json:"cc"`
	Bcc string `json:"bcc"`
	// Subject is the default subject; the message's topic metadata
	// (or mail.subject) overrides it.
	Subject string `json:"subject"`
	// HTML marks the body Content-Type text/html instead of text/plain.
	HTML bool `json:"html"`
	// Username/Password are optional SMTP-AUTH credentials. They are
	// credential-typed fields: stored outside the exported flow
	// document and merged back in at deploy time.
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

// portOrDefault returns the configured port or the conventional one for
// the security mode.
func (c *MailOutConfig) portOrDefault() int {
	if c.Port != 0 {
		return c.Port
	}
	switch c.Security {
	case "tls":
		return 465
	case "plain":
		return 25
	default:
		return 587
	}
}

// MailOutNode sends the incoming message as an e-mail via SMTP and
// passes the input through unchanged.
type MailOutNode struct {
	*nodebase.BaseNode
	config *MailOutConfig
}

// Execute builds the e-mail from config + message overrides and sends
// it through the configured server.
func (n *MailOutNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	fields := n.messageFields(input)

	if len(fields.recipients) == 0 {
		return nil, errors.New("mailOut requires at least one recipient (config to/cc/bcc or message mail.to override)")
	}
	if fields.from == "" {
		return nil, errors.New("mailOut requires a from address (config or message mail.from override)")
	}

	raw, err := buildRFC822(fields)
	if err != nil {
		return nil, err
	}

	if err := smtpSend(n.config.Host, n.config.portOrDefault(), n.config.Security,
		n.config.Username, n.config.Password, fields.from, fields.recipients, raw); err != nil {
		return nil, err
	}
	return input, nil
}

// mailMessage is the resolved set of header/body fields for one send.
type mailMessage struct {
	from       string
	to         []string
	cc         []string
	bcc        []string
	recipients []string // to + cc + bcc
	subject    string
	body       []byte
	html       bool
}

// messageFields merges the static config with message overrides:
// metadata keys to/cc/bcc/from/topic, and a metadata "mail" object
// with to/cc/bcc/from/subject keys (Node-RED contrib parity).
func (n *MailOutNode) messageFields(input nodestore.MessageInterface) *mailMessage {
	m := &mailMessage{
		from:    strings.TrimSpace(n.config.From),
		to:      splitAddrs(n.config.To),
		cc:      splitAddrs(n.config.Cc),
		bcc:     splitAddrs(n.config.Bcc),
		subject: n.config.Subject,
		html:    n.config.HTML,
	}
	if input == nil {
		m.recipients = append(append(append([]string{}, m.to...), m.cc...), m.bcc...)
		return m
	}

	meta := input.GetMetadata()
	if mail, ok := meta["mail"].(map[string]interface{}); ok {
		overrideAddrs(&m.to, mail["to"])
		overrideAddrs(&m.cc, mail["cc"])
		overrideAddrs(&m.bcc, mail["bcc"])
		if v := strOf(mail["from"]); v != "" {
			m.from = v
		}
		if v := strOf(mail["subject"]); v != "" {
			m.subject = v
		}
	}
	overrideAddrs(&m.to, meta["to"])
	overrideAddrs(&m.cc, meta["cc"])
	overrideAddrs(&m.bcc, meta["bcc"])
	if v := strOf(meta["from"]); v != "" {
		m.from = v
	}
	if v := strOf(meta["topic"]); v != "" {
		m.subject = v
	}

	m.recipients = append(append(append([]string{}, m.to...), m.cc...), m.bcc...)

	switch v := input.GetPayload().(type) {
	case string:
		m.body = []byte(v)
	case []byte:
		m.body = v
	case nil:
		m.body = []byte{}
	default:
		if raw, err := json.Marshal(v); err == nil {
			m.body = raw
		} else {
			m.body = []byte(fmt.Sprint(v))
		}
	}
	return m
}

// strOf returns the string value of v, or "".
func strOf(v interface{}) string {
	s, _ := v.(string)
	return s
}

// splitAddrs splits a comma/semicolon separated address list.
func splitAddrs(s string) []string {
	var out []string
	for _, a := range strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ';' }) {
		if a = strings.TrimSpace(a); a != "" {
			out = append(out, a)
		}
	}
	return out
}

// overrideAddrs replaces the recipient list when the override carries a
// non-empty string or string list.
func overrideAddrs(dst *[]string, v interface{}) {
	switch t := v.(type) {
	case string:
		if list := splitAddrs(t); len(list) > 0 {
			*dst = list
		}
	case []interface{}:
		var list []string
		for _, e := range t {
			if s := strOf(e); s != "" {
				list = append(list, s)
			}
		}
		if len(list) > 0 {
			*dst = list
		}
	}
}

// buildRFC822 renders headers + body. Header values are UTF-8; the
// Subject is RFC 2047 encoded when it contains non-ASCII characters.
func buildRFC822(m *mailMessage) ([]byte, error) {
	var b strings.Builder
	b.WriteString("From: " + m.from + "\r\n")
	if len(m.to) > 0 {
		b.WriteString("To: " + strings.Join(m.to, ", ") + "\r\n")
	}
	if len(m.cc) > 0 {
		b.WriteString("Cc: " + strings.Join(m.cc, ", ") + "\r\n")
	}
	b.WriteString("Subject: " + mime.QEncoding.Encode("utf-8", m.subject) + "\r\n")
	b.WriteString("Date: " + time.Now().Format("Mon, 02 Jan 2006 15:04:05 -0700") + "\r\n")
	b.WriteString("MIME-Version: 1.0\r\n")
	if m.html {
		b.WriteString("Content-Type: text/html; charset=utf-8\r\n")
	} else {
		b.WriteString("Content-Type: text/plain; charset=utf-8\r\n")
	}
	b.WriteString("\r\n")
	b.Write(m.body)
	return []byte(b.String()), nil
}

// smtpSend connects to host:port with the requested security mode,
// optionally authenticates and delivers raw to every recipient.
func smtpSend(host string, port int, security, username, password, from string, rcpts []string, raw []byte) error {
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	var c *smtp.Client
	switch security {
	case "tls":
		conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 10 * time.Second}, "tcp", addr,
			&tls.Config{ServerName: host}) //nolint:gosec -- server-verified TLS
		if err != nil {
			return fmt.Errorf("mailOut cannot connect to %s: %w", addr, err)
		}
		client, err := smtp.NewClient(conn, host)
		if err != nil {
			_ = conn.Close()
			return fmt.Errorf("mailOut cannot connect to %s: %w", addr, err)
		}
		c = client
	default:
		client, err := smtp.Dial(addr)
		if err != nil {
			return fmt.Errorf("mailOut cannot connect to %s: %w", addr, err)
		}
		c = client
	}
	defer func() { _ = c.Close() }()

	// starttls fails closed: a server that does not advertise the
	// extension must never receive credentials over cleartext
	// (STARTTLS-stripping MITM). Plaintext is opt-in via "plain".
	if security == "starttls" || security == "" {
		if ok, _ := c.Extension("STARTTLS"); !ok {
			return fmt.Errorf("mailOut: %s does not advertise STARTTLS (use security \"plain\" to send anyway)", addr)
		}
		if err := c.StartTLS(&tls.Config{ServerName: host}); err != nil {
			return fmt.Errorf("mailOut STARTTLS failed: %w", err)
		}
	}

	if username != "" {
		if ok, mechs := c.Extension("AUTH"); ok {
			auth := smtp.Auth(smtp.PlainAuth("", username, password, host))
			if !hasAuthMech(mechs, "PLAIN") && hasAuthMech(mechs, "LOGIN") {
				auth = &loginAuth{username: username, password: password}
			}
			if err := c.Auth(auth); err != nil {
				return fmt.Errorf("mailOut auth failed: %w", err)
			}
		}
	}

	if err := c.Mail(from); err != nil {
		return fmt.Errorf("mailOut MAIL FROM rejected: %w", err)
	}
	for _, rcpt := range rcpts {
		if err := c.Rcpt(rcpt); err != nil {
			return fmt.Errorf("mailOut RCPT TO %q rejected: %w", rcpt, err)
		}
	}
	wc, err := c.Data()
	if err != nil {
		return fmt.Errorf("mailOut DATA rejected: %w", err)
	}
	if _, err := wc.Write(raw); err != nil {
		_ = wc.Close()
		return fmt.Errorf("mailOut body write failed: %w", err)
	}
	if err := wc.Close(); err != nil {
		return fmt.Errorf("mailOut DATA close failed: %w", err)
	}
	_ = c.Quit()
	return nil
}

// hasAuthMech reports whether the AUTH extension parameter (a
// space-separated mechanism list) advertises mech.
func hasAuthMech(mechs, mech string) bool {
	for _, m := range strings.Fields(mechs) {
		if strings.EqualFold(m, mech) {
			return true
		}
	}
	return false
}

// loginAuth implements the SMTP AUTH LOGIN mechanism, which stdlib
// net/smtp lacks. The server sends base64 "Username:"/"Password:"
// challenges; the client answers each with the base64-encoded secret
// (net/smtp performs the response encoding).
type loginAuth struct {
	username string
	password string
}

// Start begins a LOGIN exchange with no initial response.
func (a *loginAuth) Start(server *smtp.ServerInfo) (string, []byte, error) {
	return "LOGIN", nil, nil
}

// Next answers each server challenge. fromServer is the base64 text
// after "334 "; it is decoded to distinguish the username prompt from
// the password prompt, then the matching secret is returned.
func (a *loginAuth) Next(fromServer []byte, more bool) ([]byte, error) {
	if !more {
		return nil, nil
	}
	prompt := string(fromServer)
	if decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(prompt)); err == nil {
		prompt = string(decoded)
	}
	if strings.Contains(strings.ToLower(prompt), "user") {
		return []byte(a.username), nil
	}
	return []byte(a.password), nil
}

// Validate checks the mailOut node configuration
func (n *MailOutNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(n.config.Host) == "" {
		return errors.New("mailOut requires a host")
	}
	switch n.config.Security {
	case "", "tls", "starttls", "plain":
		return nil
	default:
		return fmt.Errorf("invalid mailOut security %q (tls|starttls|plain)", n.config.Security)
	}
}

// ---------------------------------------------------------------------------
// mailIn — IMAP receive
// ---------------------------------------------------------------------------

// NewMailOutNode creates a new mailOut node
func NewMailOutNode(node nodestore.NodeInterface, config *MailOutConfig) *MailOutNode {
	return &MailOutNode{
		BaseNode: nodebase.NewBaseNode(node),
		config:   config,
	}
}

// Execute builds the e-mail from config + message overrides and sends
// it through the configured server.
