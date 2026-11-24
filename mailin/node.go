// Package mailin contains the mailIn node implementation.
package mailin

import (
	"context"
	"crypto/tls"
	_ "embed"
	"errors"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	imap "github.com/emersion/go-imap"
	imapclient "github.com/emersion/go-imap/client"
	"io"
	"mime"
	"mime/multipart"
	"net"
	netmail "net/mail"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(MailInConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewMailInNode(n, c, e), nil
}

// init registers the mailIn node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "mailIn",
			Name:        "Mail In",
			Description: "Watches an IMAP mailbox and emits one message per received mail",
			Icon:        "envelope-open",
			Category:    "Network",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the mailIn node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"host": map[string]string{
			"type":        "string",
			"title":       "Host",
			"description": "IMAP server host",
		},
		"port": map[string]interface{}{
			"type":        "integer",
			"title":       "Port",
			"description": "IMAP port (0 = default for security: 993 tls, 143 starttls/plain)",
			"default":     0,
		},
		"security": map[string]interface{}{
			"type":    "string",
			"title":   "Security",
			"enum":    []string{"tls", "starttls", "plain"},
			"default": "tls",
		},
		"mailbox": map[string]interface{}{
			"type":    "string",
			"title":   "Mailbox",
			"default": "INBOX",
		},
		"pollSeconds": map[string]interface{}{
			"type":        "integer",
			"title":       "Poll Interval (s)",
			"description": "Fallback poll interval in seconds; IDLE pushes wake the node sooner",
			"default":     30,
		},
		"useIdle": map[string]interface{}{
			"type":        "boolean",
			"title":       "Use IDLE",
			"description": "Use IMAP IDLE between polls when the server supports it",
			"default":     true,
		},
		"markSeen": map[string]interface{}{
			"type":        "boolean",
			"title":       "Mark Seen",
			"description": "Set the \\Seen flag on delivered messages (default on)",
			"default":     true,
		},
		"delete": map[string]interface{}{
			"type":        "boolean",
			"title":       "Delete",
			"description": "Expunge delivered messages from the mailbox",
			"default":     false,
		},
		"username": map[string]interface{}{
			"type":        "string",
			"format":      "credential",
			"title":       "Username",
			"description": "Mailbox username (credential: masked, never exported)",
		},
		"password": map[string]interface{}{
			"type":        "string",
			"format":      "credential",
			"title":       "Password",
			"description": "Mailbox password (credential: masked, never exported)",
		},
	},
	"required": []string{"host", "username"},
}

// MailInConfig holds configuration for the mailIn node.
type MailInConfig struct {
	// Host is the IMAP server host.
	Host string `json:"host"`
	// Port is the IMAP server port. Zero picks the default for the
	// security mode: 993 (tls), 143 (starttls/plain).
	Port int `json:"port"`
	// Security selects the transport: "tls" (implicit TLS on connect,
	// the default), "starttls" or "plain".
	Security string `json:"security"`
	// Mailbox is the folder to watch (default INBOX).
	Mailbox string `json:"mailbox"`
	// PollSeconds is the fallback poll interval in seconds (default 30).
	// With useIdle the node also wakes on server push.
	PollSeconds int `json:"pollSeconds"`
	// UseIdle uses IMAP IDLE between polls when the server supports it.
	UseIdle bool `json:"useIdle"`
	// MarkSeen sets the \Seen flag on delivered messages (default on
	// unless explicitly disabled).
	MarkSeen *bool `json:"markSeen"`
	// Delete expunges delivered messages from the mailbox.
	Delete bool `json:"delete"`
	// Username/Password are the mailbox credentials. They are
	// credential-typed fields: stored outside the exported flow
	// document and merged back in at deploy time.
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

// markSeen reports whether delivered messages get the \Seen flag
// (default true; only an explicit false disables it).
func (c *MailInConfig) markSeen() bool {
	return c.MarkSeen == nil || *c.MarkSeen
}

// mailboxOrDefault returns the configured mailbox or INBOX.
func (c *MailInConfig) mailboxOrDefault() string {
	if strings.TrimSpace(c.Mailbox) == "" {
		return "INBOX"
	}
	return c.Mailbox
}

// pollInterval returns the fallback poll interval.
func (c *MailInConfig) pollInterval() time.Duration {
	if c.PollSeconds <= 0 {
		return 30 * time.Second
	}
	return time.Duration(c.PollSeconds) * time.Second
}

// MailInNode watches an IMAP mailbox and emits one message per new
// mail. It is a Stopper: connect/login happens synchronously on deploy
// so an unreachable server surfaces as a deploy error, then a poll
// goroutine fetches arrivals (UID-tracked, so restarts re-emit only
// messages the mailbox still holds for it).
type MailInNode struct {
	*nodebase.BaseNode
	config     *MailInConfig
	host       nodebase.NodeHost
	workflowID string
	client     *imapclient.Client
	stopCh     chan struct{}
	doneCh     chan struct{}
	watching   atomic.Bool
	stopOnce   sync.Once
}

// Execute connects, logs in and starts the mailbox watch loop. The
// initial connect is synchronous so failures surface as deploy errors,
// matching mqttIn.
func (n *MailInNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	client, err := n.dial()
	if err != nil {
		return nil, err
	}
	n.client = client

	if _, err := n.fetchNew(0); err != nil {
		_ = client.Logout()
		return nil, fmt.Errorf("mailIn initial fetch failed: %w", err)
	}

	n.watching.Store(true)
	go n.loop()
	return nodestore.NewMessage().SetPayload(map[string]interface{}{
		"status":  "watching",
		"host":    n.config.Host,
		"mailbox": n.config.mailboxOrDefault(),
	}).SetTimestamp(time.Now()), nil
}

// dial connects to the server per the security mode and logs in.
func (n *MailInNode) dial() (*imapclient.Client, error) {
	port := n.config.Port
	var c *imapclient.Client
	var err error
	switch n.config.Security {
	case "", "tls":
		if port == 0 {
			port = 993
		}
		c, err = imapclient.DialTLS(net.JoinHostPort(n.config.Host, strconv.Itoa(port)), nil)
	case "starttls":
		if port == 0 {
			port = 143
		}
		c, err = imapclient.Dial(net.JoinHostPort(n.config.Host, strconv.Itoa(port)))
		if err == nil {
			err = c.StartTLS(&tls.Config{ServerName: n.config.Host})
		}
	case "plain":
		if port == 0 {
			port = 143
		}
		c, err = imapclient.Dial(net.JoinHostPort(n.config.Host, strconv.Itoa(port)))
	default:
		return nil, fmt.Errorf("invalid mailIn security %q (tls|starttls|plain)", n.config.Security)
	}
	if err != nil {
		return nil, fmt.Errorf("mailIn cannot connect to %s:%d: %w", n.config.Host, port, err)
	}
	c.Timeout = 30 * time.Second
	if err := c.Login(n.config.Username, n.config.Password); err != nil {
		_ = c.Logout()
		return nil, fmt.Errorf("mailIn login failed: %w", err)
	}
	return c, nil
}

// loop fetches new mail, then sleeps (or IDLEs) until the next cycle.
func (n *MailInNode) loop() {
	defer close(n.doneCh)
	var lastUID uint32
	for {
		uid, err := n.fetchNew(lastUID)
		if err != nil {
			if n.stopped() {
				return
			}
			n.host.EmitEvent(n.workflowID, n.GetID(), "mailIn", "error", "fetch failed: "+err.Error())
			if !n.sleep(n.config.pollInterval()) {
				return
			}
			continue
		}
		lastUID = uid
		if n.stopped() {
			return
		}
		if n.config.UseIdle {
			n.idle()
		} else if !n.sleep(n.config.pollInterval()) {
			return
		}
	}
}

// stopped reports whether Stop has been called.
func (n *MailInNode) stopped() bool {
	select {
	case <-n.stopCh:
		return true
	default:
		return false
	}
}

// sleep waits for d or Stop; false means stop.
func (n *MailInNode) sleep(d time.Duration) bool {
	select {
	case <-n.stopCh:
		return false
	case <-time.After(d):
		return true
	}
}

// idle blocks until the server pushes an update, the poll fallback
// fires, or Stop closes stopCh. go-imap falls back to NOOP polling when
// the server lacks the IDLE capability.
func (n *MailInNode) idle() {
	if err := n.client.Idle(n.stopCh, &imapclient.IdleOptions{
		PollInterval: n.config.pollInterval(),
	}); err != nil && !n.stopped() {
		n.host.EmitEvent(n.workflowID, n.GetID(), "mailIn", "warn", "idle failed: "+err.Error())
	}
}

// bodySection is the fetch item for the whole RFC822 message.
var bodySection = &imap.BodySectionName{}

// fetchNew selects the mailbox and emits every message with UID >
// lastUID (unseen-only when markSeen is on), flagging or deleting them
// afterwards per config. Returns the new lastUID.
func (n *MailInNode) fetchNew(lastUID uint32) (uint32, error) {
	mbox := n.config.mailboxOrDefault()
	if _, err := n.client.Select(mbox, false); err != nil {
		return lastUID, fmt.Errorf("select %s failed: %w", mbox, err)
	}

	criteria := imap.NewSearchCriteria()
	uidSet := new(imap.SeqSet)
	uidSet.AddRange(lastUID+1, 0) // 0 = "*"
	criteria.Uid = uidSet
	if n.config.markSeen() {
		criteria.WithoutFlags = []string{imap.SeenFlag}
	}
	uids, err := n.client.UidSearch(criteria)
	if err != nil {
		return lastUID, fmt.Errorf("search failed: %w", err)
	}
	if len(uids) == 0 {
		return lastUID, nil
	}

	fetchSet := new(imap.SeqSet)
	fetchSet.AddNum(uids...)
	messages := make(chan *imap.Message, len(uids))
	items := []imap.FetchItem{imap.FetchEnvelope, imap.FetchUid, bodySection.FetchItem()}
	if err := n.client.UidFetch(fetchSet, items, messages); err != nil {
		return lastUID, fmt.Errorf("fetch failed: %w", err)
	}

	var flagSet *imap.SeqSet
	if n.config.markSeen() || n.config.Delete {
		flagSet = new(imap.SeqSet)
	}
	newLast := lastUID
	for msg := range messages {
		if msg.Uid > newLast {
			newLast = msg.Uid
		}
		n.emitMessage(msg)
		if flagSet != nil {
			flagSet.AddNum(msg.Uid)
		}
	}

	if flagSet != nil && !flagSet.Empty() {
		flag := imap.SeenFlag
		if n.config.Delete {
			flag = imap.DeletedFlag
		}
		item := imap.FormatFlagsOp(imap.AddFlags, true)
		if err := n.client.UidStore(flagSet, item, []interface{}{flag}, nil); err != nil {
			n.host.EmitEvent(n.workflowID, n.GetID(), "mailIn", "warn", "flag update failed: "+err.Error())
		}
		if n.config.Delete {
			if err := n.client.Expunge(nil); err != nil {
				n.host.EmitEvent(n.workflowID, n.GetID(), "mailIn", "warn", "expunge failed: "+err.Error())
			}
		}
	}
	return newLast, nil
}

// emitMessage converts one fetched IMAP message into a nodestore.MessageInterface
// and pushes it downstream.
func (n *MailInNode) emitMessage(msg *imap.Message) {
	meta := map[string]interface{}{"uid": msg.Uid}
	if msg.Envelope != nil {
		meta["topic"] = msg.Envelope.Subject
		meta["messageId"] = msg.Envelope.MessageId
		meta["date"] = msg.Envelope.Date.Format(time.RFC3339)
		meta["from"] = imapAddrs(msg.Envelope.From)
		meta["to"] = imapAddrs(msg.Envelope.To)
	}

	var body string
	if r := msg.GetBody(bodySection); r != nil {
		var attachments []interface{}
		body, attachments = parseMailBody(r)
		if len(attachments) > 0 {
			meta["attachments"] = attachments
		}
	}

	_ = n.host.Emit(n.workflowID, n.GetID(), nodestore.NewMessage().SetPayload(body).SetMetadata(meta).SetTimestamp(time.Now()), nil)
}

// imapAddrs renders an address list as a comma-separated string.
func imapAddrs(addrs []*imap.Address) string {
	var out []string
	for _, a := range addrs {
		out = append(out, a.Address())
	}
	return strings.Join(out, ", ")
}

// parseMailBody extracts the body text (first text/plain part, else
// first text/* part) and attachment metadata {name, size} entries.
func parseMailBody(r io.Reader) (string, []interface{}) {
	raw, err := io.ReadAll(r)
	if err != nil {
		return "", nil
	}
	m, err := netmail.ReadMessage(strings.NewReader(string(raw)))
	if err != nil {
		// Not parseable as RFC822 — hand back the raw bytes as text.
		return string(raw), nil
	}

	mediaType, params, err := mime.ParseMediaType(m.Header.Get("Content-Type"))
	if err != nil || !strings.HasPrefix(mediaType, "multipart/") {
		body, _ := io.ReadAll(m.Body)
		return string(body), nil
	}

	var body string
	var htmlBody string
	var attachments []interface{}
	mr := multipart.NewReader(m.Body, params["boundary"])
	for {
		part, err := mr.NextPart()
		if err != nil {
			break
		}
		data, _ := io.ReadAll(part)
		ctype := part.Header.Get("Content-Type")
		name := part.FileName()
		if name != "" || strings.HasPrefix(part.Header.Get("Content-Disposition"), "attachment") {
			if name == "" {
				name = "part"
			}
			attachments = append(attachments, map[string]interface{}{
				"name": name, "size": len(data),
			})
			continue
		}
		if strings.HasPrefix(ctype, "text/plain") || ctype == "" {
			if body == "" {
				body = string(data)
			}
			continue
		}
		if strings.HasPrefix(ctype, "text/html") && htmlBody == "" {
			htmlBody = string(data)
		}
	}
	if body == "" {
		body = htmlBody
	}
	return body, attachments
}

// Stop logs out and stops the watch loop.
func (n *MailInNode) Stop() error {
	var err error
	n.stopOnce.Do(func() {
		close(n.stopCh)
		// Wait for the watch loop before logging out — the go-imap
		// client is not safe for concurrent use, so Logout must not
		// overlap an in-flight fetch. Only wait when Execute actually
		// started the loop — a failed activation leaves doneCh open
		// forever.
		if n.watching.Load() {
			<-n.doneCh
		}
		if n.client != nil {
			err = n.client.Logout()
		}
	})
	return err
}

// stopKey dedupes stopper registration across partial redeploys: one
// watch loop per host+mailbox.
func (n *MailInNode) StopKey() string {
	return fmt.Sprintf("mailIn:%s:%d:%s:%s", n.config.Host, n.config.Port,
		n.config.Username, n.config.mailboxOrDefault())
}

// Validate checks the mailIn node configuration
func (n *MailInNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(n.config.Host) == "" {
		return errors.New("mailIn requires a host")
	}
	if strings.TrimSpace(n.config.Username) == "" {
		return errors.New("mailIn requires a username")
	}
	switch n.config.Security {
	case "", "tls", "starttls", "plain":
		return nil
	default:
		return fmt.Errorf("invalid mailIn security %q (tls|starttls|plain)", n.config.Security)
	}
}

// NewMailInNode creates a new mailIn node
func NewMailInNode(node nodestore.NodeInterface, config *MailInConfig, host nodebase.NodeHost) *MailInNode {
	return &MailInNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
	}
}

// Execute connects, logs in and starts the mailbox watch loop. The
// initial connect is synchronous so failures surface as deploy errors,
// matching mqttIn.
