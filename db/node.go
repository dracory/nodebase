// Package db contains the db node implementation.
package db

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"github.com/dracory/neat"
	neatdriver "github.com/dracory/neat/database/driver"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"os"
	"strings"
	"sync"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(DBConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewDBNode(n, c, e), nil
}

// init registers the db node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "db",
			Name:        "DB",
			Description: "Parameterized SQL query/exec on a neat DSN (sqlite/postgres/mysql/...)",
			Icon:        "database",
			Category:    "Storage",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the db node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"dsn": map[string]interface{}{
			"type":   "string",
			"title":  "DSN",
			"format": "credential",
			"description": "Neat-style database DSN (sqlite://file.db, postgres://user:pass@host/db, " +
				"mysql://...). Bare paths and :memory: are treated as SQLite. " +
				"\"env:NAME\" resolves the DSN from the scoped env chain. " +
				"Credential: masked, stored outside the exported flow document",
		},
		"operation": map[string]interface{}{
			"type":    "string",
			"title":   "Operation",
			"enum":    []string{"query", "exec"},
			"default": "query",
		},
		"sql": map[string]interface{}{
			"type":   "string",
			"format": "textarea",
			"title":  "SQL",
			"description": "Statement with ? bind placeholders (rebound to the driver's native " +
				"style — $n for postgres, @pn for sqlserver). Params bind; values are never " +
				"interpolated into the SQL string",
		},
		"params": map[string]interface{}{
			"type":        "string",
			"title":       "Params Field",
			"description": "Optional dotted msg field path resolving to a bind-args array (e.g. \"params\"); empty uses the message's params data field",
		},
		"split": map[string]interface{}{
			"type":        "boolean",
			"title":       "Split Rows",
			"description": "Query only: emit one message per result row instead of one array payload",
			"default":     false,
		},
		"readOnly": map[string]interface{}{
			"type":        "boolean",
			"title":       "Read Only",
			"description": "Reject exec operations at deploy time",
			"default":     false,
		},
		"errorCatch": nodebase.ErrorCatchSchemaField(),
	},
}

func init() {
	// The DSN can carry a password — keep it out of exported flow
	// documents like every other secret field.
	nodestore.RegisterCredentialKeys("db", []string{"dsn"})
}

// DBConfig holds the node.Data fields of a db node.
type DBConfig struct {
	// DSN is a neat-style database DSN ("sqlite://file.db",
	// "postgres://user:pass@host/db", "mysql://user:pass@host/db").
	// Bare paths and ":memory:" are treated as SQLite DSNs.
	// "env:NAME" resolves the DSN from the scoped env chain.
	DSN string `json:"dsn"`
	// Operation is "query" (default) or "exec".
	Operation string `json:"operation"`
	// SQL is the statement with '?' bind placeholders (rebound to
	// the driver's native style). Values are never interpolated.
	SQL string `json:"sql"`
	// Params supplies bind values: a JSON array of literals, or a
	// string naming a dotted msg field path that resolves to an
	// array (e.g. "params", "payload.items", "msg.args"). Empty
	// falls back to an array at the message's "params" data field.
	Params interface{} `json:"params"`
	// Split emits one message per result row (query only).
	Split bool `json:"split"`
	// ReadOnly rejects exec operations at deploy time.
	ReadOnly bool `json:"readOnly"`
}

// dbState is the per-node workflow state holding the pooled database
// handle. Executors are rebuilt per message, so the pool lives here.
type dbState struct {
	mu      sync.Mutex
	db      *neat.Database
	dialect string
}

// DBNode is the executor shell: Activate opens the pool, Execute runs
// one statement, Expand fans out rows in split mode, Stop closes.
type DBNode struct {
	*nodebase.BaseNode
	config     *DBConfig
	host       nodebase.NodeHost
	workflowID string

	// splitRows carries a query's row set from Execute to Expand when
	// split mode is on (same stash pattern as fileIn/html).
	splitRows  []interface{}
	splitProto nodestore.MessageInterface

	// st caches the shared dbState so Stop can close the pool even
	// after the host has wiped node state (StopWorkflow/redeploy
	// clear e.states before stopping stoppers — recreating state
	// here would lose the live handle and leak the pool).
	stMu sync.Mutex
	st   *dbState
}

// NewDBNode creates a new db node executor shell.
func NewDBNode(node nodestore.NodeInterface, config *DBConfig, host nodebase.NodeHost) *DBNode {
	return &DBNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
	}
}

// Validate checks config at deploy time: dsn and sql are required,
// operation must be known, readOnly rejects exec.
func (n *DBNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(n.config.DSN) == "" {
		return errors.New("db node requires a dsn")
	}
	op := dbOperation(n.config.Operation)
	if op != "query" && op != "exec" {
		return fmt.Errorf("db node has invalid operation %q (query|exec)", n.config.Operation)
	}
	if n.config.ReadOnly && op == "exec" {
		return errors.New("db node is readOnly: exec operation is not allowed")
	}
	if strings.TrimSpace(n.config.SQL) == "" {
		return errors.New("db node requires sql")
	}
	return nil
}

// state returns the shared dbState for this node, creating it on
// first use and caching it on the shell for Stop.
func (n *DBNode) state() *dbState {
	n.stMu.Lock()
	defer n.stMu.Unlock()
	if n.st == nil {
		n.st = n.host.GetOrCreateNodeState(n.workflowID, n.GetID(), func() interface{} {
			return &dbState{}
		}).(*dbState)
	}
	return n.st
}

// Activate implements Activator: deploy opens the pooled connection
// and pings it, so an unparseable DSN or an unreachable database fails
// the deploy rather than the first message.
func (n *DBNode) Activate(ctx context.Context) error {
	dsn, err := n.resolveDSN()
	if err != nil {
		return err
	}
	normalized := normalizeDBDSN(dsn)
	db, err := neat.NewFromDSN(normalized)
	if err != nil {
		return fmt.Errorf("db node failed to connect: %w", err)
	}
	st := n.state()
	st.mu.Lock()
	old := st.db
	st.db = db
	st.dialect = dbDialect(normalized)
	st.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	return nil
}

// Execute runs the configured statement against the pool.
func (n *DBNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	if input == nil {
		return nil, errors.New("db node: nil input message")
	}
	st := n.state()
	st.mu.Lock()
	db := st.db
	dialect := st.dialect
	st.mu.Unlock()
	if db == nil {
		return nil, errors.New("db node is not connected (deploy the flow first)")
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("db node: %w", err)
	}

	args, err := n.bindArgs(input)
	if err != nil {
		return nil, err
	}
	statement := rebindPlaceholders(n.config.SQL, neatdriver.GetPlaceholderFunc(dialect))

	out := nodebase.ClonePayload(input)
	if out == nil {
		return nil, errors.New("db node: cannot clone nil input message")
	}
	if dbOperation(n.config.Operation) == "exec" {
		res, err := sqlDB.ExecContext(ctx, statement, args...)
		if err != nil {
			return nil, fmt.Errorf("db exec failed: %w", err)
		}
		result := map[string]interface{}{}
		if n, err := res.RowsAffected(); err == nil {
			result["affected"] = n
		}
		if id, err := res.LastInsertId(); err == nil {
			result["lastId"] = id
		}
		out.SetPayload(result)
		return out, nil
	}

	rows, err := sqlDB.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("db query failed: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result, err := scanRows(rows)
	if err != nil {
		return nil, fmt.Errorf("db query failed: %w", err)
	}
	if n.config.Split {
		n.splitRows = result
		n.splitProto = out
		out.SetPayload([]interface{}{})
		return out, nil
	}
	out.SetPayload(result)
	return out, nil
}

// Expand implements MessageExpander: in split mode each result row
// becomes its own message (Data = the row object); an empty result
// set emits nothing. Non-split executions (and exec results) pass
// their single output through unchanged.
func (n *DBNode) Expand(output nodestore.MessageInterface) []nodestore.MessageInterface {
	if !n.config.Split {
		return []nodestore.MessageInterface{output}
	}
	if len(n.splitRows) == 0 {
		return nil
	}
	outs := make([]nodestore.MessageInterface, 0, len(n.splitRows))
	for i, row := range n.splitRows {
		p := nodebase.ClonePayload(n.splitProto)
		if p == nil {
			continue
		}
		p.SetPayload(row)
		if p.GetMetadata() == nil {
			p.SetMetadata(map[string]interface{}{})
		}
		p.GetMetadata()["dbRow"] = i
		outs = append(outs, p)
	}
	return outs
}

// Stop implements Stopper: closes the pooled handle. A subsequent
// Execute fails cleanly until the next Activate reopens it. Must not
// recreate host state — it may already be wiped.
func (n *DBNode) Stop() error {
	n.stMu.Lock()
	st := n.st
	n.stMu.Unlock()
	if st == nil {
		return nil
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.db != nil {
		err := st.db.Close()
		st.db = nil
		return err
	}
	return nil
}

// stopKey dedupes stopper registration: all executor shells of one
// node share the same pool.
func (n *DBNode) StopKey() string {
	return "db:" + n.workflowID + ":" + n.GetID()
}

// resolveDSN resolves the configured dsn, honouring the "env:NAME"
// reference form via the scoped env chain (group → component → flow →
// global-config → OS environment).
func (n *DBNode) resolveDSN() (string, error) {
	dsn := strings.TrimSpace(n.config.DSN)
	if name, ok := strings.CutPrefix(dsn, "env:"); ok {
		name = strings.TrimSpace(name)
		if name == "" {
			return "", errors.New("db node dsn env: reference needs a variable name")
		}
		if n.host != nil {
			if v, ok := n.host.NodeEnv(n.workflowID, n.GetID(), name); ok {
				return nodebase.StringOf(v), nil
			}
		}
		if v := os.Getenv(name); v != "" {
			return v, nil
		}
		return "", fmt.Errorf("db node dsn env var %q is not set", name)
	}
	return dsn, nil
}

// bindArgs resolves the configured params against the incoming message
// into positional driver args. Values are bound, never interpolated.
func (n *DBNode) bindArgs(input nodestore.MessageInterface) ([]interface{}, error) {
	var raw interface{}
	switch p := n.config.Params.(type) {
	case nil:
		if m, ok := input.GetPayload().(map[string]interface{}); ok {
			raw = m["params"]
		}
	case string:
		if strings.TrimSpace(p) == "" {
			if m, ok := input.GetPayload().(map[string]interface{}); ok {
				raw = m["params"]
			}
			break
		}
		var found bool
		if path, ok := nodebase.MsgPath(p); ok {
			raw, found = nodebase.GetPath(input.GetMetadata(), path)
		} else {
			m, ok := input.GetPayload().(map[string]interface{})
			if ok {
				raw, found = nodebase.GetPath(m, p)
			}
		}
		if !found {
			return nil, fmt.Errorf("db node params field %q not found in the message", p)
		}
	default:
		raw = p
	}
	if raw == nil {
		return nil, nil
	}
	items, ok := raw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("db node params must be an array, got %T", raw)
	}
	return items, nil
}

// dbOperation normalizes the operation field (default "query").
func dbOperation(op string) string {
	op = strings.ToLower(strings.TrimSpace(op))
	if op == "" {
		return "query"
	}
	return op
}

// normalizeDBDSN applies the same rule as storage.normalizeDSN: a
// full neat DSN passes through; a plain path or ":memory:" becomes
// a sqlite:// DSN.
func normalizeDBDSN(dsn string) string {
	if strings.Contains(dsn, "://") || strings.HasPrefix(dsn, "file:") {
		return dsn
	}
	if strings.Contains(dsn, ":memory:") {
		return "sqlite://:memory:"
	}
	return "sqlite://" + dsn
}

// dbDialect extracts the neat driver name from a normalized DSN so
// '?' placeholders rebind to the driver's native style. "file:" and
// bare sqlite DSNs report "sqlite"; "postgresql://" reports
// "postgres".
func dbDialect(normalized string) string {
	if strings.HasPrefix(normalized, "file:") {
		return "sqlite"
	}
	if i := strings.Index(normalized, "://"); i >= 0 {
		scheme := normalized[:i]
		if scheme == "postgresql" || scheme == "libsql" {
			scheme = map[string]string{"postgresql": "postgres", "libsql": "turso"}[scheme]
		}
		return scheme
	}
	return "sqlite"
}

// rebindPlaceholders rewrites each '?' outside string literals using
// the dialect's placeholder function, so one config style works
// across sqlite/mysql (?) and postgres ($n) / sqlserver (@pn) /
// oracle (:n).
func rebindPlaceholders(query string, ph neatdriver.PlaceholderFunc) string {
	var b strings.Builder
	b.Grow(len(query) + 8)
	idx := 0
	var quote byte
	for i := 0; i < len(query); i++ {
		c := query[i]
		if quote != 0 {
			b.WriteByte(c)
			if c == quote {
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			quote = c
			b.WriteByte(c)
		case '?':
			idx++
			b.WriteString(ph(idx))
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// scanRows drains *sql.Rows into a slice of column→value maps.
// []byte values are converted to strings so rows serialize cleanly.
func scanRows(rows *sql.Rows) ([]interface{}, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	out := []interface{}{}
	for rows.Next() {
		cells := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(map[string]interface{}, len(cols))
		for i, c := range cols {
			if b, ok := cells[i].([]byte); ok {
				row[c] = string(b)
			} else {
				row[c] = cells[i]
			}
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
