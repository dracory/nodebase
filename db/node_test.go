package db

// Pack-local unit tests for the db node — sqlite in-memory round
// trips through the real neat pool plus the pure helpers (DSN
// normalization, dialect, placeholder rebinding, bindArgs).

import (
	"context"
	"crypto/tls"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// fakeHost supplies the node-state slot the pool lives in.
type fakeHost struct {
	state map[string]interface{}
	env   map[string]interface{}
}

func newFakeHost() *fakeHost { return &fakeHost{state: map[string]interface{}{}} }

func (h *fakeHost) Emit(wf, nodeID string, p nodestore.MessageInterface, ports []string) error {
	return nil
}
func (h *fakeHost) EmitEvent(wf, nodeID, name, level, message string) {}
func (h *fakeHost) InjectMessage(wf, from string, p nodestore.MessageInterface) (string, chan nodestore.MessageInterface, error) {
	return "", nil, nil
}
func (h *fakeHost) ResolveResponse(id string, p nodestore.MessageInterface) error { return nil }
func (h *fakeHost) DiscardPending(id string)                                      {}
func (h *fakeHost) LookupNode(wf, nodeID string) (nodestore.NodeInterface, bool) {
	return nil, false
}
func (h *fakeHost) RunningFlow(wf string) (nodestore.WorkflowInterface, bool) { return nil, false }
func (h *fakeHost) RunContext(wf string) context.Context                      { return context.Background() }
func (h *fakeHost) GetOrCreateNodeState(wf, nodeID string, f func() interface{}) interface{} {
	key := wf + ":" + nodeID
	if v, ok := h.state[key]; ok {
		return v
	}
	v := f()
	h.state[key] = v
	return v
}
func (h *fakeHost) ExecuteDownstream(ctx context.Context, wf, from string, w nodestore.WorkflowInterface, p nodestore.MessageInterface, ports []string) {
}
func (h *fakeHost) NodeEnv(wf, nodeID, name string) (interface{}, bool) {
	v, ok := h.env[name]
	return v, ok
}
func (h *fakeHost) ResolvedContextStore() nodebase.ContextStore        { return nil }
func (h *fakeHost) LoadTLSConfig(wf, ref string) (*tls.Config, error)  { return nil, nil }
func (h *fakeHost) FileRootPath() string                               { return "" }
func (h *fakeHost) ExecAllowed() bool                                  { return false }
func (h *fakeHost) SharedHTTPRouter() nodebase.HTTPRouter              { return nil }
func (h *fakeHost) HTTPAuthFunc() func(r *http.Request) (string, bool) { return nil }
func (h *fakeHost) QueueService() nodebase.QueueService                { return nil }

func newNode(id string, cfg *DBConfig, host *fakeHost) *DBNode {
	return NewDBNode(nodestore.NewNode(id, "db").SetWorkflowID("wf"), cfg, host)
}

// activated returns a node with an open sqlite pool on the given file.
func activated(t *testing.T, dsn, id string, cfg *DBConfig) *DBNode {
	t.Helper()
	cfg.DSN = dsn
	n := newNode(id, cfg, newFakeHost())
	if err := n.Activate(context.Background()); err != nil {
		t.Fatalf("activate failed: %v", err)
	}
	t.Cleanup(func() { n.Stop() })
	return n
}

func TestDBQueryRoundTrip(t *testing.T) {
	dsn := "sqlite://" + filepath.Join(t.TempDir(), "t.db")
	exec := activated(t, dsn, "n1", &DBConfig{Operation: "exec", SQL: "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"})
	if _, err := exec.Execute(context.Background(), nodestore.NewMessage()); err != nil {
		t.Fatalf("create failed: %v", err)
	}
	exec.Stop()

	ins := activated(t, dsn, "n1", &DBConfig{Operation: "exec", SQL: "INSERT INTO t (name) VALUES (?)", Params: []interface{}{"alice"}})
	out, err := ins.Execute(context.Background(), nodestore.NewMessage())
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	res, _ := out.GetPayload().(map[string]interface{})
	if res["affected"] != int64(1) {
		t.Fatalf("affected wrong: %v", res)
	}

	q := activated(t, dsn, "n1", &DBConfig{SQL: "SELECT name FROM t WHERE name = ?", Params: []interface{}{"alice"}})
	out, err = q.Execute(context.Background(), nodestore.NewMessage())
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	rows, _ := out.GetPayload().([]interface{})
	if len(rows) != 1 || rows[0].(map[string]interface{})["name"] != "alice" {
		t.Fatalf("rows wrong: %v", rows)
	}
}

func TestDBSplitExpand(t *testing.T) {
	n := activated(t, "sqlite://"+filepath.Join(t.TempDir(), "t.db"), "n1", &DBConfig{
		SQL:   "SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT 3",
		Split: true,
	})
	out, err := n.Execute(context.Background(), nodestore.NewMessage())
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	outs := n.Expand(out)
	if len(outs) != 3 {
		t.Fatalf("split should emit one message per row: %d", len(outs))
	}
	if outs[0].GetPayload().(map[string]interface{})["v"] != int64(1) {
		t.Fatalf("row payload wrong: %v", outs[0].GetPayload())
	}
	// a non-split node's Expand passes its output through unchanged
	n2 := activated(t, "sqlite://"+filepath.Join(t.TempDir(), "t.db"), "n1", &DBConfig{SQL: "SELECT 1"})
	pass := nodestore.NewMessage()
	if got := n2.Expand(pass); len(got) != 1 || got[0] != pass {
		t.Fatal("non-split output should pass through")
	}
}

func TestDBNotConnected(t *testing.T) {
	n := newNode("n1", &DBConfig{SQL: "SELECT 1"}, newFakeHost())
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("execute before activate should fail")
	}
}

func TestDBValidate(t *testing.T) {
	host := newFakeHost()
	if err := newNode("n1", &DBConfig{DSN: "x", SQL: "SELECT 1"}, host).Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if err := newNode("n1", &DBConfig{SQL: "SELECT 1"}, host).Validate(); err == nil {
		t.Fatal("missing dsn should fail")
	}
	if err := newNode("n1", &DBConfig{DSN: "x"}, host).Validate(); err == nil {
		t.Fatal("missing sql should fail")
	}
	if err := newNode("n1", &DBConfig{DSN: "x", SQL: "s", Operation: "bogus"}, host).Validate(); err == nil {
		t.Fatal("bad operation should fail")
	}
	if err := newNode("n1", &DBConfig{DSN: "x", SQL: "s", Operation: "exec", ReadOnly: true}, host).Validate(); err == nil {
		t.Fatal("readOnly+exec should fail")
	}
}

func TestDBResolveDSNEnv(t *testing.T) {
	host := newFakeHost()
	host.env = map[string]interface{}{"MY_DSN": "sqlite://scoped.db"}
	n := newNode("n1", &DBConfig{DSN: "env:MY_DSN", SQL: "SELECT 1"}, host)
	dsn, err := n.resolveDSN()
	if err != nil || dsn != "sqlite://scoped.db" {
		t.Fatalf("env dsn wrong: %q %v", dsn, err)
	}
	n2 := newNode("n1", &DBConfig{DSN: "env:MISSING_VAR_XYZ", SQL: "s"}, host)
	if _, err := n2.resolveDSN(); err == nil {
		t.Fatal("unset env dsn should fail")
	}
	n3 := newNode("n1", &DBConfig{DSN: "env:", SQL: "s"}, host)
	if _, err := n3.resolveDSN(); err == nil {
		t.Fatal("empty env name should fail")
	}
}

func TestDBBindArgs(t *testing.T) {
	host := newFakeHost()
	// literal array
	n := newNode("n1", &DBConfig{DSN: "x", SQL: "s", Params: []interface{}{1, "a"}}, host)
	args, err := n.bindArgs(nodestore.NewMessage())
	if err != nil || len(args) != 2 {
		t.Fatalf("literal args wrong: %v %v", args, err)
	}
	// default: msg payload "params" key
	n2 := newNode("n1", &DBConfig{DSN: "x", SQL: "s"}, host)
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{"params": []interface{}{9}})
	args, _ = n2.bindArgs(msg)
	if len(args) != 1 || args[0] != 9 {
		t.Fatalf("default params wrong: %v", args)
	}
	// string path lookup
	n3 := newNode("n1", &DBConfig{DSN: "x", SQL: "s", Params: "items"}, host)
	args, err = n3.bindArgs(msg.SetPayload(map[string]interface{}{"items": []interface{}{"v"}}))
	if err != nil || len(args) != 1 {
		t.Fatalf("path params wrong: %v %v", args, err)
	}
	if _, err := n3.bindArgs(nodestore.NewMessage().SetPayload(map[string]interface{}{})); err == nil {
		t.Fatal("missing params path should fail")
	}
	// non-array fails
	n4 := newNode("n1", &DBConfig{DSN: "x", SQL: "s", Params: "scalar"}, host)
	if _, err := n4.bindArgs(msg.SetPayload(map[string]interface{}{"scalar": 5})); err == nil {
		t.Fatal("non-array params should fail")
	}
}

func TestNormalizeDialectRebind(t *testing.T) {
	if normalizeDBDSN(":memory:") != "sqlite://:memory:" {
		t.Fatal(":memory: normalization wrong")
	}
	if normalizeDBDSN("/tmp/x.db") != "sqlite:///tmp/x.db" {
		t.Fatal("bare path normalization wrong")
	}
	if normalizeDBDSN("postgres://h/db") != "postgres://h/db" {
		t.Fatal("full DSN should pass through")
	}
	if dbDialect("sqlite://x") != "sqlite" || dbDialect("postgresql://x") != "postgres" ||
		dbDialect("libsql://x") != "turso" || dbDialect("file:x.db") != "sqlite" {
		t.Fatal("dialect mapping wrong")
	}
	if dbOperation("") != "query" || dbOperation(" EXEC ") != "exec" {
		t.Fatal("operation normalization wrong")
	}
	// placeholders rebind for postgres, quoted ? is untouched
	got := rebindPlaceholders("SELECT * FROM t WHERE a = ? AND b = '?'", func(i int) string {
		return "$" + string(rune('0'+i))
	})
	if got != "SELECT * FROM t WHERE a = $1 AND b = '?'" {
		t.Fatalf("rebind wrong: %q", got)
	}
}
