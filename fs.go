package nodebase

import (
	"fmt"
	nodestore "github.com/dracory/nodestore"
	"log"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// This file defines the unified remote-filesystem seam (task
// remote_fs): one small Fs interface feeding three
// consumers — the queue "dir" backend (which now accepts Fs locators
// like sftp://user@host/path), the fsIn node (poll a remote dir, emit
// one message per new file — Enfocus Switch's FTP receive), and the
// fsOut node (write payloads to remote paths — Switch's FTP send).
//
// Design notes:
//   - Fs is intentionally small (list/read/write/rename/remove/mkdir)
//     — it is NOT the surface of any underlying library. Per GOD
//     direction, protocol code is not hand-rolled: the canonical
//     libraries (github.com/pkg/sftp, github.com/jlaffaye/ftp) sit
//     behind adapters. The shortlisted adapter libraries
//     (goforj/storage et al.) were rejected for vendor weight — they
//     pull the full AWS/GCS/Dropbox SDKs for protocols v1 does not
//     ship.
//   - Remote backends are POLLING only: there is no fsnotify. The
//     queue drainer already scans on arm; fsIn polls on an interval.
//     This matches Switch's folder-poll model.
//   - Ordering: directory listing order is NOT seq order — the dir
//     queue backend already keys on the body seq (see queue_dir.go).
//   - Atomicity: writes are tmp-file + rename on every adapter. Local
//     and SFTP renames are atomic-ish (SFTP posix rename); FTP rename
//     (RNFR/RNTO) is NOT atomic — a reader mid-rename can see the tmp
//     name, but never a partial final file. S3 has no rename and is
//     out of v1 scope (locator returns an explicit error).
//   - Concurrent-drainer safety: a remote dir can be shared the same
//     way an NFS/SMB/SSHFS mount is — multi-runner safety remains a
//     deployment property, the store makes no claim.
//   - Credentials: passwords live in credential-typed node fields
//     (format:"credential", nodestore.CredentialSentinel storage), never inline
//     in the locator URI or the exported flow document. A userinfo
//     password in the URI is accepted for tooling but should not be
//     used in flows.

// FsEntry describes one directory entry or file.
type FsEntry struct {
	Name    string
	Size    int64
	ModTime time.Time
	IsDir   bool
}

// Fs is the filesystem seam: the minimal surface DirQueueStore and
// the fsIn/fsOut nodes need.
type Fs interface {
	// Join joins a directory and a base name using the backend's
	// path convention (OS separator locally, "/" remotely).
	Join(dir, name string) string
	// MkdirAll creates dir and any missing parents.
	MkdirAll(dir string) error
	// List returns the direct children of dir.
	List(dir string) ([]FsEntry, error)
	// ReadFile reads the whole file.
	ReadFile(name string) ([]byte, error)
	// WriteFile writes data to name, creating or truncating.
	WriteFile(name string, data []byte) error
	// AppendFile appends data to name, creating it if needed.
	AppendFile(name string, data []byte) error
	// Rename moves old to new. Atomic on local and sftp; NOT atomic
	// on ftp (documented — never yields a partial final file).
	Rename(oldName, newName string) error
	// Remove deletes a file. A missing file is not an error.
	Remove(name string) error
	// Close releases any connection. Local is a no-op.
	Close() error
}

// FsCredentials carries the secret/auth material for remote locators.
// Password is credential-typed on every node schema; host keys are
// public information and travel as plain config.
type FsCredentials struct {
	// Password authenticates password login (credential field).
	Password string
	// KeyFile is a LOCAL path to an SSH private key (sftp only). An
	// optional passphrase rides in Password.
	KeyFile string
	// HostKey pins the expected SSH host public key — authorized_keys
	// line or base64 wire format. ftp ignores it.
	HostKey string
	// AcceptAnyHostKey disables SSH host-key verification when
	// HostKey is empty (INSECURE — explicit opt-out required; sftp
	// otherwise refuses to connect without a pinned key).
	AcceptAnyHostKey bool
}

// ParseFsLocator splits a locator into (scheme, dir). A plain path or
// a file:// URI is local. sftp://[user[:pass]@]host[:port]/path and
// ftp://… select remote adapters. s3:// and unknown schemes error.
// Returned dir uses the backend's native separator.
func ParseFsLocator(locator string) (scheme, dir string, u *url.URL, err error) {
	locator = strings.TrimSpace(locator)
	if locator == "" {
		return "", "", nil, fmt.Errorf("fs locator is empty")
	}
	if !strings.Contains(locator, "://") {
		return "file", locator, nil, nil
	}
	u, err = url.Parse(locator)
	if err != nil {
		return "", "", nil, fmt.Errorf("invalid fs locator %q: %w", locator, err)
	}
	switch u.Scheme {
	case "file":
		return "file", u.Path, u, nil
	case "sftp", "ftp":
		dir := u.Path
		if dir == "" {
			dir = "/"
		}
		return u.Scheme, dir, u, nil
	case "s3":
		return "", "", nil, fmt.Errorf("s3:// locators are not supported in this version")
	default:
		return "", "", nil, fmt.Errorf("unsupported fs scheme %q (supported: file path, sftp://, ftp://)", u.Scheme)
	}
}

// OpenFs resolves a locator to a connected Fs plus the directory the
// locator pointed at. Remote locators are dialed once here so config
// errors surface at deploy; the returned Fs transparently re-dials if
// the connection later drops (see redialFs). creds may be nil. The
// caller owns the Fs and should Close it when done.
func OpenFs(locator string, creds *FsCredentials) (Fs, string, error) {
	if creds == nil {
		creds = &FsCredentials{}
	}
	scheme, dir, u, err := ParseFsLocator(locator)
	if err != nil {
		return nil, "", err
	}
	// Credentials embedded in the locator URI are used (sftp/ftp read
	// u.User) but live in the flow document as plain config — outside
	// the credential-sentinel model. Warn so operators move them to
	// the credential "password" field instead.
	if u != nil && u.User != nil {
		if _, hasPw := u.User.Password(); hasPw {
			log.Printf("fs locator %s:// embeds a password in the URI; prefer the credential \"password\" config key so the secret stays behind the credential sentinel", u.Scheme)
		}
	}
	switch scheme {
	case "file":
		return LocalFs{}, dir, nil
	case "sftp":
		r := &redialFs{open: func() (Fs, error) {
			return openSFTPFs(u, creds)
		}}
		if _, err := r.get(); err != nil {
			return nil, "", err
		}
		return r, dir, nil
	case "ftp":
		r := &redialFs{open: func() (Fs, error) {
			return openFTPFs(u, creds)
		}}
		if _, err := r.get(); err != nil {
			return nil, "", err
		}
		return r, dir, nil
	}
	return nil, "", fmt.Errorf("unsupported fs scheme %q", scheme)
}

// FsCredentialsFromData extracts the credential/config keys shared by
// the queue dir backend and the fsIn/fsOut nodes.
func FsCredentialsFromData(data map[string]interface{}) *FsCredentials {
	creds := &FsCredentials{}
	get := func(key string) string {
		v, _ := data[key].(string)
		if nodestore.IsCredentialSentinel(v) {
			return ""
		}
		return strings.TrimSpace(v)
	}
	creds.Password = get("password")
	creds.KeyFile = get("keyFile")
	creds.HostKey = get("hostKey")
	switch v := data["acceptAnyHostKey"].(type) {
	case bool:
		creds.AcceptAnyHostKey = v
	case string:
		creds.AcceptAnyHostKey = strings.EqualFold(strings.TrimSpace(v), "true")
	}
	return creds
}

/* ---- local adapter ---- */

// LocalFs wraps the native os package — the pre-task DirQueueStore
// behaviour, byte for byte.
type LocalFs struct{}

func (LocalFs) Join(dir, name string) string         { return filepath.Join(dir, name) }
func (LocalFs) MkdirAll(dir string) error            { return os.MkdirAll(dir, 0755) }
func (LocalFs) ReadFile(name string) ([]byte, error) { return os.ReadFile(name) }
func (LocalFs) WriteFile(name string, data []byte) error {
	return os.WriteFile(name, data, 0644)
}
func (LocalFs) AppendFile(name string, data []byte) error {
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	_, err = f.Write(data)
	return err
}
func (LocalFs) Rename(oldName, newName string) error { return os.Rename(oldName, newName) }
func (LocalFs) Remove(name string) error {
	if err := os.Remove(name); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
func (LocalFs) Close() error { return nil }

func (LocalFs) List(dir string) ([]FsEntry, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	out := make([]FsEntry, 0, len(entries))
	for _, e := range entries {
		fe := FsEntry{Name: e.Name(), IsDir: e.IsDir()}
		if info, err := e.Info(); err == nil {
			fe.Size = info.Size()
			fe.ModTime = info.ModTime()
		}
		out = append(out, fe)
	}
	return out, nil
}

/* ---- redial decorator ---- */

// redialFs decorates a dial-once remote Fs: the first call dials,
// subsequent calls reuse the connection, and a failed call re-dials
// once before giving up. Remote connections drop; the queue drainer
// and fsIn poller are long-lived, so one retry keeps a stale session
// from permanently failing the node.
type redialFs struct {
	mu   sync.Mutex
	open func() (Fs, error)
	cur  Fs
}

func (r *redialFs) get() (Fs, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cur != nil {
		return r.cur, nil
	}
	f, err := r.open()
	if err != nil {
		return nil, err
	}
	r.cur = f
	return f, nil
}

// redial drops the current connection and opens a fresh one.
func (r *redialFs) redial() (Fs, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cur != nil {
		_ = r.cur.Close()
		r.cur = nil
	}
	f, err := r.open()
	if err != nil {
		return nil, err
	}
	r.cur = f
	return f, nil
}

func (r *redialFs) call(fn func(Fs) error) error {
	f, err := r.get()
	if err != nil {
		return err
	}
	if err := fn(f); err == nil {
		return nil
	}
	// Any error may be a dead session; retry once on a fresh dial.
	f, err = r.redial()
	if err != nil {
		return err
	}
	return fn(f)
}

func (r *redialFs) Join(dir, name string) string { return path.Join(dir, name) }
func (r *redialFs) MkdirAll(dir string) error {
	return r.call(func(f Fs) error { return f.MkdirAll(dir) })
}
func (r *redialFs) List(dir string) (out []FsEntry, err error) {
	err = r.call(func(f Fs) error {
		var e error
		out, e = f.List(dir)
		return e
	})
	return out, err
}
func (r *redialFs) ReadFile(name string) (out []byte, err error) {
	err = r.call(func(f Fs) error {
		var e error
		out, e = f.ReadFile(name)
		return e
	})
	return out, err
}
func (r *redialFs) WriteFile(name string, data []byte) error {
	return r.call(func(f Fs) error { return f.WriteFile(name, data) })
}
func (r *redialFs) AppendFile(name string, data []byte) error {
	return r.call(func(f Fs) error { return f.AppendFile(name, data) })
}
func (r *redialFs) Rename(oldName, newName string) error {
	return r.call(func(f Fs) error { return f.Rename(oldName, newName) })
}
func (r *redialFs) Remove(name string) error {
	return r.call(func(f Fs) error { return f.Remove(name) })
}
func (r *redialFs) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cur != nil {
		err := r.cur.Close()
		r.cur = nil
		return err
	}
	return nil
}

var _ Fs = (*redialFs)(nil)
var _ Fs = LocalFs{}

// logRemoteWarn centralises the "insecure host key" style warnings so
// operators see them once per connection attempt.
func logRemoteWarn(format string, args ...interface{}) {
	log.Printf("warning: fs: "+format, args...)
}
