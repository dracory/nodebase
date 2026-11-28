package nodebase

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/jlaffaye/ftp"
)

// ftpFs is the Fs adapter over FTP (github.com/jlaffaye/ftp — the
// canonical Go FTP client; protocol code is not hand-rolled).
//
// Semantics caveats (recorded in the task Evidence):
//   - FTP rename (RNFR/RNTO) is NOT atomic: a reader listing mid-move
//     can see the tmp name, but never a partially-written final file —
//     the tmp+rename pattern still guarantees no truncated .json.
//   - Listings have no ordering guarantee — the queue backend keys on
//     the body seq.
type ftpFs struct {
	conn *ftp.ServerConn
}

// openFTPFs connects and logs in. Username comes from the URI
// userinfo (default "anonymous"); the password is the credential
// field, falling back to the URI userinfo password, then "anonymous".
func openFTPFs(u *url.URL, creds *FsCredentials) (Fs, error) {
	host := u.Host
	if !strings.Contains(host, ":") {
		host += ":21"
	}
	user := u.User.Username()
	if user == "" {
		user = "anonymous"
	}
	password := creds.Password
	if password == "" {
		if pw, ok := u.User.Password(); ok {
			password = pw
		}
	}
	if password == "" {
		password = "anonymous"
	}
	conn, err := ftp.Dial(host, ftp.DialWithTimeout(15*time.Second))
	if err != nil {
		return nil, fmt.Errorf("ftp: dial %s failed: %w", host, err)
	}
	if err := conn.Login(user, password); err != nil {
		_ = conn.Quit()
		return nil, fmt.Errorf("ftp: login %s failed: %w", host, err)
	}
	return &ftpFs{conn: conn}, nil
}

func (f *ftpFs) Join(dir, name string) string { return path.Join(dir, name) }

func (f *ftpFs) MkdirAll(dir string) error {
	if dir == "" || dir == "/" || dir == "." {
		return nil
	}
	// FTP has no mkdir -p: create each segment, tolerating
	// "already exists" errors.
	cur := ""
	if strings.HasPrefix(dir, "/") {
		cur = "/"
	}
	for _, seg := range strings.Split(strings.Trim(dir, "/"), "/") {
		if seg == "" || seg == "." {
			continue
		}
		cur = path.Join(cur, seg)
		if err := f.conn.MakeDir(cur); err != nil {
			// Existence is the only tolerated failure; verify.
			if _, lerr := f.conn.List(cur); lerr != nil {
				return fmt.Errorf("ftp: mkdir %q failed: %w", cur, err)
			}
		}
	}
	return nil
}

func (f *ftpFs) List(dir string) ([]FsEntry, error) {
	entries, err := f.conn.List(dir)
	if err != nil {
		return nil, err
	}
	out := make([]FsEntry, 0, len(entries))
	for _, e := range entries {
		name := e.Name
		// Some servers return absolute names — strip the dir prefix.
		if base := path.Base(name); strings.HasPrefix(name, "/") {
			name = base
		}
		out = append(out, FsEntry{
			Name:    name,
			Size:    int64(e.Size),
			ModTime: e.Time,
			IsDir:   e.Type == ftp.EntryTypeFolder,
		})
	}
	return out, nil
}

func (f *ftpFs) ReadFile(name string) ([]byte, error) {
	r, err := f.conn.Retr(name)
	if err != nil {
		return nil, err
	}
	defer func() { _ = r.Close() }()
	return io.ReadAll(r)
}

func (f *ftpFs) WriteFile(name string, data []byte) error {
	return f.conn.Stor(name, bytes.NewReader(data))
}

func (f *ftpFs) AppendFile(name string, data []byte) error {
	// jlaffaye/ftp has no APPE verb: read-modify-store. Documented
	// append is best-effort — the fsOut append mode on ftp is not
	// atomic and not multi-writer safe.
	existing, err := f.ReadFile(name)
	if err != nil {
		existing = nil // missing file starts empty
	}
	return f.WriteFile(name, append(existing, data...))
}

func (f *ftpFs) Rename(oldName, newName string) error {
	return f.conn.Rename(oldName, newName)
}

func (f *ftpFs) Remove(name string) error {
	if err := f.conn.Delete(name); err != nil {
		// A missing file is not an error; FTP has no portable
		// not-exist sentinel, so confirm with a stat attempt.
		if _, lerr := f.conn.GetEntry(name); lerr != nil {
			return nil
		}
		return err
	}
	return nil
}

func (f *ftpFs) Close() error {
	if f.conn != nil {
		return f.conn.Quit()
	}
	return nil
}

var _ Fs = (*ftpFs)(nil)
