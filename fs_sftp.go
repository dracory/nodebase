package nodebase

import (
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// sftpFs is the Fs adapter over SSH File Transfer Protocol
// (github.com/pkg/sftp + golang.org/x/crypto/ssh — the canonical
// implementation, not hand-rolled protocol code). SFTP rename is
// atomic-ish on every server implementing posix-rename, so the
// tmp+rename write pattern keeps its crash-safety property.
type sftpFs struct {
	conn   *ssh.Client
	client *sftp.Client
}

// openSFTPFs dials the locator's host and opens an SFTP session.
// Authentication precedence: private key (creds.KeyFile) then
// password (creds.Password, falling back to the URI userinfo
// password). Host key handling: creds.HostKey pins the expected
// server key (authorized_keys line or base64 wire format); empty
// accepts any host key and logs a warning — acceptable for first
// deployments, pin a key for anything that matters.
func openSFTPFs(u *url.URL, creds *FsCredentials) (Fs, error) {
	host := u.Host
	if !strings.Contains(host, ":") {
		host += ":22"
	}
	user := u.User.Username()

	var auths []ssh.AuthMethod
	if creds.KeyFile != "" {
		raw, err := os.ReadFile(creds.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("sftp: cannot read keyFile %q: %w", creds.KeyFile, err)
		}
		var signer ssh.Signer
		if creds.Password != "" {
			signer, err = ssh.ParsePrivateKeyWithPassphrase(raw, []byte(creds.Password))
		} else {
			signer, err = ssh.ParsePrivateKey(raw)
		}
		if err != nil {
			return nil, fmt.Errorf("sftp: cannot parse keyFile %q: %w", creds.KeyFile, err)
		}
		auths = append(auths, ssh.PublicKeys(signer))
	}
	password := creds.Password
	if password == "" {
		if pw, ok := u.User.Password(); ok {
			password = pw
		}
	}
	if password != "" {
		auths = append(auths, ssh.Password(password))
	}
	if len(auths) == 0 {
		return nil, fmt.Errorf("sftp: no credentials — set the password (credential field) or keyFile")
	}

	var hostKeyCb ssh.HostKeyCallback
	if creds.HostKey != "" {
		expected, err := parseHostKey(creds.HostKey)
		if err != nil {
			return nil, fmt.Errorf("sftp: invalid hostKey: %w", err)
		}
		hostKeyCb = func(_ string, _ net.Addr, key ssh.PublicKey) error {
			if string(key.Marshal()) == string(expected.Marshal()) {
				return nil
			}
			return fmt.Errorf("sftp: host key mismatch (got %s %s)", key.Type(), base64.StdEncoding.EncodeToString(key.Marshal()))
		}
	} else {
		// Fail closed: accepting any host key is a MITM opening
		// (password/key auth runs inside the tunnel), so it must be
		// an explicit opt-out, not the silent default.
		if !creds.AcceptAnyHostKey {
			return nil, fmt.Errorf("sftp: no hostKey pinned for %s — set hostKey (authorized_keys or base64) or acceptAnyHostKey=true to connect anyway", host)
		}
		logRemoteWarn("sftp host %s: acceptAnyHostKey set; accepting any host key (insecure)", host)
		hostKeyCb = ssh.InsecureIgnoreHostKey() //nolint:gosec — explicit user opt-out
	}

	cfg := &ssh.ClientConfig{
		User:            user,
		Auth:            auths,
		HostKeyCallback: hostKeyCb,
		Timeout:         15 * time.Second,
	}
	conn, err := ssh.Dial("tcp", host, cfg)
	if err != nil {
		return nil, fmt.Errorf("sftp: dial %s failed: %w", host, err)
	}
	client, err := sftp.NewClient(conn)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("sftp: session open failed: %w", err)
	}
	return &sftpFs{conn: conn, client: client}, nil
}

// parseHostKey accepts an authorized_keys-format line
// ("ssh-ed25519 AAAA… [comment]") or the raw base64 wire key.
func parseHostKey(s string) (ssh.PublicKey, error) {
	s = strings.TrimSpace(s)
	if key, _, _, _, err := ssh.ParseAuthorizedKey([]byte(s)); err == nil {
		return key, nil
	}
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("host key is neither authorized_keys format nor base64")
	}
	return ssh.ParsePublicKey(raw)
}

func (s *sftpFs) Join(dir, name string) string { return path.Join(dir, name) }
func (s *sftpFs) MkdirAll(dir string) error    { return s.client.MkdirAll(dir) }
func (s *sftpFs) ReadFile(name string) ([]byte, error) {
	f, err := s.client.Open(name)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return io.ReadAll(f)
}
func (s *sftpFs) WriteFile(name string, data []byte) error {
	f, err := s.client.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	_, err = f.Write(data)
	return err
}
func (s *sftpFs) AppendFile(name string, data []byte) error {
	f, err := s.client.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_APPEND)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	_, err = f.Write(data)
	return err
}
func (s *sftpFs) Rename(oldName, newName string) error {
	// PosixRename is the atomic variant; fall back to plain rename on
	// servers that do not implement the extension.
	if err := s.client.PosixRename(oldName, newName); err != nil {
		return s.client.Rename(oldName, newName)
	}
	return nil
}
func (s *sftpFs) Remove(name string) error {
	if err := s.client.Remove(name); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
func (s *sftpFs) Close() error {
	_ = s.client.Close()
	return s.conn.Close()
}
func (s *sftpFs) List(dir string) ([]FsEntry, error) {
	infos, err := s.client.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	out := make([]FsEntry, 0, len(infos))
	for _, info := range infos {
		out = append(out, FsEntry{
			Name:    info.Name(),
			Size:    info.Size(),
			ModTime: info.ModTime(),
			IsDir:   info.IsDir(),
		})
	}
	return out, nil
}

var _ Fs = (*sftpFs)(nil)
