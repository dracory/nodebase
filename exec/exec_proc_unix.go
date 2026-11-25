//go:build !windows

package exec

// Process signalling and exit-status decoding for unix hosts.

import (
	"os"
	oexec "os/exec"
	"strings"
	"syscall"
)

// signalByName maps Node-RED msg.kill signal names onto unix signals.
var signalByName = map[string]os.Signal{
	"SIGHUP":  syscall.SIGHUP,
	"SIGINT":  syscall.SIGINT,
	"SIGQUIT": syscall.SIGQUIT,
	"SIGABRT": syscall.SIGABRT,
	"SIGKILL": syscall.SIGKILL,
	"SIGALRM": syscall.SIGALRM,
	"SIGTERM": syscall.SIGTERM,
	"SIGUSR1": syscall.SIGUSR1,
	"SIGUSR2": syscall.SIGUSR2,
	"SIGPIPE": syscall.SIGPIPE,
}

// signalProcess sends the named signal to the process (SIGTERM when
// the name is unknown — matching the caller's normalization).
func signalProcess(cmd *oexec.Cmd, name string) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	sig, ok := signalByName[name]
	if !ok {
		sig = syscall.SIGTERM
	}
	_ = cmd.Process.Signal(sig)
}

// exitCodeAndSignal decodes the finished process state: a negative
// code means the process died on a signal — the signal name is
// returned for the {code:null,signal:...} output object.
func exitCodeAndSignal(state *os.ProcessState) (code int, signal string) {
	if state == nil {
		return -1, ""
	}
	if ws, ok := state.Sys().(syscall.WaitStatus); ok {
		if ws.Signaled() {
			sig := ws.Signal()
			for name, s := range signalByName {
				if s == sig {
					return -1, name
				}
			}
			return -1, strings.ToUpper(sig.String())
		}
	}
	return state.ExitCode(), ""
}
