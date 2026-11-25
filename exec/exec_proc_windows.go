//go:build windows

package exec

// Process signalling and exit-status decoding for Windows hosts.
// Windows has no POSIX signals: the only way a child dies externally
// is Process.Kill, which Node-RED reports as SIGTERM.

import (
	"os"
	oexec "os/exec"
)

// signalProcess kills the process — named signals cannot be
// delivered on Windows, so every signal degrades to a kill.
func signalProcess(cmd *oexec.Cmd, name string) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill()
}

// exitCodeAndSignal decodes the finished process state: a killed
// process reports ExitCode -1, surfaced as {code:null,signal:SIGTERM}.
func exitCodeAndSignal(state *os.ProcessState) (code int, signal string) {
	if state == nil {
		return -1, ""
	}
	code = state.ExitCode()
	if code < 0 {
		return -1, "SIGTERM"
	}
	return code, ""
}
