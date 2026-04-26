package main

import (
	"fmt"
	"os/exec"
	"runtime"
	"strings"
)

// readClipboard pulls the current clipboard contents as bytes. Pure
// Go — shells out per OS to keep us CGO-free.
//
//	macOS:   pbpaste
//	Linux:   wl-paste (Wayland) -> xclip (X11) -> xsel (X11 fallback)
//	Windows: powershell Get-Clipboard
//
// The Linux chain is tried in order and the first one that exists is
// used. If none, we surface a friendly install hint.
func readClipboard() ([]byte, error) {
	switch runtime.GOOS {
	case "darwin":
		return exec.Command("pbpaste").Output()

	case "linux":
		// Try Wayland first (modern Linux desktops), then X11.
		for _, c := range []struct {
			cmd  string
			args []string
		}{
			{"wl-paste", []string{"-n"}}, // -n = no trailing newline
			{"xclip", []string{"-selection", "clipboard", "-o"}},
			{"xsel", []string{"--clipboard", "--output"}},
		} {
			if _, err := exec.LookPath(c.cmd); err != nil {
				continue
			}
			out, err := exec.Command(c.cmd, c.args...).Output()
			if err != nil {
				return nil, fmt.Errorf("%s: %w", c.cmd, err)
			}
			return out, nil
		}
		return nil, fmt.Errorf("no clipboard tool found.\n" +
			"  install one of: wl-clipboard (Wayland), xclip, or xsel")

	case "windows":
		// PowerShell's Get-Clipboard is the most portable — works on
		// every modern Windows. UTF-16 -> UTF-8 conversion is handled
		// by PowerShell's stdout pipe.
		out, err := exec.Command("powershell", "-NoProfile", "-Command", "Get-Clipboard").Output()
		if err != nil {
			return nil, fmt.Errorf("powershell Get-Clipboard: %w", err)
		}
		// PowerShell tends to add a trailing CRLF; trim ONE.
		s := strings.TrimRight(string(out), "\r\n")
		return []byte(s), nil

	default:
		return nil, fmt.Errorf("clipboard not supported on %s", runtime.GOOS)
	}
}
