package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

// runLog is written by this process at start and closed with an end timestamp.
// The header is not edited afterwards.
type runLog struct {
	f     *os.File
	start time.Time
}

func openRunLog(path string) (*runLog, error) {
	if strings.TrimSpace(path) == "" {
		return nil, nil
	}
	head, gov, err := cleanHead()
	if err != nil {
		return nil, err
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	lg := &runLog{f: f, start: time.Now().UTC()}
	fmt.Fprintf(f, "commit %s\n", head)
	fmt.Fprintf(f, "git_status clean\n")
	fmt.Fprintf(f, "go %s\n", gov)
	fmt.Fprintf(f, "start %s\n", lg.start.Format(time.RFC3339Nano))
	return lg, nil
}

func (l *runLog) Close() {
	if l == nil || l.f == nil {
		return
	}
	fmt.Fprintf(l.f, "end %s\n", time.Now().UTC().Format(time.RFC3339Nano))
	_ = l.f.Close()
	l.f = nil
}

func (l *runLog) Write(p []byte) (int, error) {
	if l == nil || l.f == nil {
		return len(p), nil
	}
	return l.f.Write(p)
}

func cleanHead() (head, gov string, err error) {
	root, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return "", "", fmt.Errorf("git rev-parse: %w", err)
	}
	dir := strings.TrimSpace(string(root))
	hb, err := exec.Command("git", "-C", dir, "rev-parse", "HEAD").Output()
	if err != nil {
		return "", "", fmt.Errorf("git rev-parse HEAD: %w", err)
	}
	st, err := exec.Command("git", "-C", dir, "status", "--porcelain").Output()
	if err != nil {
		return "", "", fmt.Errorf("git status: %w", err)
	}
	if strings.TrimSpace(string(st)) != "" {
		return "", "", fmt.Errorf("working tree is not clean:\n%s", st)
	}
	gv, err := exec.Command("go", "version").Output()
	if err != nil {
		return "", "", err
	}
	return strings.TrimSpace(string(hb)), strings.TrimSpace(string(gv)), nil
}
