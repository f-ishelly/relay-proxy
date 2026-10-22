package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type etcdProc struct {
	name   string
	client string
	cmd    *exec.Cmd
}

func runParent(lg *runLog, out io.Writer, hosts, procs int, ttl int64, renew, keep, killAfter, registerTimeout time.Duration, label string) int {
	if procs < 2 {
		procs = 2
	}
	if hosts < procs {
		fatal(fmt.Errorf("hosts %d < procs %d", hosts, procs))
	}
	dir, err := os.MkdirTemp("", "registrar-external-*")
	if err != nil {
		fatal(err)
	}
	defer os.RemoveAll(dir)
	bin := filepath.Join(dir, "etcdnode")
	build := exec.Command("go", "build", "-o", bin, "./cmd/etcdnode")
	build.Stdout = os.Stderr
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		fatal(fmt.Errorf("build etcdnode: %w", err))
	}
	members, err := startExternal(bin, dir, 3)
	if err != nil {
		fatal(err)
	}
	defer stopMembers(members)
	var eps []string
	for _, m := range members {
		eps = append(eps, m.client)
	}
	fmt.Fprintf(out, "{\"event\":\"external_etcd\",\"endpoints\":%q,\"procs\":%d,\"hosts\":%d}\n", strings.Join(eps, ","), procs, hosts)

	redialPath := filepath.Join(dir, "redial-at")
	if err := os.WriteFile(redialPath, []byte("0\n"), 0o644); err != nil {
		fatal(err)
	}
	self, err := os.Executable()
	if err != nil {
		self = os.Args[0]
	}
	// go run leaves a temp binary; re-exec that same binary.
	per := hosts / procs
	extra := hosts % procs
	type child struct {
		cmd *exec.Cmd
		out io.ReadCloser
	}
	var children []child
	base := 600000
	used := 0
	for p := 0; p < procs; p++ {
		n := per
		if p < extra {
			n++
		}
		args := []string{
			"-child",
			"-embed=false",
			"-hosts", strconv.Itoa(n),
			"-id-base", strconv.Itoa(base + used),
			"-endpoints", strings.Join(eps, ","),
			"-ttl", strconv.FormatInt(ttl, 10),
			"-renew", renew.String(),
			"-keep", keep.String(),
			"-kill-after", "0s",
			"-register-timeout", registerTimeout.String(),
			"-redial-inflight", "0",
			"-redial-file", redialPath,
			"-stagger-base", strconv.Itoa(used),
			"-label", fmt.Sprintf("%s-p%d", label, p),
		}
		used += n
		cmd := exec.Command(self, args...)
		cmd.Stderr = os.Stderr
		rc, err := cmd.StdoutPipe()
		if err != nil {
			fatal(err)
		}
		if err := cmd.Start(); err != nil {
			fatal(err)
		}
		children = append(children, child{cmd: cmd, out: rc})
	}

	var mu sync.Mutex
	var registered, redialEvents, regEvents int
	ready := make(chan struct{})
	redialDone := make(chan struct{})
	var once sync.Once
	var onceRedial sync.Once
	summaries := make([]map[string]any, len(children))
	var wg sync.WaitGroup
	for i, ch := range children {
		wg.Add(1)
		go func(i int, rc io.ReadCloser) {
			defer wg.Done()
			sc := bufio.NewScanner(rc)
			sc.Buffer(make([]byte, 64*1024), 1024*1024)
			for sc.Scan() {
				line := sc.Text()
				fmt.Fprintln(out, line)
				var ev map[string]any
				if json.Unmarshal([]byte(line), &ev) != nil {
					continue
				}
				switch ev["event"] {
				case "registered":
					mu.Lock()
					regEvents++
					if up, ok := asInt(ev["up"]); ok {
						registered += up
					}
					if regEvents >= procs {
						once.Do(func() { close(ready) })
					}
					mu.Unlock()
				case "redial":
					mu.Lock()
					redialEvents++
					if redialEvents >= procs {
						onceRedial.Do(func() { close(redialDone) })
					}
					mu.Unlock()
				case "summary":
					mu.Lock()
					summaries[i] = ev
					mu.Unlock()
				}
			}
		}(i, ch.out)
	}
	select {
	case <-ready:
	case <-time.After(registerTimeout):
		fmt.Fprintf(out, "{\"event\":\"register_timeout\",\"registered\":%d}\n", registered)
	}
	when := time.Now().Add(time.Second).UnixMilli()
	if err := os.WriteFile(redialPath, []byte(strconv.FormatInt(when, 10)+"\n"), 0o644); err != nil {
		fatal(err)
	}
	fmt.Fprintf(out, "{\"event\":\"redial_at\",\"unix_ms\":%d}\n", when)
	select {
	case <-redialDone:
	case <-time.After(registerTimeout):
		fmt.Fprintf(out, "{\"event\":\"redial_timeout\"}\n")
	}
	killed := ""
	if killAfter > 0 {
		time.Sleep(killAfter)
		if ep, err := killExternalLeader(eps, members); err != nil {
			fmt.Fprintf(out, "{\"event\":\"kill_leader_error\",\"err\":%q}\n", err.Error())
		} else {
			killed = ep
			fmt.Fprintf(out, "{\"event\":\"kill_leader\",\"endpoint\":%q}\n", ep)
		}
	}
	for _, ch := range children {
		_ = ch.cmd.Wait()
	}
	wg.Wait()
	agg := map[string]any{
		"event":           "aggregate",
		"label":           label,
		"hosts":           hosts,
		"procs":           procs,
		"redial_inflight": 0,
		"external_etcd":   true,
		"endpoints":       len(eps),
		"leader_killed":   killed != "",
		"leader_endpoint": killed,
	}
	sumInto(agg, summaries, "registered", "ready_err", "drain_ok", "drain_err", "keepalive_ok", "keepalive_err", "lease_lost", "watch_put", "watch_del")
	b, _ := json.Marshal(agg)
	fmt.Fprintln(out, string(b))
	reg, _ := asInt(agg["registered"])
	dOK, _ := asInt(agg["drain_ok"])
	dErr, _ := asInt(agg["drain_err"])
	lost, _ := asInt(agg["lease_lost"])
	wdel, _ := asInt(agg["watch_del"])
	rerr, _ := asInt(agg["ready_err"])
	if reg != hosts || rerr != 0 || dOK != hosts || dErr != 0 || lost != 0 || wdel != dOK {
		return 1
	}
	return 0
}

func sumInto(dst map[string]any, rows []map[string]any, keys ...string) {
	for _, k := range keys {
		var n int
		for _, row := range rows {
			if row == nil {
				continue
			}
			if v, ok := asInt(row[k]); ok {
				n += v
			}
		}
		dst[k] = n
	}
}

func asInt(v any) (int, bool) {
	switch n := v.(type) {
	case float64:
		return int(n), true
	case int:
		return n, true
	case json.Number:
		i, err := n.Int64()
		return int(i), err == nil
	default:
		return 0, false
	}
}

func startExternal(bin, dir string, n int) ([]*etcdProc, error) {
	peers := make([]string, n)
	clients := make([]string, n)
	for i := 0; i < n; i++ {
		pp, err := freePort()
		if err != nil {
			return nil, err
		}
		cp, err := freePort()
		if err != nil {
			return nil, err
		}
		peers[i] = fmt.Sprintf("http://127.0.0.1:%d", pp)
		clients[i] = fmt.Sprintf("http://127.0.0.1:%d", cp)
	}
	var initial []string
	members := make([]*etcdProc, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("e%d", i)
		initial = append(initial, name+"="+peers[i])
		members[i] = &etcdProc{name: name, client: clients[i]}
	}
	token := fmt.Sprintf("relay-%d", time.Now().UnixNano())
	for i := 0; i < n; i++ {
		data := filepath.Join(dir, members[i].name)
		cmd := exec.Command(bin,
			"-name", members[i].name,
			"-data", data,
			"-listen-client", clients[i],
			"-advertise-client", clients[i],
			"-listen-peer", peers[i],
			"-advertise-peer", peers[i],
			"-initial-cluster", strings.Join(initial, ","),
			"-token", token,
		)
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			stopMembers(members)
			return nil, err
		}
		members[i].cmd = cmd
	}
	deadline := time.Now().Add(40 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		ok := true
		for _, m := range members {
			c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			cli, err := clientv3.New(clientv3.Config{Endpoints: []string{m.client}, DialTimeout: 2 * time.Second, Logger: zap.NewNop()})
			if err != nil {
				cancel()
				last = err
				ok = false
				break
			}
			_, err = cli.Status(c, m.client)
			cli.Close()
			cancel()
			if err != nil {
				last = err
				ok = false
				break
			}
		}
		if ok {
			return members, nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	stopMembers(members)
	if last == nil {
		last = fmt.Errorf("etcd not ready")
	}
	return nil, last
}

func killExternalLeader(eps []string, members []*etcdProc) (string, error) {
	c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cli, err := clientv3.New(clientv3.Config{Endpoints: eps, DialTimeout: 2 * time.Second, Logger: zap.NewNop()})
	if err != nil {
		return "", err
	}
	defer cli.Close()
	for _, ep := range eps {
		st, err := cli.Status(c, ep)
		if err != nil || st.Leader == 0 || st.Header.MemberId != st.Leader {
			continue
		}
		for _, m := range members {
			if m.client == ep && m.cmd != nil && m.cmd.Process != nil {
				_ = m.cmd.Process.Kill()
				return ep, nil
			}
		}
	}
	return "", fmt.Errorf("no leader")
}

func stopMembers(members []*etcdProc) {
	for _, m := range members {
		if m != nil && m.cmd != nil && m.cmd.Process != nil {
			_ = m.cmd.Process.Kill()
		}
	}
	for _, m := range members {
		if m != nil && m.cmd != nil && m.cmd.Process != nil {
			_, _ = m.cmd.Process.Wait()
		}
	}
}

func freePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	p := ln.Addr().(*net.TCPAddr).Port
	return p, ln.Close()
}
