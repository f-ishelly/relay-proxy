//go:build docfixture

package proxy

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/f-ishelly/relay-proxy/internal/docfixture/l1metrics"
)

var (
	gostA   string
	gostB   string
	gostBad string
)

func TestMain(m *testing.M) {
	var cmds []*exec.Cmd
	start := func(user, pass string) string {
		addr, cmd, err := startGost(user, pass)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		cmds = append(cmds, cmd)
		return addr
	}
	gostA = start("app@node-a", "s3cret")
	gostB = start("app@node-b", "s3cret")
	gostBad = start("app@node-a", "nope")
	code := m.Run()
	for _, c := range cmds {
		_ = c.Process.Kill()
	}
	os.Exit(code)
}

func startGost(user, pass string) (string, *exec.Cmd, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	dir, err := os.MkdirTemp("", "gostcfg")
	if err != nil {
		return "", nil, err
	}
	cfg := filepath.Join(dir, "gost.yaml")
	body := fmt.Sprintf(`services:
  - name: l2
    addr: "127.0.0.1:%d"
    handler:
      type: http
      auth:
        username: %q
        password: %q
      metadata:
        sniffing: false
    listener:
      type: tcp
`, port, user, pass)
	if err := os.WriteFile(cfg, []byte(body), 0o644); err != nil {
		return "", nil, err
	}
	cmd := exec.Command("/tmp/gost", "-C", cfg)
	var logs bytes.Buffer
	cmd.Stdout = &logs
	cmd.Stderr = &logs
	if err := cmd.Start(); err != nil {
		return "", nil, err
	}
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			c.Close()
			return addr, cmd, nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	_ = cmd.Process.Kill()
	return "", nil, fmt.Errorf("gost %s not listening: %s", addr, logs.String())
}

type mapResolver struct{ m map[string]string }

func (r mapResolver) Resolve(id string) (string, error) {
	a, ok := r.m[id]
	if !ok {
		return "", ErrUnknownNode
	}
	if a == "" {
		return "", ErrNoHealthyAddr
	}
	return a, nil
}

type staticResolver struct {
	addr string
	err  error
}

func (s staticResolver) Resolve(string) (string, error) {
	if s.err != nil {
		return "", s.err
	}
	return s.addr, nil
}

var handlers sync.WaitGroup

func startL1(t *testing.T, r Resolver) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			handlers.Add(1)
			go func() {
				defer handlers.Done()
				_ = serveProxyConn(c, r)
			}()
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().String()
}

func serveRaw(t *testing.T, fn func(net.Conn)) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go fn(c)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().String()
}

func basicToken(user, pass string) string {
	return base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))
}

func assertNoConnectMetrics(t *testing.T, snap l1metrics.Calls) {
	t.Helper()
	if len(snap.Rejects) != 0 || len(snap.Fails) != 0 || len(snap.Observes) != 0 || snap.Entered != 0 {
		t.Fatalf("plain HTTP touched CONNECT metrics: rejects=%v fails=%v observes=%v entered=%d",
			snap.Rejects, snap.Fails, snap.Observes, snap.Entered)
	}
}

func assertSetup(t *testing.T, snap l1metrics.Calls, want ...string) {
	t.Helper()
	if len(snap.HTTPSetup) != len(want) {
		t.Fatalf("setup %#v want %#v", snap.HTTPSetup, want)
	}
	for i, w := range want {
		if snap.HTTPSetup[i] != w {
			t.Fatalf("setup %#v want %#v", snap.HTTPSetup, want)
		}
	}
}

func assertBalanced(t *testing.T, snap l1metrics.Calls) {
	t.Helper()
	if snap.HTTPLeft != snap.HTTPEntered || snap.HTTPTunnels != snap.HTTPEntered {
		t.Fatalf("tunnel unbalanced entered=%d left=%d duration=%d", snap.HTTPEntered, snap.HTTPLeft, snap.HTTPTunnels)
	}
}

func waitCalls(t *testing.T, pred func(l1metrics.Calls) bool) l1metrics.Calls {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var snap l1metrics.Calls
	for {
		snap = l1metrics.Snapshot()
		if pred(snap) || !time.Now().Before(deadline) {
			return snap
		}
		time.Sleep(5 * time.Millisecond)
	}
}

type scriptConn struct {
	net.Conn
	write    func([]byte) (int, error)
	readDead func(time.Time) error
}

func (s scriptConn) Write(p []byte) (int, error) {
	if s.write != nil {
		return s.write(p)
	}
	return s.Conn.Write(p)
}

func (s scriptConn) SetReadDeadline(deadline time.Time) error {
	if s.readDead != nil {
		return s.readDead(deadline)
	}
	return s.Conn.SetReadDeadline(deadline)
}

func scriptedDial(t *testing.T, wrap func(net.Conn) net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				_, _ = io.Copy(io.Discard, c)
				_ = c.Close()
			}()
		}
	}()
	orig := dialUpstream
	dialUpstream = func(string) (net.Conn, error) {
		c, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			return nil, err
		}
		if wrap != nil {
			return wrap(c), nil
		}
		return c, nil
	}
	t.Cleanup(func() {
		handlers.Wait()
		dialUpstream = orig
	})
}

func countDials(t *testing.T) *atomic.Int32 {
	t.Helper()
	var n atomic.Int32
	orig := dialUpstream
	dialUpstream = func(addr string) (net.Conn, error) {
		n.Add(1)
		return orig(addr)
	}
	t.Cleanup(func() {
		handlers.Wait()
		dialUpstream = orig
	})
	return &n
}

func doRequest(t *testing.T, addr, raw string) []byte {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	return writeAndRead(t, c, raw)
}

func writeAndRead(t *testing.T, c net.Conn, raw string) []byte {
	t.Helper()
	_ = c.SetDeadline(time.Now().Add(4 * time.Second))
	if _, err := io.WriteString(c, raw); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	head, err := readHTTPHead(br)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	body, err := readMessageBody(br, head, false)
	if err != nil {
		t.Fatal(err)
	}
	return append(append([]byte{}, head...), body...)
}

func readMessageBody(br *bufio.Reader, head []byte, headMethod bool) ([]byte, error) {
	start, headers, err := splitHead(head)
	if err != nil {
		return nil, err
	}
	if headMethod {
		return nil, nil
	}
	code := 0
	if fields := strings.Split(start, " "); len(fields) >= 2 {
		code, _ = strconv.Atoi(fields[1])
	}
	if code > 0 && (code < 200 || code == 204 || code == 304 || code == 101) {
		return nil, nil
	}
	chunked := false
	cl := -1
	for _, ln := range headers {
		name, _, ok := headerName(ln)
		if !ok {
			continue
		}
		switch strings.ToLower(string(name)) {
		case "transfer-encoding":
			if hasToken(splitTokens(headerValue(ln)), "chunked") {
				chunked = true
			}
		case "content-length":
			n, err := strconv.Atoi(strings.TrimSpace(headerValue(ln)))
			if err != nil {
				return nil, err
			}
			cl = n
		}
	}
	if chunked {
		var buf bytes.Buffer
		if _, err := copyChunked(&buf, br); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}
	if cl > 0 {
		b := make([]byte, cl)
		if _, err := io.ReadFull(br, b); err != nil {
			return nil, err
		}
		return b, nil
	}
	return nil, nil
}

func readOriginRequest(br *bufio.Reader) ([]byte, error) {
	head, err := readHTTPHead(br)
	if err != nil {
		return nil, err
	}
	body, err := requestBody(br, head)
	if err != nil {
		return nil, err
	}
	return append(head, body...), nil
}

func requestBody(br *bufio.Reader, head []byte) ([]byte, error) {
	_, headers, err := splitHead(head)
	if err != nil {
		return nil, err
	}
	chunked := false
	cl := -1
	for _, ln := range headers {
		name, _, ok := headerName(ln)
		if !ok {
			continue
		}
		switch strings.ToLower(string(name)) {
		case "transfer-encoding":
			if hasToken(splitTokens(headerValue(ln)), "chunked") {
				chunked = true
			}
		case "content-length":
			n, err := strconv.Atoi(strings.TrimSpace(headerValue(ln)))
			if err != nil {
				return nil, err
			}
			cl = n
		}
	}
	if chunked {
		var buf bytes.Buffer
		if _, err := copyChunked(&buf, br); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}
	if cl > 0 {
		b := make([]byte, cl)
		if _, err := io.ReadFull(br, b); err != nil {
			return nil, err
		}
		return b, nil
	}
	return nil, nil
}

func mustStatus(t *testing.T, resp []byte, code int) {
	t.Helper()
	line := string(resp)
	if i := strings.IndexByte(line, '\n'); i >= 0 {
		line = line[:i]
	}
	fields := strings.Fields(strings.TrimSpace(line))
	if len(fields) < 2 || fields[1] != strconv.Itoa(code) {
		t.Fatalf("status got %q want %d\n%s", line, code, strconv.Quote(string(resp)))
	}
}

func TestHeaderOrderAndCaseThroughGost(t *testing.T) {
	gotCh := make(chan []byte, 1)
	origin := serveRaw(t, func(c net.Conn) {
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(4 * time.Second))
		br := bufio.NewReader(c)
		raw, err := readOriginRequest(br)
		if err != nil {
			return
		}
		gotCh <- raw
		_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nx-Odd: YeS\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
	})
	l1 := startL1(t, mapResolver{m: map[string]string{"node-a": gostA}})
	req := fmt.Sprintf(
		"GET http://%s/PaTh?q=1 HTTP/1.1\r\n"+
			"hOsT: KeepMe.Example\r\n"+
			"X-Zz-Trace: one\r\n"+
			"x-custom: MiXeD\r\n"+
			"X-Custom: second\r\n"+
			"X-Forwarded-For: client-set\r\n"+
			"Proxy-Authorization: Basic %s\r\n"+
			"Proxy-Connection: keep-alive\r\n"+
			"Connection:  keep-alive\r\n"+
			"Content-Length: 4\r\n"+
			"\r\n"+
			"body",
		origin, basicToken("app@node-a", "s3cret"))
	resp := doRequest(t, l1, req)
	got := <-gotCh
	want := fmt.Sprintf(
		"GET /PaTh?q=1 HTTP/1.1\r\n" +
			"hOsT: KeepMe.Example\r\n" +
			"X-Zz-Trace: one\r\n" +
			"x-custom: MiXeD\r\n" +
			"X-Custom: second\r\n" +
			"X-Forwarded-For: client-set\r\n" +
			"Connection:  keep-alive\r\n" +
			"Content-Length: 4\r\n" +
			"\r\n" +
			"body")
	if string(got) != want {
		t.Fatalf("origin bytes\n got %s\nwant %s", strconv.Quote(string(got)), strconv.Quote(want))
	}
	if bytes.Contains(got, []byte("Proxy-Authorization")) || bytes.Contains(got, []byte("Proxy-Connection")) ||
		bytes.Contains(got, []byte("Via:")) || bytes.Contains(got, []byte("User-Agent")) {
		t.Fatalf("injected or hop-by-hop header: %s", strconv.Quote(string(got)))
	}
	if !bytes.Contains(resp, []byte("x-Odd: YeS")) || !bytes.Contains(resp, []byte("ok")) {
		t.Fatalf("response not preserved: %s", strconv.Quote(string(resp)))
	}
}

func TestChunkedAndContentLength(t *testing.T) {
	gotCh := make(chan []byte, 1)
	respRaw := []byte("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nx-Odd: AbC\r\n\r\n5\r\nworld\r\n0\r\n\r\n")
	origin := serveRaw(t, func(c net.Conn) {
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(4 * time.Second))
		br := bufio.NewReader(c)
		raw, err := readOriginRequest(br)
		if err != nil {
			return
		}
		gotCh <- raw
		_, _ = c.Write(respRaw)
	})
	l1 := startL1(t, mapResolver{m: map[string]string{"node-a": gostA}})
	req := fmt.Sprintf(
		"POST http://%s/up HTTP/1.1\r\n"+
			"Host: h\r\n"+
			"Transfer-Encoding: chunked\r\n"+
			"Content-Length: 999\r\n"+
			"Proxy-Authorization: Basic %s\r\n"+
			"\r\n"+
			"5;ext=1\r\nhello\r\n0\r\nX-Trail: v\r\n\r\n",
		origin, basicToken("app@node-a", "s3cret"))
	resp := doRequest(t, l1, req)
	got := <-gotCh
	want := "POST /up HTTP/1.1\r\n" +
		"Host: h\r\n" +
		"Transfer-Encoding: chunked\r\n" +
		"\r\n" +
		"5;ext=1\r\nhello\r\n0\r\nX-Trail: v\r\n\r\n"
	if string(got) != want {
		t.Fatalf("chunked request\n got %s\nwant %s", strconv.Quote(string(got)), strconv.Quote(want))
	}
	if !bytes.Equal(resp, respRaw) {
		t.Fatalf("chunked response\n got %s\nwant %s", strconv.Quote(string(resp)), strconv.Quote(string(respRaw)))
	}
}

func TestExpect100Continue(t *testing.T) {
	l1metrics.Reset()
	startedBody := make(chan struct{})
	sawBody := make(chan string, 1)
	origin := serveRaw(t, func(c net.Conn) {
		defer c.Close()
		br := bufio.NewReader(c)
		_ = c.SetDeadline(time.Now().Add(4 * time.Second))
		if _, err := readHTTPHead(br); err != nil {
			return
		}
		_ = c.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		extra := make([]byte, 8)
		n, _ := br.Read(extra)
		if n > 0 {
			sawBody <- "early:" + string(extra[:n])
			return
		}
		close(startedBody)
		if _, err := io.WriteString(c, "HTTP/1.1 100 Continue\r\nExtra: keep\r\n\r\n"); err != nil {
			return
		}
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 5)
		if _, err := io.ReadFull(br, buf); err != nil {
			sawBody <- "read:" + err.Error()
			return
		}
		sawBody <- string(buf)
		_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello")
	})
	l1 := startL1(t, mapResolver{m: map[string]string{"node-a": gostA}})
	c, err := net.Dial("tcp", l1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(4 * time.Second))
	req := fmt.Sprintf("POST http://%s/e HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\nExpect: 100-continue\r\nProxy-Authorization: Basic %s\r\n\r\n",
		origin, basicToken("app@node-a", "s3cret"))
	if _, err := io.WriteString(c, req); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	cont, err := readHTTPHead(br)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(cont, []byte("100 Continue")) || !bytes.Contains(cont, []byte("Extra: keep")) {
		t.Fatalf("100 not preserved: %s", strconv.Quote(string(cont)))
	}
	select {
	case <-startedBody:
	case early := <-sawBody:
		t.Fatal(early)
	case <-time.After(2 * time.Second):
		t.Fatal("origin did not observe the pre-body window")
	}
	if _, err := io.WriteString(c, "hello"); err != nil {
		t.Fatal(err)
	}
	final, err := readHTTPHead(br)
	if err != nil {
		t.Fatal(err)
	}
	body, err := readMessageBody(br, final, false)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "hello" {
		t.Fatalf("final body %q", body)
	}
	select {
	case got := <-sawBody:
		if got != "hello" {
			t.Fatal(got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("origin did not see body")
	}
	snap := waitCalls(t, func(c l1metrics.Calls) bool {
		return len(c.HTTPObserves) == 1 && len(c.Events) == 1 && c.HTTPEntered == 1 && c.HTTPLeft == 1 && c.HTTPTunnels == 1
	})
	if snap.HTTPObserves[0] != l1metrics.ResultOK || len(snap.HTTPObserves) != 1 {
		t.Fatalf("100-continue observes %#v", snap.HTTPObserves)
	}
	assertSetup(t, snap, l1metrics.ResultOK)
	assertBalanced(t, snap)
	if len(snap.HTTPFails) != 0 || len(snap.HTTPAborts) != 0 {
		t.Fatalf("100-continue fail/abort %#v %#v", snap.HTTPFails, snap.HTTPAborts)
	}
	if snap.Events[0].Protocol != l1metrics.ProtocolHTTP || snap.Events[0].Result != l1metrics.ResultOK || snap.Events[0].V != 2 {
		t.Fatalf("event %+v", snap.Events[0])
	}
	assertNoConnectMetrics(t, snap)
}

func TestExpect100RejectedDoesNotForwardBody(t *testing.T) {
	l1metrics.Reset()
	saw := make(chan string, 1)
	origin := serveRaw(t, func(c net.Conn) {
		defer c.Close()
		br := bufio.NewReader(c)
		_ = c.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := readHTTPHead(br); err != nil {
			saw <- "head:" + err.Error()
			return
		}
		_ = c.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		extra := make([]byte, 8)
		n, _ := br.Read(extra)
		if n > 0 {
			saw <- "early:" + string(extra[:n])
			return
		}
		_, _ = io.WriteString(c, "HTTP/1.1 417 Expectation Failed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
		_ = c.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
		n, _ = br.Read(extra)
		if n > 0 {
			saw <- "late:" + string(extra[:n])
			return
		}
		saw <- "clean"
	})
	l1 := startL1(t, mapResolver{m: map[string]string{"node-a": gostA}})
	req := fmt.Sprintf("POST http://%s/e HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\nExpect: 100-continue\r\nProxy-Authorization: Basic %s\r\n\r\nhello",
		origin, basicToken("app@node-a", "s3cret"))
	resp := doRequest(t, l1, req)
	mustStatus(t, resp, 417)
	select {
	case got := <-saw:
		if got != "clean" {
			t.Fatal(got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("origin did not finish")
	}
	snap := waitCalls(t, func(c l1metrics.Calls) bool {
		return len(c.HTTPObserves) == 1 && c.HTTPObserves[0] == l1metrics.ResultOK && len(c.HTTPSetup) == 1 && c.HTTPEntered == c.HTTPLeft && c.HTTPTunnels == c.HTTPEntered
	})
	if len(snap.HTTPFails) != 0 {
		t.Fatalf("417 counted as failure %#v", snap.HTTPFails)
	}
	assertSetup(t, snap, l1metrics.ResultOK)
	assertBalanced(t, snap)
	assertNoConnectMetrics(t, snap)
}

func TestWebSocketUpgrade(t *testing.T) {
	l1metrics.Reset()
	gotCh := make(chan []byte, 1)
	origin := serveRaw(t, func(c net.Conn) {
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(4 * time.Second))
		br := bufio.NewReader(c)
		head, err := readHTTPHead(br)
		if err != nil {
			return
		}
		gotCh <- append([]byte(nil), head...)
		if _, err := io.WriteString(c, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nx-Ws: AbC\r\n\r\n"); err != nil {
			return
		}
		buf := make([]byte, 4)
		if _, err := io.ReadFull(br, buf); err != nil {
			return
		}
		_, _ = c.Write(buf)
	})
	l1 := startL1(t, mapResolver{m: map[string]string{"node-a": gostA}})
	c, err := net.Dial("tcp", l1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(4 * time.Second))
	req := fmt.Sprintf("GET http://%s/ws HTTP/1.1\r\nHost: h\r\nUpGrade: WebSocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: abc\r\nProxy-Authorization: Basic %s\r\nProxy-Connection: keep-alive\r\n\r\n",
		origin, basicToken("app@node-a", "s3cret"))
	if _, err := io.WriteString(c, req); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	head, err := readHTTPHead(br)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(head, []byte("101 Switching Protocols")) || !bytes.Contains(head, []byte("x-Ws: AbC")) {
		t.Fatalf("101: %s", strconv.Quote(string(head)))
	}
	if _, err := io.WriteString(c, "ping"); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(br, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "ping" {
		t.Fatalf("echo %q", buf)
	}
	want := "GET /ws HTTP/1.1\r\nHost: h\r\nUpGrade: WebSocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: abc\r\n\r\n"
	got := <-gotCh
	if string(got) != want {
		t.Fatalf("upgrade request\n got %s\nwant %s", strconv.Quote(string(got)), strconv.Quote(want))
	}
	snap := waitCalls(t, func(c l1metrics.Calls) bool {
		return len(c.HTTPObserves) == 1 && c.HTTPUp >= 4 && c.HTTPDown >= 4 && c.HTTPEntered == 1
	})
	if snap.HTTPObserves[0] != l1metrics.ResultOK || len(snap.HTTPObserves) != 1 {
		t.Fatalf("101 observes %#v", snap.HTTPObserves)
	}
	assertSetup(t, snap, l1metrics.ResultOK)
	if len(snap.HTTPFails) != 0 || len(snap.HTTPAborts) != 0 {
		t.Fatalf("101 changed result %#v %#v", snap.HTTPFails, snap.HTTPAborts)
	}
	assertNoConnectMetrics(t, snap)
	_ = c.Close()
	snap = waitCalls(t, func(c l1metrics.Calls) bool {
		return c.HTTPLeft == 1 && c.HTTPTunnels == 1
	})
	if snap.HTTPLeft != 1 || snap.HTTPTunnels != 1 {
		t.Fatalf("101 tunnel left=%d duration=%d", snap.HTTPLeft, snap.HTTPTunnels)
	}
}

func TestKeepAliveDifferentHostsAndSameHostReuse(t *testing.T) {
	l1metrics.Reset()
	newOrigin := func() (string, *atomic.Int32, *[][]byte, *sync.Mutex) {
		var n atomic.Int32
		var mu sync.Mutex
		var reqs [][]byte
		addr := serveRaw(t, func(c net.Conn) {
			n.Add(1)
			defer c.Close()
			br := bufio.NewReader(c)
			_ = c.SetDeadline(time.Now().Add(4 * time.Second))
			for {
				raw, err := readOriginRequest(br)
				if err != nil {
					return
				}
				mu.Lock()
				reqs = append(reqs, raw)
				mu.Unlock()
				_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
			}
		})
		return addr, &n, &reqs, &mu
	}
	aAddr, aN, aReqs, aMu := newOrigin()
	bAddr, bN, bReqs, bMu := newOrigin()
	l1 := startL1(t, mapResolver{m: map[string]string{"node-a": gostA, "node-b": gostB}})
	c, err := net.Dial("tcp", l1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(4 * time.Second))
	br := bufio.NewReader(c)
	send := func(origin, user, path string) []byte {
		t.Helper()
		raw := fmt.Sprintf("GET http://%s%s HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
			origin, path, basicToken(user, "s3cret"))
		if _, err := io.WriteString(c, raw); err != nil {
			t.Fatal(err)
		}
		head, err := readHTTPHead(br)
		if err != nil {
			t.Fatal(err)
		}
		body, err := readMessageBody(br, head, false)
		if err != nil {
			t.Fatal(err)
		}
		if string(body) != "ok" {
			t.Fatalf("body %q head %s", body, strconv.Quote(string(head)))
		}
		return head
	}
	send(aAddr, "app@node-a", "/one")
	send(bAddr, "app@node-b", "/two")
	send(aAddr, "app@node-a", "/three")
	send(aAddr, "app@node-a", "/four")
	snap := waitCalls(t, func(c l1metrics.Calls) bool {
		return len(c.HTTPObserves) == 4 && len(c.Events) == 4 && c.HTTPEntered == 3 && c.HTTPLeft == 2
	})
	for _, result := range snap.HTTPObserves {
		if result != l1metrics.ResultOK {
			t.Fatalf("keep-alive observes %#v", snap.HTTPObserves)
		}
	}
	assertSetup(t, snap, l1metrics.ResultOK, l1metrics.ResultOK, l1metrics.ResultOK)
	if snap.HTTPEntered != 3 || snap.HTTPLeft != 2 {
		t.Fatalf("tunnels before close entered=%d left=%d", snap.HTTPEntered, snap.HTTPLeft)
	}
	for _, ev := range snap.Events {
		if ev.Protocol != l1metrics.ProtocolHTTP || ev.Result != l1metrics.ResultOK || ev.V != 2 {
			t.Fatalf("event %+v", ev)
		}
	}
	assertNoConnectMetrics(t, snap)
	_ = c.Close()
	if aN.Load() != 2 {
		t.Fatalf("origin A accepts=%d want 2 (switch away, then reuse)", aN.Load())
	}
	if bN.Load() != 1 {
		t.Fatalf("origin B accepts=%d want 1", bN.Load())
	}
	aMu.Lock()
	aCopy := append([][]byte(nil), (*aReqs)...)
	aMu.Unlock()
	bMu.Lock()
	bCopy := append([][]byte(nil), (*bReqs)...)
	bMu.Unlock()
	if len(bCopy) != 1 || !bytes.Contains(bCopy[0], []byte("GET /two ")) {
		t.Fatalf("origin B reqs %s", bCopy)
	}
	if len(aCopy) != 3 {
		t.Fatalf("origin A reqs %d", len(aCopy))
	}
	if !bytes.Contains(aCopy[0], []byte("GET /one ")) || !bytes.Contains(aCopy[1], []byte("GET /three ")) || !bytes.Contains(aCopy[2], []byte("GET /four ")) {
		t.Fatalf("A paths %s", aCopy)
	}
	handlers.Wait()
	snap = l1metrics.Snapshot()
	if snap.HTTPLeft != 3 || snap.HTTPTunnels != 3 || snap.HTTPEntered != 3 {
		t.Fatalf("tunnels after close entered=%d left=%d duration=%d", snap.HTTPEntered, snap.HTTPLeft, snap.HTTPTunnels)
	}
}

func TestLastAtNodeID(t *testing.T) {
	addr, cmd, err := startGost("user@corp@node-a", "s3cret")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	gotCh := make(chan []byte, 1)
	origin := serveRaw(t, func(c net.Conn) {
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(4 * time.Second))
		br := bufio.NewReader(c)
		raw, err := readOriginRequest(br)
		if err != nil {
			return
		}
		gotCh <- raw
		_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
	})
	l1 := startL1(t, mapResolver{m: map[string]string{"node-a": addr}})
	req := fmt.Sprintf("GET http://%s/z HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
		origin, basicToken("user@corp@node-a", "s3cret"))
	resp := doRequest(t, l1, req)
	mustStatus(t, resp, 200)
	got := <-gotCh
	if !bytes.HasPrefix(got, []byte("GET /z HTTP/1.1\r\n")) {
		t.Fatalf("origin %s", strconv.Quote(string(got)))
	}
}

func TestLocalRejectsDoNotDial(t *testing.T) {
	cases := []struct {
		name   string
		user   string
		pass   string
		target string
		status int
		reason string
	}{
		{"auth", "app@node-a", "wrong", "http://127.0.0.1/a", 407, l1metrics.ReasonRejectAuth},
		{"no-node", "app", "s3cret", "http://127.0.0.1/a", 400, l1metrics.ReasonRejectNoNode},
		{"origin-form", "app@node-a", "s3cret", "/a", 400, l1metrics.ReasonRejectBadTarget},
		{"https", "app@node-a", "s3cret", "https://example.com/a", 400, l1metrics.ReasonRejectBadTarget},
	}
	l1 := startL1(t, mapResolver{m: map[string]string{"node-a": gostA}})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l1metrics.Reset()
			dials := countDials(t)
			req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
				tc.target, basicToken(tc.user, tc.pass))
			resp := doRequest(t, l1, req)
			mustStatus(t, resp, tc.status)
			if dials.Load() != 0 {
				t.Fatalf("dialed %d", dials.Load())
			}
			handlers.Wait()
			snap := l1metrics.Snapshot()
			if len(snap.HTTPRejects) != 1 || snap.HTTPRejects[0] != tc.reason {
				t.Fatalf("rejects %#v", snap.HTTPRejects)
			}
			if len(snap.HTTPObserves) != 0 || len(snap.HTTPFails) != 0 {
				t.Fatalf("reject observed latency or fail %#v %#v", snap.HTTPObserves, snap.HTTPFails)
			}
			wantEvents := 0
			if tc.reason == l1metrics.ReasonRejectNoNode || tc.reason == l1metrics.ReasonRejectBadTarget {
				wantEvents = 1
			}
			if len(snap.Events) != wantEvents {
				t.Fatalf("events %d want %d (%+v)", len(snap.Events), wantEvents, snap.Events)
			}
			if wantEvents == 1 && (snap.Events[0].Protocol != l1metrics.ProtocolHTTP || snap.Events[0].Result != tc.reason || snap.Events[0].HandshakeMs != 0) {
				t.Fatalf("event %+v", snap.Events[0])
			}
			assertNoConnectMetrics(t, snap)
			if tc.status == 407 && !bytes.Contains(resp, []byte("Proxy-Authenticate:")) {
				t.Fatalf("missing challenge: %s", resp)
			}
		})
	}
}

func TestBadCredentialDoesNotDial(t *testing.T) {
	l1metrics.Reset()
	dials := countDials(t)
	l1 := startL1(t, mapResolver{m: map[string]string{"node-a": gostA}})
	token := base64.StdEncoding.EncodeToString([]byte("app@node-a:bad\nsecret"))
	req := fmt.Sprintf("GET http://127.0.0.1/a HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n", token)
	resp := doRequest(t, l1, req)
	mustStatus(t, resp, 400)
	if dials.Load() != 0 {
		t.Fatalf("dialed %d", dials.Load())
	}
	handlers.Wait()
	snap := l1metrics.Snapshot()
	if len(snap.HTTPRejects) != 1 || snap.HTTPRejects[0] != l1metrics.ReasonRejectBadCredential {
		t.Fatalf("rejects %#v", snap.HTTPRejects)
	}
	if len(snap.Events) != 0 || len(snap.HTTPObserves) != 0 {
		t.Fatalf("bad credential emitted %+v observes %#v", snap.Events, snap.HTTPObserves)
	}
	assertNoConnectMetrics(t, snap)
}

func TestL2Auth407(t *testing.T) {
	l1metrics.Reset()
	l1 := startL1(t, mapResolver{m: map[string]string{"node-a": gostBad}})
	req := fmt.Sprintf("GET http://127.0.0.1/a HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
		basicToken("app@node-a", "s3cret"))
	resp := doRequest(t, l1, req)
	mustStatus(t, resp, 407)
	if !bytes.Contains(resp, []byte("Proxy-Authenticate:")) {
		t.Fatalf("challenge not passed: %s", strconv.Quote(string(resp)))
	}
	handlers.Wait()
	snap := l1metrics.Snapshot()
	if len(snap.HTTPFails) != 1 || snap.HTTPFails[0][1] != l1metrics.ReasonL2Auth {
		t.Fatalf("fails %#v", snap.HTTPFails)
	}
	if len(snap.HTTPObserves) != 1 || snap.HTTPObserves[0] != l1metrics.ReasonL2Auth || snap.HTTPEntered != 0 {
		t.Fatalf("l2_auth observe %#v entered %d", snap.HTTPObserves, snap.HTTPEntered)
	}
	assertSetup(t, snap, l1metrics.ReasonL2Auth)
	assertBalanced(t, snap)
	assertNoConnectMetrics(t, snap)
}

func TestL2Status502(t *testing.T) {
	l1metrics.Reset()
	bad := serveRaw(t, func(c net.Conn) {
		defer c.Close()
		buf := make([]byte, 2048)
		_, _ = c.Read(buf)
		_, _ = io.WriteString(c, "HTTP/1.1 500 Nope\r\nContent-Length: 0\r\n\r\n")
	})
	l1 := startL1(t, mapResolver{m: map[string]string{"node-a": bad}})
	req := fmt.Sprintf("GET http://127.0.0.1/a HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
		basicToken("app@node-a", "s3cret"))
	resp := doRequest(t, l1, req)
	mustStatus(t, resp, 502)
	handlers.Wait()
	snap := l1metrics.Snapshot()
	if len(snap.HTTPFails) != 1 || snap.HTTPFails[0][1] != l1metrics.ReasonL2Status || snap.HTTPFails[0][0] != "node-a" {
		t.Fatalf("fails %#v", snap.HTTPFails)
	}
	if len(snap.HTTPObserves) != 1 || snap.HTTPObserves[0] != l1metrics.ReasonL2Status || snap.HTTPEntered != 0 {
		t.Fatalf("l2_status observe %#v entered %d", snap.HTTPObserves, snap.HTTPEntered)
	}
	assertSetup(t, snap, l1metrics.ReasonL2Status)
	assertBalanced(t, snap)
	assertNoConnectMetrics(t, snap)
}

func TestDialAndResolve502(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := ln.Addr().String()
	_ = ln.Close()

	t.Run("dial", func(t *testing.T) {
		l1metrics.Reset()
		l1 := startL1(t, mapResolver{m: map[string]string{"node-a": closed}})
		resp := doRequest(t, l1, fmt.Sprintf("GET http://127.0.0.1/a HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
			basicToken("app@node-a", "s3cret")))
		mustStatus(t, resp, 502)
		handlers.Wait()
		snap := l1metrics.Snapshot()
		if len(snap.HTTPFails) != 1 || snap.HTTPFails[0] != [2]string{"node-a", l1metrics.ReasonDial} {
			t.Fatalf("fails %#v", snap.HTTPFails)
		}
		if len(snap.HTTPObserves) != 1 || snap.HTTPObserves[0] != l1metrics.ReasonDial {
			t.Fatalf("dial observe %#v", snap.HTTPObserves)
		}
		assertSetup(t, snap, l1metrics.ReasonDial)
		assertBalanced(t, snap)
		assertNoConnectMetrics(t, snap)
	})
	t.Run("unknown", func(t *testing.T) {
		l1metrics.Reset()
		l1 := startL1(t, staticResolver{err: ErrUnknownNode})
		resp := doRequest(t, l1, fmt.Sprintf("GET http://127.0.0.1/a HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
			basicToken("app@node-a", "s3cret")))
		mustStatus(t, resp, 502)
		handlers.Wait()
		snap := l1metrics.Snapshot()
		if len(snap.HTTPFails) != 1 || snap.HTTPFails[0] != [2]string{l1metrics.NodeUnknown, l1metrics.ReasonNoUpstream} {
			t.Fatalf("fails %#v", snap.HTTPFails)
		}
		if len(snap.HTTPObserves) != 0 {
			t.Fatalf("no_upstream observed %#v", snap.HTTPObserves)
		}
		assertSetup(t, snap)
		assertBalanced(t, snap)
		if len(snap.Events) != 1 || snap.Events[0].Result != l1metrics.ReasonNoUpstream || snap.Events[0].Protocol != l1metrics.ProtocolHTTP {
			t.Fatalf("event %+v", snap.Events)
		}
		assertNoConnectMetrics(t, snap)
	})
	t.Run("no-healthy", func(t *testing.T) {
		l1metrics.Reset()
		l1 := startL1(t, staticResolver{err: ErrNoHealthyAddr})
		resp := doRequest(t, l1, fmt.Sprintf("GET http://127.0.0.1/a HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
			basicToken("app@node-a", "s3cret")))
		mustStatus(t, resp, 502)
		handlers.Wait()
		snap := l1metrics.Snapshot()
		if len(snap.HTTPFails) != 1 || snap.HTTPFails[0] != [2]string{"node-a", l1metrics.ReasonNoUpstream} {
			t.Fatalf("fails %#v", snap.HTTPFails)
		}
		if len(snap.HTTPObserves) != 0 {
			t.Fatalf("no_upstream observed %#v", snap.HTTPObserves)
		}
		assertSetup(t, snap)
		assertBalanced(t, snap)
		assertNoConnectMetrics(t, snap)
	})
	t.Run("empty", func(t *testing.T) {
		l1metrics.Reset()
		l1 := startL1(t, staticResolver{})
		resp := doRequest(t, l1, fmt.Sprintf("GET http://127.0.0.1/a HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
			basicToken("app@node-a", "s3cret")))
		mustStatus(t, resp, 502)
		handlers.Wait()
		snap := l1metrics.Snapshot()
		if len(snap.HTTPFails) != 1 || snap.HTTPFails[0] != [2]string{l1metrics.NodeUnknown, l1metrics.ReasonNoUpstream} {
			t.Fatalf("fails %#v", snap.HTTPFails)
		}
		if len(snap.HTTPObserves) != 0 {
			t.Fatalf("no_upstream observed %#v", snap.HTTPObserves)
		}
		assertSetup(t, snap)
		assertBalanced(t, snap)
		assertNoConnectMetrics(t, snap)
	})
}

func TestL2ResponseTimeout(t *testing.T) {
	old := l2ResponseTimeout
	l2ResponseTimeout = 300 * time.Millisecond
	t.Cleanup(func() {
		handlers.Wait()
		l2ResponseTimeout = old
	})
	blackhole := serveRaw(t, func(c net.Conn) {
		time.Sleep(5 * time.Second)
		c.Close()
	})
	l1metrics.Reset()
	l1 := startL1(t, mapResolver{m: map[string]string{"node-a": blackhole}})
	start := time.Now()
	resp := doRequest(t, l1, fmt.Sprintf("GET http://127.0.0.1/a HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
		basicToken("app@node-a", "s3cret")))
	if time.Since(start) > 2*time.Second {
		t.Fatalf("timeout too slow: %s", time.Since(start))
	}
	mustStatus(t, resp, 502)
	handlers.Wait()
	snap := l1metrics.Snapshot()
	if len(snap.HTTPFails) != 1 || snap.HTTPFails[0][1] != l1metrics.ReasonL2Response {
		t.Fatalf("fails %#v", snap.HTTPFails)
	}
	if len(snap.HTTPObserves) != 1 || snap.HTTPObserves[0] != l1metrics.ReasonL2Response || snap.HTTPEntered != 0 {
		t.Fatalf("l2_response observe %#v entered %d", snap.HTTPObserves, snap.HTTPEntered)
	}
	assertSetup(t, snap, l1metrics.ReasonL2Response)
	assertBalanced(t, snap)
	assertNoConnectMetrics(t, snap)
}

func TestClientIdleTimeout(t *testing.T) {
	old := tunnelIdleTimeout
	tunnelIdleTimeout = 200 * time.Millisecond
	t.Cleanup(func() {
		handlers.Wait()
		tunnelIdleTimeout = old
	})
	l1 := startL1(t, mapResolver{m: map[string]string{"node-a": gostA}})
	c, err := net.Dial("tcp", l1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	_, err = c.Read(buf)
	if err == nil {
		t.Fatal("expected idle close")
	}
}

func TestAbsoluteFormMutations(t *testing.T) {
	type gotT struct {
		connect string
		http    []byte
	}
	got := make(chan gotT, 1)
	fake := serveRaw(t, func(c net.Conn) {
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(3 * time.Second))
		br := bufio.NewReader(c)
		head, err := readHTTPHead(br)
		if err != nil {
			return
		}
		start, _, _ := splitHead(head)
		_, _ = io.WriteString(c, "HTTP/1.1 200 Connection Established\r\n\r\n")
		raw, err := readOriginRequest(br)
		if err != nil {
			return
		}
		_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
		got <- gotT{connect: start, http: raw}
	})
	l1 := startL1(t, mapResolver{m: map[string]string{"node-a": fake}})
	req := fmt.Sprintf("GET http://user:pw@127.0.0.1/a%%2Fb?x=1#frag HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
		basicToken("app@node-a", "s3cret"))
	resp := doRequest(t, l1, req)
	mustStatus(t, resp, 200)
	select {
	case g := <-got:
		if g.connect != "CONNECT 127.0.0.1:80 HTTP/1.1" {
			t.Fatalf("connect line %q", g.connect)
		}
		if !bytes.HasPrefix(g.http, []byte("GET /a%2Fb?x=1 HTTP/1.1\r\n")) {
			t.Fatalf("origin-form %s", strconv.Quote(string(g.http)))
		}
		if bytes.Contains(g.http, []byte("user:pw")) || bytes.Contains(g.http, []byte("#frag")) {
			t.Fatalf("userinfo or fragment forwarded: %s", strconv.Quote(string(g.http)))
		}
	default:
		t.Fatal("fake l2 saw nothing")
	}

	got6 := make(chan string, 1)
	fake6 := serveRaw(t, func(c net.Conn) {
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(3 * time.Second))
		br := bufio.NewReader(c)
		head, err := readHTTPHead(br)
		if err != nil {
			return
		}
		start, _, _ := splitHead(head)
		got6 <- start
		_, _ = io.WriteString(c, "HTTP/1.1 200 Connection Established\r\n\r\n")
		_, _ = readOriginRequest(br)
		_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
	})
	l16 := startL1(t, mapResolver{m: map[string]string{"node-a": fake6}})
	resp = doRequest(t, l16, fmt.Sprintf("GET http://[::1]/z HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
		basicToken("app@node-a", "s3cret")))
	mustStatus(t, resp, 200)
	if line := <-got6; line != "CONNECT [::1]:80 HTTP/1.1" {
		t.Fatalf("ipv6 connect %q", line)
	}
}

func TestHopHeaderStripAndHEAD(t *testing.T) {
	gotCh := make(chan []byte, 1)
	origin := serveRaw(t, func(c net.Conn) {
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(3 * time.Second))
		br := bufio.NewReader(c)
		head, err := readHTTPHead(br)
		if err != nil {
			return
		}
		gotCh <- append([]byte(nil), head...)
		_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\n")
	})
	l1 := startL1(t, mapResolver{m: map[string]string{"node-a": gostA}})
	c, err := net.Dial("tcp", l1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	start := time.Now()
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	req := fmt.Sprintf("HEAD http://%s/h HTTP/1.1\r\nHost: h\r\nConnection: keep-alive, x-hop\r\nx-hop: secret\r\nProxy-Authorization: Basic %s\r\nProxy-Connection: close\r\n\r\n",
		origin, basicToken("app@node-a", "s3cret"))
	if _, err := io.WriteString(c, req); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	resp, err := readHTTPHead(br)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("HEAD waited for a body")
	}
	mustStatus(t, resp, 200)
	want := "HEAD /h HTTP/1.1\r\nHost: h\r\nConnection: keep-alive\r\n\r\n"
	got := <-gotCh
	if string(got) != want {
		t.Fatalf("stripped head\n got %s\nwant %s", strconv.Quote(string(got)), strconv.Quote(want))
	}
}

func TestApproachAGostRewritesHeaders(t *testing.T) {
	gotCh := make(chan []byte, 1)
	origin := serveRaw(t, func(c net.Conn) {
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(3 * time.Second))
		br := bufio.NewReader(c)
		raw, err := readOriginRequest(br)
		if err != nil {
			return
		}
		gotCh <- raw
		_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
	})
	c, err := net.Dial("tcp", gostA)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	req := fmt.Sprintf("GET http://%s/PaTh?q=1 HTTP/1.1\r\nhOsT: KeepMe.Example\r\nX-Zz-Trace: one\r\nx-custom: MiXeD\r\nProxy-Authorization: Basic %s\r\nContent-Length: 0\r\n\r\n",
		origin, basicToken("app@node-a", "s3cret"))
	if _, err := io.WriteString(c, req); err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, c)
	got := <-gotCh
	if len(got) == 0 {
		t.Fatal("origin saw nothing")
	}
	_ = os.WriteFile("/tmp/approach-a-origin.txt", got, 0o644)
	if !bytes.Contains(got, []byte("User-Agent: Go-http-client/1.1")) {
		t.Fatalf("expected injected user-agent: %s", strconv.Quote(string(got)))
	}
	if bytes.Contains(got, []byte("x-custom:")) {
		t.Fatalf("lowercase header survived: %s", strconv.Quote(string(got)))
	}
	if bytes.Contains(got, []byte("hOsT:")) {
		t.Fatalf("host case survived: %s", strconv.Quote(string(got)))
	}
	if bytes.Index(got, []byte("X-Custom:")) < 0 || bytes.Index(got, []byte("X-Custom:")) > bytes.Index(got, []byte("X-Zz-Trace:")) {
		t.Fatalf("expected canonical sorted order: %s", strconv.Quote(string(got)))
	}
	if bytes.Contains(got, []byte("KeepMe.Example")) {
		t.Fatalf("client Host survived: %s", strconv.Quote(string(got)))
	}
	if !bytes.Contains(got, []byte("Host: "+origin)) {
		t.Fatalf("Host was not replaced by the URL authority: %s", strconv.Quote(string(got)))
	}
	if bytes.Contains(got, []byte("Via:")) || bytes.Contains(got, []byte("X-Forwarded-For:")) || bytes.Contains(got, []byte("Proxy-Authorization")) {
		t.Fatalf("unexpected header: %s", strconv.Quote(string(got)))
	}
	if !bytes.HasPrefix(got, []byte("GET /PaTh?q=1 HTTP/1.1\r\n")) {
		t.Fatalf("request line: %s", strconv.Quote(string(got)))
	}
}

func TestClientHelloBytesJA3JA4(t *testing.T) {
	hello := buildClientHello("example.com")
	direct := captureHello(t, func(origin string) {
		c, err := net.Dial("tcp", origin)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		if _, err := c.Write(hello); err != nil {
			t.Fatal(err)
		}
	})
	proxied := captureHello(t, func(origin string) {
		l1 := startL1(t, mapResolver{m: map[string]string{"node-a": gostA}})
		c, err := net.Dial("tcp", l1)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(4 * time.Second))
		fmt.Fprintf(c, "CONNECT %s HTTP/1.1\r\nHost: %s\r\nProxy-Authorization: Basic %s\r\n\r\n",
			origin, origin, basicToken("app@node-a", "s3cret"))
		br := bufio.NewReader(c)
		head, err := readHTTPHead(br)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(head, []byte(" 200 ")) {
			t.Fatalf("connect status %s", strconv.Quote(string(head)))
		}
		if br.Buffered() != 0 {
			t.Fatalf("buffered %d bytes before client hello", br.Buffered())
		}
		off := 0
		for _, n := range []int{1, 2, 3, 5, 8, 13, 21} {
			if off >= len(hello) {
				break
			}
			end := off + n
			if end > len(hello) {
				end = len(hello)
			}
			if _, err := c.Write(hello[off:end]); err != nil {
				t.Fatal(err)
			}
			off = end
			time.Sleep(5 * time.Millisecond)
		}
		if off < len(hello) {
			if _, err := c.Write(hello[off:]); err != nil {
				t.Fatal(err)
			}
		}
	})
	if !bytes.Equal(direct, hello) {
		t.Fatalf("direct capture differs, %d vs %d", len(direct), len(hello))
	}
	if !bytes.Equal(proxied, hello) {
		t.Fatalf("proxied capture differs\n got %x\nwant %x", proxied, hello)
	}
	dh, err := parseClientHello(direct)
	if err != nil {
		t.Fatal(err)
	}
	ph, err := parseClientHello(proxied)
	if err != nil {
		t.Fatal(err)
	}
	if ja3(dh) != ja3(ph) || ja4(dh) != ja4(ph) {
		t.Fatalf("ja3 %s vs %s\nja4 %s vs %s", ja3(dh), ja3(ph), ja4(dh), ja4(ph))
	}
	if ja3(dh) == "" || !strings.Contains(ja4(dh), "_") {
		t.Fatalf("empty fingerprint ja3=%s ja4=%s", ja3(dh), ja4(dh))
	}
	t.Logf("ja3=%s ja4=%s", ja3(dh), ja4(dh))
}

func captureHello(t *testing.T, write func(origin string)) []byte {
	t.Helper()
	got := make(chan []byte, 1)
	origin := serveRaw(t, func(c net.Conn) {
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(4 * time.Second))
		hdr := make([]byte, 5)
		if _, err := io.ReadFull(c, hdr); err != nil {
			got <- nil
			return
		}
		n := int(binary.BigEndian.Uint16(hdr[3:5]))
		body := make([]byte, n)
		if _, err := io.ReadFull(c, body); err != nil {
			got <- nil
			return
		}
		got <- append(hdr, body...)
	})
	write(origin)
	select {
	case b := <-got:
		if b == nil {
			t.Fatal("capture failed")
		}
		return b
	case <-time.After(4 * time.Second):
		t.Fatal("capture timeout")
	}
	return nil
}

func buildClientHello(host string) []byte {
	var hs bytes.Buffer
	hs.Write([]byte{0x03, 0x03})
	hs.Write(bytes.Repeat([]byte{0x11}, 32))
	hs.WriteByte(0)
	ciphers := []byte{0x0a, 0x0a, 0x13, 0x01, 0x00, 0x2f}
	_ = binary.Write(&hs, binary.BigEndian, uint16(len(ciphers)))
	hs.Write(ciphers)
	hs.WriteByte(1)
	hs.WriteByte(0)
	var ext bytes.Buffer
	writeExt := func(typ uint16, payload []byte) {
		_ = binary.Write(&ext, binary.BigEndian, typ)
		_ = binary.Write(&ext, binary.BigEndian, uint16(len(payload)))
		ext.Write(payload)
	}
	writeExt(0x1a1a, nil)
	name := []byte(host)
	var sni bytes.Buffer
	_ = binary.Write(&sni, binary.BigEndian, uint16(len(name)+3))
	sni.WriteByte(0)
	_ = binary.Write(&sni, binary.BigEndian, uint16(len(name)))
	sni.Write(name)
	writeExt(0, sni.Bytes())
	groups := []byte{0x00, 0x1d, 0x00, 0x17}
	var g bytes.Buffer
	_ = binary.Write(&g, binary.BigEndian, uint16(len(groups)))
	g.Write(groups)
	writeExt(10, g.Bytes())
	writeExt(11, []byte{1, 0})
	sigs := []byte{0x04, 0x03, 0x08, 0x04}
	var sg bytes.Buffer
	_ = binary.Write(&sg, binary.BigEndian, uint16(len(sigs)))
	sg.Write(sigs)
	writeExt(13, sg.Bytes())
	var list bytes.Buffer
	for _, v := range [][]byte{[]byte("h2"), []byte("http/1.1")} {
		list.WriteByte(byte(len(v)))
		list.Write(v)
	}
	var alpn bytes.Buffer
	_ = binary.Write(&alpn, binary.BigEndian, uint16(list.Len()))
	alpn.Write(list.Bytes())
	writeExt(16, alpn.Bytes())
	vers := []byte{0x03, 0x04, 0x03, 0x03}
	sv := append([]byte{byte(len(vers))}, vers...)
	writeExt(43, sv)
	_ = binary.Write(&hs, binary.BigEndian, uint16(ext.Len()))
	hs.Write(ext.Bytes())

	body := hs.Bytes()
	var msg bytes.Buffer
	msg.WriteByte(1)
	msg.Write([]byte{byte(len(body) >> 16), byte(len(body) >> 8), byte(len(body))})
	msg.Write(body)
	var rec bytes.Buffer
	rec.WriteByte(22)
	rec.Write([]byte{0x03, 0x01})
	_ = binary.Write(&rec, binary.BigEndian, uint16(msg.Len()))
	rec.Write(msg.Bytes())
	return rec.Bytes()
}

type helloView struct {
	version uint16
	ciphers []uint16
	exts    []tlsExt
}

type tlsExt struct {
	typ  uint16
	data []byte
}

func parseClientHello(rec []byte) (helloView, error) {
	if len(rec) < 5 || rec[0] != 22 {
		return helloView{}, errors.New("not handshake")
	}
	n := int(binary.BigEndian.Uint16(rec[3:5]))
	if len(rec) < 5+n {
		return helloView{}, errors.New("short record")
	}
	hs := rec[5 : 5+n]
	if len(hs) < 4 || hs[0] != 1 {
		return helloView{}, errors.New("not client hello")
	}
	hl := int(hs[1])<<16 | int(hs[2])<<8 | int(hs[3])
	body := hs[4:]
	if len(body) < hl || len(body) < 35 {
		return helloView{}, errors.New("short hello")
	}
	body = body[:hl]
	ver := binary.BigEndian.Uint16(body[:2])
	p := 34
	if p >= len(body) {
		return helloView{}, errors.New("short")
	}
	sidLen := int(body[p])
	p += 1 + sidLen
	if p+2 > len(body) {
		return helloView{}, errors.New("short ciphers")
	}
	csLen := int(binary.BigEndian.Uint16(body[p:]))
	p += 2
	if p+csLen > len(body) {
		return helloView{}, errors.New("cipher overflow")
	}
	var ciphers []uint16
	for i := 0; i+1 < csLen; i += 2 {
		ciphers = append(ciphers, binary.BigEndian.Uint16(body[p+i:]))
	}
	p += csLen
	if p >= len(body) {
		return helloView{}, errors.New("short comp")
	}
	compLen := int(body[p])
	p += 1 + compLen
	var exts []tlsExt
	if p+2 <= len(body) {
		extLen := int(binary.BigEndian.Uint16(body[p:]))
		p += 2
		end := p + extLen
		for p+4 <= end && p+4 <= len(body) {
			typ := binary.BigEndian.Uint16(body[p:])
			ln := int(binary.BigEndian.Uint16(body[p+2:]))
			p += 4
			if p+ln > len(body) {
				return helloView{}, errors.New("ext overflow")
			}
			exts = append(exts, tlsExt{typ: typ, data: append([]byte(nil), body[p:p+ln]...)})
			p += ln
		}
	}
	return helloView{version: ver, ciphers: ciphers, exts: exts}, nil
}

func isGREASE(v uint16) bool { return v&0x0f0f == 0x0a0a }

func ja3(h helloView) string {
	var ciphers, exts, curves, points []string
	for _, c := range h.ciphers {
		if isGREASE(c) {
			continue
		}
		ciphers = append(ciphers, strconv.Itoa(int(c)))
	}
	for _, e := range h.exts {
		if isGREASE(e.typ) {
			continue
		}
		exts = append(exts, strconv.Itoa(int(e.typ)))
		switch e.typ {
		case 10:
			if len(e.data) < 2 {
				continue
			}
			n := int(binary.BigEndian.Uint16(e.data))
			data := e.data[2:]
			if len(data) < n {
				continue
			}
			for i := 0; i+1 < n; i += 2 {
				g := binary.BigEndian.Uint16(data[i:])
				if isGREASE(g) {
					continue
				}
				curves = append(curves, strconv.Itoa(int(g)))
			}
		case 11:
			if len(e.data) < 1 {
				continue
			}
			n := int(e.data[0])
			for i := 0; i < n && 1+i < len(e.data); i++ {
				points = append(points, strconv.Itoa(int(e.data[1+i])))
			}
		}
	}
	raw := fmt.Sprintf("%d,%s,%s,%s,%s", h.version, strings.Join(ciphers, "-"), strings.Join(exts, "-"), strings.Join(curves, "-"), strings.Join(points, "-"))
	sum := md5.Sum([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func ja4(h helloView) string {
	ver := h.version
	hasSNI := false
	var alpn []byte
	var sigs []uint16
	var extTypes []uint16
	for _, e := range h.exts {
		if isGREASE(e.typ) {
			continue
		}
		extTypes = append(extTypes, e.typ)
		switch e.typ {
		case 0:
			hasSNI = true
		case 16:
			alpn = firstALPN(e.data)
		case 13:
			sigs = parseSigs(e.data)
		case 43:
			if v := highestVersion(e.data); v != 0 {
				ver = v
			}
		}
	}
	var ciphers []uint16
	for _, c := range h.ciphers {
		if !isGREASE(c) {
			ciphers = append(ciphers, c)
		}
	}
	sni := "i"
	if hasSNI {
		sni = "d"
	}
	a := fmt.Sprintf("t%s%s%02d%02d%s", verLabel(ver), sni, cap99(len(ciphers)), cap99(len(extTypes)), alpnLabel(alpn))
	b := "000000000000"
	if len(ciphers) > 0 {
		parts := make([]string, len(ciphers))
		for i, c := range ciphers {
			parts[i] = fmt.Sprintf("%04x", c)
		}
		sort.Strings(parts)
		sum := sha256.Sum256([]byte(strings.Join(parts, ",")))
		b = hex.EncodeToString(sum[:])[:12]
	}
	var extParts []string
	for _, typ := range extTypes {
		if typ == 0 || typ == 16 {
			continue
		}
		extParts = append(extParts, fmt.Sprintf("%04x", typ))
	}
	sort.Strings(extParts)
	c := "000000000000"
	if len(extParts) > 0 {
		s := strings.Join(extParts, ",")
		if len(sigs) > 0 {
			sp := make([]string, len(sigs))
			for i, sg := range sigs {
				sp[i] = fmt.Sprintf("%04x", sg)
			}
			s += "_" + strings.Join(sp, ",")
		}
		sum := sha256.Sum256([]byte(s))
		c = hex.EncodeToString(sum[:])[:12]
	}
	return a + "_" + b + "_" + c
}

func cap99(n int) int {
	if n > 99 {
		return 99
	}
	return n
}

func verLabel(v uint16) string {
	switch v {
	case 0x0304:
		return "13"
	case 0x0303:
		return "12"
	case 0x0302:
		return "11"
	case 0x0301:
		return "10"
	case 0x0300:
		return "s3"
	case 0x0002:
		return "s2"
	default:
		return "00"
	}
}

func firstALPN(data []byte) []byte {
	if len(data) < 3 {
		return nil
	}
	n := int(data[2])
	if len(data) < 3+n {
		return nil
	}
	return data[3 : 3+n]
}

func parseSigs(data []byte) []uint16 {
	if len(data) < 2 {
		return nil
	}
	n := int(binary.BigEndian.Uint16(data))
	data = data[2:]
	if len(data) < n {
		return nil
	}
	var out []uint16
	for i := 0; i+1 < n; i += 2 {
		out = append(out, binary.BigEndian.Uint16(data[i:]))
	}
	return out
}

func highestVersion(data []byte) uint16 {
	if len(data) < 1 {
		return 0
	}
	n := int(data[0])
	data = data[1:]
	if len(data) < n {
		return 0
	}
	var best uint16
	for i := 0; i+1 < n; i += 2 {
		v := binary.BigEndian.Uint16(data[i:])
		if isGREASE(v) {
			continue
		}
		if v > best {
			best = v
		}
	}
	return best
}

func alpnLabel(v []byte) string {
	if len(v) == 0 {
		return "00"
	}
	if isALPNAlnum(v[0]) && isALPNAlnum(v[len(v)-1]) {
		return string([]byte{v[0], v[len(v)-1]})
	}
	h := hex.EncodeToString(v)
	return h[:1] + h[len(h)-1:]
}

func isALPNAlnum(b byte) bool {
	return (b >= '0' && b <= '9') || (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z')
}

func TestHTTPL2WriteAndDeadline(t *testing.T) {
	req := fmt.Sprintf("GET http://127.0.0.1/a HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
		basicToken("app@node-a", "s3cret"))
	t.Run("write-error", func(t *testing.T) {
		l1metrics.Reset()
		scriptedDial(t, func(c net.Conn) net.Conn {
			return &scriptConn{Conn: c, write: func([]byte) (int, error) { return 0, io.ErrClosedPipe }}
		})
		l1 := startL1(t, mapResolver{m: map[string]string{"node-a": "192.0.2.1:1"}})
		resp := doRequest(t, l1, req)
		mustStatus(t, resp, 502)
		handlers.Wait()
		snap := l1metrics.Snapshot()
		if len(snap.HTTPFails) != 1 || snap.HTTPFails[0] != [2]string{"node-a", l1metrics.ReasonL2Write} {
			t.Fatalf("fails %#v", snap.HTTPFails)
		}
		if len(snap.HTTPObserves) != 1 || snap.HTTPObserves[0] != l1metrics.ReasonL2Write || snap.HTTPEntered != 0 {
			t.Fatalf("observe %#v entered %d", snap.HTTPObserves, snap.HTTPEntered)
		}
		assertSetup(t, snap, l1metrics.ReasonL2Write)
		assertBalanced(t, snap)
		assertNoConnectMetrics(t, snap)
	})
	t.Run("short-write", func(t *testing.T) {
		l1metrics.Reset()
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ln.Close() })
		go func() {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			defer c.Close()
			br := bufio.NewReader(c)
			for {
				line, err := br.ReadString('\n')
				if err != nil || line == "\r\n" || line == "\n" {
					break
				}
			}
			_, _ = io.WriteString(c, "HTTP/1.1 200 Connection Established\r\n\r\n")
			_, _ = io.Copy(io.Discard, c)
		}()
		orig := dialUpstream
		var connectWrite atomic.Bool
		dialUpstream = func(string) (net.Conn, error) {
			c, err := net.Dial("tcp", ln.Addr().String())
			if err != nil {
				return nil, err
			}
			return &scriptConn{Conn: c, write: func(p []byte) (int, error) {
				if connectWrite.CompareAndSwap(false, true) {
					return c.Write(p)
				}
				if len(p) == 0 {
					return 0, nil
				}
				return 1, nil
			}}, nil
		}
		t.Cleanup(func() {
			handlers.Wait()
			dialUpstream = orig
		})
		l1 := startL1(t, mapResolver{m: map[string]string{"node-a": ln.Addr().String()}})
		resp := doRequest(t, l1, req)
		mustStatus(t, resp, 502)
		handlers.Wait()
		snap := l1metrics.Snapshot()
		if len(snap.HTTPFails) != 1 || snap.HTTPFails[0][1] != l1metrics.ReasonL2Write {
			t.Fatalf("fails %#v", snap.HTTPFails)
		}
		if len(snap.HTTPObserves) != 1 || snap.HTTPObserves[0] != l1metrics.ReasonL2Write || snap.HTTPEntered != 1 {
			t.Fatalf("observe %#v entered %d", snap.HTTPObserves, snap.HTTPEntered)
		}
		assertSetup(t, snap, l1metrics.ResultOK)
		assertBalanced(t, snap)
		assertNoConnectMetrics(t, snap)
	})
	t.Run("deadline", func(t *testing.T) {
		l1metrics.Reset()
		scriptedDial(t, func(c net.Conn) net.Conn {
			return &scriptConn{Conn: c, readDead: func(time.Time) error { return io.ErrClosedPipe }}
		})
		l1 := startL1(t, mapResolver{m: map[string]string{"node-a": "192.0.2.1:1"}})
		resp := doRequest(t, l1, req)
		mustStatus(t, resp, 502)
		handlers.Wait()
		snap := l1metrics.Snapshot()
		if len(snap.HTTPFails) != 1 || snap.HTTPFails[0][1] != l1metrics.ReasonL2Deadline {
			t.Fatalf("fails %#v", snap.HTTPFails)
		}
		if len(snap.HTTPObserves) != 1 || snap.HTTPObserves[0] != l1metrics.ReasonL2Deadline || snap.HTTPEntered != 0 {
			t.Fatalf("observe %#v entered %d", snap.HTTPObserves, snap.HTTPEntered)
		}
		assertSetup(t, snap, l1metrics.ReasonL2Deadline)
		assertBalanced(t, snap)
		assertNoConnectMetrics(t, snap)
	})
}

func TestHTTPClientGone(t *testing.T) {
	l1metrics.Reset()
	origin := serveRaw(t, func(c net.Conn) {
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(3 * time.Second))
		br := bufio.NewReader(c)
		if _, err := readHTTPHead(br); err != nil {
			return
		}
		buf := make([]byte, 5)
		_, _ = io.ReadFull(br, buf)
	})
	l1 := startL1(t, mapResolver{m: map[string]string{"node-a": gostA}})
	c, err := net.Dial("tcp", l1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(4 * time.Second))
	req := fmt.Sprintf("POST http://%s/a HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\nProxy-Authorization: Basic %s\r\n\r\nAB",
		origin, basicToken("app@node-a", "s3cret"))
	if _, err := io.WriteString(c, req); err != nil {
		t.Fatal(err)
	}
	if err := c.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	resp := make([]byte, 256)
	n, _ := c.Read(resp)
	mustStatus(t, resp[:n], 502)
	handlers.Wait()
	snap := l1metrics.Snapshot()
	if len(snap.HTTPAborts) != 1 || snap.HTTPAborts[0][1] != l1metrics.ReasonClientGone {
		t.Fatalf("aborts %#v", snap.HTTPAborts)
	}
	if len(snap.HTTPObserves) != 1 || snap.HTTPObserves[0] != l1metrics.ReasonClientGone {
		t.Fatalf("observe %#v", snap.HTTPObserves)
	}
	if len(snap.HTTPFails) != 0 {
		t.Fatalf("client_gone entered the failure counter %#v", snap.HTTPFails)
	}
	assertSetup(t, snap, l1metrics.ResultOK)
	assertBalanced(t, snap)
	if len(snap.Events) != 1 || snap.Events[0].Result != l1metrics.ReasonClientGone || snap.Events[0].Protocol != l1metrics.ProtocolHTTP {
		t.Fatalf("event %+v", snap.Events)
	}
	assertNoConnectMetrics(t, snap)
}

func TestHTTPResponseAfter100IsL2Response(t *testing.T) {
	l1metrics.Reset()
	origin := serveRaw(t, func(c net.Conn) {
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(3 * time.Second))
		br := bufio.NewReader(c)
		if _, err := readHTTPHead(br); err != nil {
			return
		}
		_, _ = io.WriteString(c, "HTTP/1.1 100 Continue\r\n\r\n")
	})
	l1 := startL1(t, mapResolver{m: map[string]string{"node-a": gostA}})
	c, err := net.Dial("tcp", l1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(4 * time.Second))
	req := fmt.Sprintf("GET http://%s/a HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
		origin, basicToken("app@node-a", "s3cret"))
	if _, err := io.WriteString(c, req); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	cont, err := readHTTPHead(br)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(cont, []byte("100 Continue")) {
		t.Fatalf("100: %s", strconv.Quote(string(cont)))
	}
	rest, err := io.ReadAll(br)
	if len(rest) > 0 && bytes.Contains(rest, []byte("502")) {
		t.Fatalf("appended 502 after 100: %s", strconv.Quote(string(rest)))
	}
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	handlers.Wait()
	snap := l1metrics.Snapshot()
	if len(snap.HTTPFails) != 1 || snap.HTTPFails[0][1] != l1metrics.ReasonL2Response {
		t.Fatalf("fails %#v", snap.HTTPFails)
	}
	if len(snap.HTTPObserves) != 1 || snap.HTTPObserves[0] != l1metrics.ReasonL2Response {
		t.Fatalf("observe %#v", snap.HTTPObserves)
	}
	if snap.HTTPEntered != 1 || snap.HTTPLeft != 1 || snap.HTTPTunnels != 1 {
		t.Fatalf("tunnel entered=%d left=%d duration=%d", snap.HTTPEntered, snap.HTTPLeft, snap.HTTPTunnels)
	}
	assertSetup(t, snap, l1metrics.ResultOK)
	assertNoConnectMetrics(t, snap)
}

func TestClosedHTTPLabels(t *testing.T) {
	l1metrics.Reset()
	l1metrics.RejectHTTP("typo")
	l1metrics.FailHTTP("node-a", "client_gone")
	l1metrics.FailHTTP("node-a", "nope")
	l1metrics.AbortHTTP("node-a", "dial")
	l1metrics.ObserveHTTP("node-a", "no_upstream", time.Now())
	l1metrics.ObserveHTTP("node-a", l1metrics.ResultOK, time.Time{})
	l1metrics.ObserveHTTPSetup("node-a", l1metrics.ReasonStaleTunnel, time.Now())
	l1metrics.ObserveHTTPSetup("node-a", l1metrics.ResultOK, time.Time{})
	l1metrics.AddHTTPBytes("node-a", "sideways", 10)
	l1metrics.AddHTTPBytes("node-a", l1metrics.DirUp, 0)
	l1metrics.TryEmit(l1metrics.MeterEvent{Protocol: "nope", Result: "ok"})
	snap := l1metrics.Snapshot()
	if len(snap.HTTPRejects) != 0 || len(snap.HTTPFails) != 0 || len(snap.HTTPAborts) != 0 || len(snap.HTTPObserves) != 0 || len(snap.HTTPSetup) != 0 || snap.HTTPUp != 0 || len(snap.Events) != 0 || snap.Drops != 1 {
		t.Fatalf("closed set created series: %+v", snap)
	}
}

func TestKeepAliveSameHostGauge(t *testing.T) {
	l1metrics.Reset()
	var accepts atomic.Int32
	origin := serveRaw(t, func(c net.Conn) {
		accepts.Add(1)
		defer c.Close()
		br := bufio.NewReader(c)
		_ = c.SetDeadline(time.Now().Add(4 * time.Second))
		for {
			if _, err := readOriginRequest(br); err != nil {
				return
			}
			_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
		}
	})
	l1 := startL1(t, mapResolver{m: map[string]string{"node-a": gostA}})
	c, err := net.Dial("tcp", l1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(4 * time.Second))
	br := bufio.NewReader(c)
	for i := 1; i <= 4; i++ {
		raw := fmt.Sprintf("GET http://%s/g HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
			origin, basicToken("app@node-a", "s3cret"))
		if _, err := io.WriteString(c, raw); err != nil {
			t.Fatal(err)
		}
		head, err := readHTTPHead(br)
		if err != nil {
			t.Fatal(err)
		}
		body, err := readMessageBody(br, head, false)
		if err != nil {
			t.Fatal(err)
		}
		if string(body) != "ok" {
			t.Fatalf("body %q", body)
		}
		snap := waitCalls(t, func(s l1metrics.Calls) bool {
			return len(s.HTTPObserves) == i && s.HTTPEntered-s.HTTPLeft == 1
		})
		if len(snap.HTTPObserves) != i || snap.HTTPEntered-snap.HTTPLeft != 1 {
			t.Fatalf("after %d: observes %d gauge %d", i, len(snap.HTTPObserves), snap.HTTPEntered-snap.HTTPLeft)
		}
		assertSetup(t, snap, l1metrics.ResultOK)
		if snap.HTTPRetries != 0 || len(snap.HTTPFails) != 0 {
			t.Fatalf("retry/fail %#v retries %d", snap.HTTPFails, snap.HTTPRetries)
		}
	}
	if accepts.Load() != 1 {
		t.Fatalf("origin accepts=%d want 1", accepts.Load())
	}
	_ = c.Close()
	snap := waitCalls(t, func(s l1metrics.Calls) bool {
		return s.HTTPLeft == 1 && s.HTTPTunnels == 1
	})
	assertBalanced(t, snap)
	if len(snap.HTTPObserves) != 4 || snap.HTTPEntered != 1 {
		t.Fatalf("observes %#v entered %d", snap.HTTPObserves, snap.HTTPEntered)
	}
	assertNoConnectMetrics(t, snap)
}

func TestStaleKeepAliveRetry(t *testing.T) {
	l1metrics.Reset()
	var accepts atomic.Int32
	origin := serveRaw(t, func(c net.Conn) {
		accepts.Add(1)
		defer c.Close()
		br := bufio.NewReader(c)
		_ = c.SetDeadline(time.Now().Add(8 * time.Second))
		if _, err := readOriginRequest(br); err != nil {
			return
		}
		if _, err := io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"); err != nil {
			return
		}
		// Node's default keep-alive idle is 5s: read until that deadline, then close.
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, _ = c.Read(make([]byte, 1))
	})
	l1 := startL1(t, mapResolver{m: map[string]string{"node-a": gostA}})
	c, err := net.Dial("tcp", l1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(12 * time.Second))
	br := bufio.NewReader(c)
	send := func() []byte {
		t.Helper()
		raw := fmt.Sprintf("GET http://%s/idle HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
			origin, basicToken("app@node-a", "s3cret"))
		if _, err := io.WriteString(c, raw); err != nil {
			t.Fatal(err)
		}
		head, err := readHTTPHead(br)
		if err != nil {
			t.Fatal(err)
		}
		body, err := readMessageBody(br, head, false)
		if err != nil {
			t.Fatal(err)
		}
		return append(head, body...)
	}
	first := send()
	mustStatus(t, first, 200)
	time.Sleep(5*time.Second + 300*time.Millisecond)
	second := send()
	mustStatus(t, second, 200)
	if accepts.Load() != 2 {
		t.Fatalf("origin accepts=%d want 2", accepts.Load())
	}
	snap := waitCalls(t, func(s l1metrics.Calls) bool {
		return len(s.HTTPObserves) == 2 && s.HTTPRetries == 1 && len(s.HTTPSetup) == 2
	})
	if snap.HTTPRetries != 1 || len(snap.HTTPFails) != 0 {
		t.Fatalf("retries %d fails %#v", snap.HTTPRetries, snap.HTTPFails)
	}
	if len(snap.HTTPObserves) != 2 || snap.HTTPObserves[0] != l1metrics.ResultOK || snap.HTTPObserves[1] != l1metrics.ResultOK {
		t.Fatalf("observes %#v", snap.HTTPObserves)
	}
	assertSetup(t, snap, l1metrics.ResultOK, l1metrics.ResultOK)
	if snap.HTTPEntered != 2 {
		t.Fatalf("entered %d", snap.HTTPEntered)
	}
	assertNoConnectMetrics(t, snap)
	_ = c.Close()
	snap = waitCalls(t, func(s l1metrics.Calls) bool {
		return s.HTTPLeft == 2 && s.HTTPTunnels == 2
	})
	assertBalanced(t, snap)
}

func TestStaleNonIdempotentPOST(t *testing.T) {
	l1metrics.Reset()
	origin := serveRaw(t, func(c net.Conn) {
		defer c.Close()
		br := bufio.NewReader(c)
		_ = c.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := readOriginRequest(br); err != nil {
			return
		}
		_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
	})
	l1 := startL1(t, mapResolver{m: map[string]string{"node-a": gostA}})
	c, err := net.Dial("tcp", l1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(4 * time.Second))
	br := bufio.NewReader(c)
	get := fmt.Sprintf("GET http://%s/a HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
		origin, basicToken("app@node-a", "s3cret"))
	if _, err := io.WriteString(c, get); err != nil {
		t.Fatal(err)
	}
	head, err := readHTTPHead(br)
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, head, 200)
	post := fmt.Sprintf("POST http://%s/a HTTP/1.1\r\nHost: h\r\nContent-Length: 4\r\nProxy-Authorization: Basic %s\r\n\r\nbody",
		origin, basicToken("app@node-a", "s3cret"))
	if _, err := io.WriteString(c, post); err != nil {
		t.Fatal(err)
	}
	resp, err := readHTTPHead(br)
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, 502)
	handlers.Wait()
	snap := l1metrics.Snapshot()
	if snap.HTTPRetries != 0 {
		t.Fatalf("retried a non-idempotent POST: %d", snap.HTTPRetries)
	}
	if len(snap.HTTPFails) != 1 || snap.HTTPFails[0][1] != l1metrics.ReasonStaleTunnel {
		t.Fatalf("fails %#v", snap.HTTPFails)
	}
	if len(snap.HTTPObserves) != 2 || snap.HTTPObserves[0] != l1metrics.ResultOK || snap.HTTPObserves[1] != l1metrics.ReasonStaleTunnel {
		t.Fatalf("observes %#v", snap.HTTPObserves)
	}
	assertSetup(t, snap, l1metrics.ResultOK)
	assertBalanced(t, snap)
	assertNoConnectMetrics(t, snap)
}

func TestBodyAbortAfterStatus(t *testing.T) {
	l1metrics.Reset()
	origin := serveRaw(t, func(c net.Conn) {
		defer c.Close()
		br := bufio.NewReader(c)
		_ = c.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := readOriginRequest(br); err != nil {
			return
		}
		_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: close\r\n\r\nabc")
	})
	l1 := startL1(t, mapResolver{m: map[string]string{"node-a": gostA}})
	c, err := net.Dial("tcp", l1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(4 * time.Second))
	req := fmt.Sprintf("GET http://%s/a HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
		origin, basicToken("app@node-a", "s3cret"))
	if _, err := io.WriteString(c, req); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 256)
	_, _ = c.Read(buf)
	handlers.Wait()
	snap := l1metrics.Snapshot()
	if len(snap.HTTPObserves) != 1 || snap.HTTPObserves[0] != l1metrics.ResultOK {
		t.Fatalf("observes %#v", snap.HTTPObserves)
	}
	if len(snap.HTTPFails) != 0 || snap.HTTPBodyAborts != 1 {
		t.Fatalf("fails %#v bodyAborts %d", snap.HTTPFails, snap.HTTPBodyAborts)
	}
	assertSetup(t, snap, l1metrics.ResultOK)
	assertBalanced(t, snap)
	assertNoConnectMetrics(t, snap)
}

func TestIncompleteHeadRejectAttribution(t *testing.T) {
	cases := []struct {
		name       string
		raw        string
		connectBad bool
		httpBad    bool
	}{
		{"partial-connect", "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n", true, false},
		{"partial-get", "GET http://example.com/a HTTP/1.1\r\nHost: example.com\r\n", false, true},
		{"empty", "", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l1metrics.Reset()
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()
			done := make(chan struct{})
			go func() {
				defer close(done)
				c, err := ln.Accept()
				if err != nil {
					return
				}
				_ = serveProxyConn(c, mapResolver{m: map[string]string{"node-a": gostA}})
			}()
			c, err := net.Dial("tcp", ln.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			_ = c.SetDeadline(time.Now().Add(2 * time.Second))
			if tc.raw != "" {
				if _, err := io.WriteString(c, tc.raw); err != nil {
					t.Fatal(err)
				}
			}
			if err := c.(*net.TCPConn).CloseWrite(); err != nil {
				t.Fatal(err)
			}
			_, _ = io.ReadAll(c)
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("handler did not return")
			}
			snap := l1metrics.Snapshot()
			if tc.connectBad {
				if len(snap.Rejects) != 1 || snap.Rejects[0] != l1metrics.ReasonRejectBadTarget || len(snap.HTTPRejects) != 0 {
					t.Fatalf("connect reject rejects=%v http=%v", snap.Rejects, snap.HTTPRejects)
				}
			} else if len(snap.Rejects) != 0 {
				t.Fatalf("CONNECT reject inflated: %v", snap.Rejects)
			}
			if tc.httpBad {
				if len(snap.HTTPRejects) != 1 || snap.HTTPRejects[0] != l1metrics.ReasonRejectBadTarget {
					t.Fatalf("http rejects %#v", snap.HTTPRejects)
				}
			} else if !tc.connectBad && len(snap.HTTPRejects) != 0 {
				t.Fatalf("http rejects %#v", snap.HTTPRejects)
			}
			if len(snap.HTTPObserves) != 0 || len(snap.HTTPSetup) != 0 || len(snap.HTTPFails) != 0 || snap.HTTPEntered != 0 {
				t.Fatalf("incomplete head observed %+v", snap)
			}
			if !tc.connectBad && (len(snap.Fails) != 0 || len(snap.Observes) != 0 || snap.Entered != 0) {
				t.Fatalf("plain HTTP touched CONNECT series: %+v", snap)
			}
		})
	}
}
