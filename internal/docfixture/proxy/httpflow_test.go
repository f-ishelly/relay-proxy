//go:build docfixture

package proxy

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/f-ishelly/relay-proxy/internal/docfixture/l1metrics"
)

var (
	gostA         string
	gostB         string
	gostBad       string
	gostBin       string
	gostSkip      string
	gostCacheRoot string
	gostGet       = http.Get
)

func TestMain(m *testing.M) {
	go consumeMeters()
	bin, err := prepareGost()
	var cmds []*exec.Cmd
	if err != nil {
		gostSkip = err.Error()
		fmt.Fprintln(os.Stderr, "gost skipped:", err)
	} else {
		gostBin = bin
		start := func(user, pass string) string {
			addr, cmd, err := startGost(user, pass)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				for _, c := range cmds {
					if c != nil && c.Process != nil {
						_ = c.Process.Kill()
					}
				}
				os.Exit(1)
			}
			cmds = append(cmds, cmd)
			return addr
		}
		gostA = start("app@node-a", "s3cret")
		gostB = start("app@node-b", "s3cret")
		gostBad = start("app@node-a", "nope")
	}
	code := m.Run()
	for _, c := range cmds {
		if c.Process != nil {
			_ = c.Process.Kill()
		}
	}
	os.Exit(code)
}

func requireGost(t *testing.T) {
	t.Helper()
	if gostSkip == "" {
		return
	}
	if os.Getenv("GOST_REQUIRED") == "1" {
		t.Fatal(gostSkip)
	}
	t.Skip(gostSkip)
}

// gost v3.3.1 release assets. Archive hashes are the upstream checksums.txt.
// Binary hashes are the extracted `gost` file. A truncated cache file fails the binary hash and is not reused.
var gostAssets = map[string]gostAsset{
	"linux/amd64": {
		archive:    "gost_3.3.1_linux_amd64.tar.gz",
		archiveSHA: "e0e3a2c9116e2f025e962bba340749f80696bda318613d45e5ae96dd0bcae37c",
		binarySHA:  "327720a6dba0e816dbd761f2211140108f579e886bf88075f73da2d5fba5dfc1",
	},
	"linux/arm64": {
		archive:    "gost_3.3.1_linux_arm64.tar.gz",
		archiveSHA: "c58af4a74362a95caa7f580817ad624497d192dfeb60468dc8a4c70c293c6b01",
		binarySHA:  "37da362d2870c3f4e83aab848b6dcf4508f44e4334e6f0675e571e926f3c666f",
	},
	"darwin/amd64": {
		archive:    "gost_3.3.1_darwin_amd64.tar.gz",
		archiveSHA: "857f44606177db09505a8871800142c352010b74093532a1b74554779dd9358a",
		binarySHA:  "0192ffe5f4e789d5ac8dd7806a8732ed121bb543bbdbdf3ad9f8f8589d084742",
	},
	"darwin/arm64": {
		archive:    "gost_3.3.1_darwin_arm64.tar.gz",
		archiveSHA: "6b87b8c5fd728536d1b208382fc6b93a7bb04111d633a9f1e4651cd1ce62886d",
		binarySHA:  "d58192888fb516e2eedf0a1787088db8e10b3ed9c127fdba34ff6258f5621a3a",
	},
}

type gostAsset struct {
	archive    string
	archiveSHA string
	binarySHA  string
}

func prepareGost() (string, error) {
	if env := os.Getenv("GOST_BIN"); env != "" {
		st, err := os.Stat(env)
		if err != nil || st.IsDir() {
			return "", fmt.Errorf("GOST_BIN %q is not a gost v3.3.1 binary (%v)", env, err)
		}
		if err := gostVersionOK(env); err != nil {
			return "", err
		}
		return env, nil
	}
	key := runtime.GOOS + "/" + runtime.GOARCH
	asset, ok := gostAssets[key]
	if !ok {
		return "", fmt.Errorf("gost v3.3.1 has no pinned release asset for %s/%s; set GOST_BIN to a gost v3.3.1 binary", runtime.GOOS, runtime.GOARCH)
	}
	cache, err := gostCachePath()
	if err != nil {
		return "", err
	}
	if reusableGost(cache, asset.binarySHA) {
		if err := gostVersionOK(cache); err != nil {
			return "", err
		}
		return cache, nil
	}
	_ = os.Remove(cache)
	if err := downloadGost(asset, cache); err != nil {
		return "", err
	}
	if !reusableGost(cache, asset.binarySHA) {
		_ = os.Remove(cache)
		return "", fmt.Errorf("gost v3.3.1 cached binary sha256 mismatch for %s/%s", runtime.GOOS, runtime.GOARCH)
	}
	if err := gostVersionOK(cache); err != nil {
		return "", err
	}
	return cache, nil
}

func gostCachePath() (string, error) {
	base := gostCacheRoot
	if base == "" {
		var err error
		base, err = os.UserCacheDir()
		if err != nil {
			return "", fmt.Errorf("gost cache dir: %w", err)
		}
	}
	return filepath.Join(base, "relay-proxy", "gost-v3.3.1", runtime.GOOS+"-"+runtime.GOARCH, "gost"), nil
}

func reusableGost(path, want string) bool {
	got, err := fileSHA256(path)
	return err == nil && got == want
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func gostVersionOK(bin string) error {
	cmd := exec.Command(bin, "-V")
	out, err := cmd.CombinedOutput()
	text := strings.TrimSpace(string(out))
	if !gostVersionToken(text, "v3.3.1") {
		if err != nil {
			return fmt.Errorf("GOST_BIN %q failed -V (%v): %s", bin, err, text)
		}
		return fmt.Errorf("GOST_BIN %q is not gost v3.3.1 (got %q)", bin, text)
	}
	return nil
}

// gostVersionToken matches the version as a whole field. "v3.3.10" is not "v3.3.1".
func gostVersionToken(text, ver string) bool {
	for _, f := range strings.Fields(text) {
		if f == ver {
			return true
		}
	}
	return false
}

func downloadGost(asset gostAsset, dest string) error {
	dir := filepath.Dir(dest)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	resp, err := gostGet("https://github.com/go-gost/gost/releases/download/v3.3.1/" + asset.archive)
	if err != nil {
		return fmt.Errorf("gost v3.3.1 download failed: %v; set GOST_BIN to a gost v3.3.1 binary", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("gost v3.3.1 download returned %s; set GOST_BIN to a gost v3.3.1 binary", resp.Status)
	}
	arc, err := os.CreateTemp(dir, ".gost-archive-*")
	if err != nil {
		return err
	}
	arcName := arc.Name()
	defer os.Remove(arcName)
	ah := sha256.New()
	if _, err := io.Copy(io.MultiWriter(arc, ah), resp.Body); err != nil {
		arc.Close()
		return fmt.Errorf("gost v3.3.1 download: %w", err)
	}
	if err := arc.Sync(); err != nil {
		arc.Close()
		return err
	}
	if err := arc.Close(); err != nil {
		return err
	}
	if got := hex.EncodeToString(ah.Sum(nil)); got != asset.archiveSHA {
		return fmt.Errorf("gost v3.3.1 archive sha256 mismatch for %s: got %s", asset.archive, got)
	}
	af, err := os.Open(arcName)
	if err != nil {
		return err
	}
	defer af.Close()
	gz, err := gzip.NewReader(af)
	if err != nil {
		return fmt.Errorf("gost v3.3.1 archive: %v", err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("gost v3.3.1 archive: %v", err)
		}
		if hdr.Typeflag != tar.TypeReg || filepath.Base(hdr.Name) != "gost" {
			continue
		}
		tmp, err := os.OpenFile(filepath.Join(dir, ".gost-bin-"+strconv.FormatInt(time.Now().UnixNano(), 10)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o755)
		if err != nil {
			return err
		}
		tmpName := tmp.Name()
		bh := sha256.New()
		if _, err := io.Copy(io.MultiWriter(tmp, bh), tr); err != nil {
			tmp.Close()
			os.Remove(tmpName)
			return err
		}
		if got := hex.EncodeToString(bh.Sum(nil)); got != asset.binarySHA {
			tmp.Close()
			os.Remove(tmpName)
			return fmt.Errorf("gost v3.3.1 binary sha256 mismatch: got %s", got)
		}
		if err := tmp.Sync(); err != nil {
			tmp.Close()
			os.Remove(tmpName)
			return err
		}
		if err := tmp.Close(); err != nil {
			os.Remove(tmpName)
			return err
		}
		if err := os.Rename(tmpName, dest); err != nil {
			os.Remove(tmpName)
			return err
		}
		df, err := os.Open(dir)
		if err != nil {
			return err
		}
		err = df.Sync()
		df.Close()
		return err
	}
	return fmt.Errorf("gost v3.3.1 archive had no gost binary")
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
	cmd := exec.Command(gostBin, "-C", cfg)
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

// handlerGate 代替 sync.WaitGroup。Accept 里的 Add 和测试里的 Wait 会同时发生，
// WaitGroup 在计数回到 0 时这样用会让 race detector 报错。
type handlerGate struct {
	mu sync.Mutex
	n  int
	cv *sync.Cond
}

func newHandlerGate() *handlerGate {
	g := &handlerGate{}
	g.cv = sync.NewCond(&g.mu)
	return g
}

func (g *handlerGate) Add(delta int) {
	g.mu.Lock()
	g.n += delta
	if g.n == 0 {
		g.cv.Broadcast()
	}
	g.mu.Unlock()
}

func (g *handlerGate) Done() { g.Add(-1) }

func (g *handlerGate) Wait() {
	g.mu.Lock()
	for g.n > 0 {
		g.cv.Wait()
	}
	g.mu.Unlock()
}

var handlers = newHandlerGate()

func startL1(t *testing.T, r Resolver) string {
	t.Helper()
	return startL1Limited(t, r, defaultLimits())
}

func startL1Limited(t *testing.T, r Resolver, lim proxyLimits) string {
	t.Helper()
	lim = fillLimits(lim)
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
				_ = serveProxyConnLimited(c, r, lim)
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

func assertNoConnectMetrics(t *testing.T, snap metricSnap) {
	t.Helper()
	if len(snap.Rejects) != 0 || len(snap.Fails) != 0 || len(snap.Observes) != 0 || snap.Entered != 0 {
		t.Fatalf("plain HTTP touched CONNECT metrics: rejects=%v fails=%v observes=%v entered=%d",
			snap.Rejects, snap.Fails, snap.Observes, snap.Entered)
	}
}

func assertSetup(t *testing.T, snap metricSnap, want ...string) {
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

func assertBalanced(t *testing.T, snap metricSnap) {
	t.Helper()
	if snap.HTTPLeft != snap.HTTPEntered || snap.HTTPTunnels != snap.HTTPEntered {
		t.Fatalf("tunnel unbalanced entered=%d left=%d duration=%d", snap.HTTPEntered, snap.HTTPLeft, snap.HTTPTunnels)
	}
}

func waitCalls(t *testing.T, pred func(metricSnap) bool) metricSnap {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var snap metricSnap
	for {
		snap = snapMetrics()
		if pred(snap) {
			return snap
		}
		if time.Now().After(deadline) {
			t.Fatalf("snapshot did not match: %+v", snap)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

type scriptConn struct {
	net.Conn
	write    func([]byte) (int, error)
	read     func([]byte) (int, error)
	readDead func(time.Time) error
}

func (s *scriptConn) Write(p []byte) (int, error) {
	if s.write != nil {
		return s.write(p)
	}
	return s.Conn.Write(p)
}

func (s *scriptConn) Read(p []byte) (int, error) {
	if s.read != nil {
		return s.read(p)
	}
	return s.Conn.Read(p)
}

func (s scriptConn) SetReadDeadline(deadline time.Time) error {
	if s.readDead != nil {
		return s.readDead(deadline)
	}
	return s.Conn.SetReadDeadline(deadline)
}

func scriptedDial(t *testing.T, wrap func(net.Conn) net.Conn) func(string) (net.Conn, error) {
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
	return func(string) (net.Conn, error) {
		c, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			return nil, err
		}
		if wrap != nil {
			return wrap(c), nil
		}
		return c, nil
	}
}

func countingDial(n *atomic.Int32) func(string) (net.Conn, error) {
	return func(addr string) (net.Conn, error) {
		n.Add(1)
		return dialUpstream(addr)
	}
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
	requireGost(t)
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
	requireGost(t)
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
	requireGost(t)
	resetMetrics()
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
	_ = c.Close()
	handlers.Wait()
	snap := waitCalls(t, func(c metricSnap) bool {
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
	requireGost(t)
	resetMetrics()
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
	handlers.Wait()
	snap := waitCalls(t, func(c metricSnap) bool {
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
	requireGost(t)
	resetMetrics()
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
	snap := waitCalls(t, func(c metricSnap) bool {
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
	handlers.Wait()
	snap = waitCalls(t, func(c metricSnap) bool {
		return c.HTTPLeft == 1 && c.HTTPTunnels == 1
	})
	if snap.HTTPLeft != 1 || snap.HTTPTunnels != 1 {
		t.Fatalf("101 tunnel left=%d duration=%d", snap.HTTPLeft, snap.HTTPTunnels)
	}
	if snap.HTTPBodyAborts != 0 {
		t.Fatalf("clean 101 teardown counted as abort: %v", snap.BodyByReason)
	}
}

func TestKeepAliveDifferentHostsAndSameHostReuse(t *testing.T) {
	requireGost(t)
	resetMetrics()
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
	snap := waitCalls(t, func(c metricSnap) bool {
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
	snap = snapMetrics()
	if snap.HTTPLeft != 3 || snap.HTTPTunnels != 3 || snap.HTTPEntered != 3 {
		t.Fatalf("tunnels after close entered=%d left=%d duration=%d", snap.HTTPEntered, snap.HTTPLeft, snap.HTTPTunnels)
	}
}

func TestLastAtNodeID(t *testing.T) {
	requireGost(t)
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
	handlers.Wait()
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
		{"https-upper", "app@node-a", "s3cret", "HTTPS://example.com/a", 400, l1metrics.ReasonRejectBadTarget},
	}
	var dials atomic.Int32
	lim := defaultLimits()
	lim.dial = countingDial(&dials)
	l1 := startL1Limited(t, mapResolver{m: map[string]string{"node-a": "127.0.0.1:9"}}, lim)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetMetrics()
			before := dials.Load()
			req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
				tc.target, basicToken(tc.user, tc.pass))
			resp := doRequest(t, l1, req)
			mustStatus(t, resp, tc.status)
			if dials.Load() != before {
				t.Fatalf("dialed %d", dials.Load()-before)
			}
			handlers.Wait()
			snap := snapMetrics()
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
			if wantEvents == 1 && (snap.Events[0].Protocol != l1metrics.ProtocolHTTP || snap.Events[0].Result != tc.reason || snap.Events[0].RequestMs != 0 || snap.Events[0].SetupMs != 0) {
				t.Fatalf("event %+v", snap.Events[0])
			}
			assertNoConnectMetrics(t, snap)
			if tc.status == 407 && !bytes.Contains(resp, []byte("Proxy-Authenticate:")) {
				t.Fatalf("missing challenge: %s", resp)
			}
			head, body := splitRawMessage(t, resp)
			if tc.name == "https" || tc.name == "https-upper" {
				if string(body) != httpsHintBody {
					t.Fatalf("hint body %s", strconv.Quote(string(body)))
				}
				if got := contentLengthOf(t, head); got != len(body) || got != len(httpsHintBody) {
					t.Fatalf("Content-Length %d body %d hint %d", got, len(body), len(httpsHintBody))
				}
				if !bytes.Contains(head, []byte("Content-Type: text/plain; charset=utf-8\r\n")) {
					t.Fatalf("content-type missing: %s", strconv.Quote(string(head)))
				}
				if !bytes.Contains(head, []byte("\r\nConnection: close\r\n")) {
					t.Fatalf("connection: %s", strconv.Quote(string(head)))
				}
			} else {
				if len(body) != 0 {
					t.Fatalf("body %s", strconv.Quote(string(body)))
				}
				if got := contentLengthOf(t, head); got != 0 {
					t.Fatalf("Content-Length %d", got)
				}
				if !bytes.Contains(head, []byte("\r\nConnection: close\r\n")) {
					t.Fatalf("connection: %s", strconv.Quote(string(head)))
				}
				if bytes.Contains(resp, []byte(httpsHintBody)) {
					t.Fatalf("hint leaked into %s", tc.name)
				}
			}
		})
	}
}

func TestHTTPHeadLimit(t *testing.T) {
	const target = "http://example/ok"
	exact := sizedProxyHead(t, target, maxHTTPHead)
	over := sizedProxyHead(t, target, maxHTTPHead+1)
	if len(exact) != maxHTTPHead || len(over) != maxHTTPHead+1 {
		t.Fatalf("sizes exact=%d over=%d limit=%d", len(exact), len(over), maxHTTPHead)
	}

	t.Run("exact", func(t *testing.T) {
		resetMetrics()
		got := make(chan []byte, 1)
		fake := serveRaw(t, func(c net.Conn) {
			defer c.Close()
			_ = c.SetDeadline(time.Now().Add(4 * time.Second))
			br := bufio.NewReaderSize(c, maxHTTPHead)
			if _, err := readHTTPHead(br); err != nil {
				return
			}
			if _, err := io.WriteString(c, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
				return
			}
			raw, err := readOriginRequest(br)
			if err != nil {
				return
			}
			got <- raw
			_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
		})
		l1 := startL1(t, mapResolver{m: map[string]string{"node-a": fake}})
		resp := doRequest(t, l1, exact)
		mustStatus(t, resp, 200)
		select {
		case raw := <-got:
			if !bytes.HasPrefix(raw, []byte("GET /ok HTTP/1.1\r\n")) {
				t.Fatalf("origin request %s", strconv.Quote(string(raw[:min(80, len(raw))])))
			}
			if !bytes.Contains(raw, []byte("\r\nX-Pad: ")) {
				t.Fatal("padding header was not forwarded")
			}
			if bytes.Contains(raw, []byte("Proxy-Authorization")) {
				t.Fatal("proxy authorization was forwarded")
			}
		default:
			t.Fatal("exact-limit head was not forwarded")
		}
		handlers.Wait()
		snap := snapMetrics()
		if len(snap.HTTPRejects) != 0 {
			t.Fatalf("exact head rejected %#v", snap.HTTPRejects)
		}
		if len(snap.HTTPObserves) != 1 || snap.HTTPObserves[0] != l1metrics.ResultOK {
			t.Fatalf("observes %#v", snap.HTTPObserves)
		}
	})

	t.Run("one-over", func(t *testing.T) {
		resetMetrics()
		var dials atomic.Int32
		fake := serveRaw(t, func(c net.Conn) {
			dials.Add(1)
			_ = c.Close()
		})
		l1 := startL1(t, mapResolver{m: map[string]string{"node-a": fake}})
		resp := doRequest(t, l1, over)
		mustStatus(t, resp, 400)
		head, body := splitRawMessage(t, resp)
		if len(body) != 0 {
			t.Fatalf("body %s", strconv.Quote(string(body)))
		}
		if got := contentLengthOf(t, head); got != 0 {
			t.Fatalf("Content-Length %d", got)
		}
		if !bytes.Contains(head, []byte("\r\nConnection: close\r\n")) {
			t.Fatalf("connection: %s", strconv.Quote(string(head)))
		}
		if dials.Load() != 0 {
			t.Fatalf("over-limit head was forwarded, dials=%d", dials.Load())
		}
		handlers.Wait()
		snap := snapMetrics()
		if len(snap.HTTPRejects) != 1 || snap.HTTPRejects[0] != l1metrics.ReasonRejectBadTarget {
			t.Fatalf("rejects %#v", snap.HTTPRejects)
		}
		if len(snap.Rejects) != 0 || len(snap.HTTPObserves) != 0 || len(snap.HTTPFails) != 0 {
			t.Fatalf("over-limit counted elsewhere: %+v", snap)
		}
	})
}

func TestRejectDrainsUnreadThenEOF(t *testing.T) {
	l1 := startL1(t, mapResolver{m: map[string]string{"node-a": "127.0.0.1:9"}})
	token := basicToken("app@node-a", "s3cret")

	t.Run("https-content-length", func(t *testing.T) {
		resetMetrics()
		const payload = "hello"
		req := fmt.Sprintf("POST https://example.com/a HTTP/1.1\r\nHost: h\r\nContent-Length: %d\r\nProxy-Authorization: Basic %s\r\n\r\n%s",
			len(payload), token, payload)
		head, body := writeAndReadEOF(t, l1, []byte(req))
		mustStatus(t, head, 400)
		if string(body) != httpsHintBody {
			t.Fatalf("hint %s", strconv.Quote(string(body)))
		}
		if got := contentLengthOf(t, head); got != len(body) {
			t.Fatalf("Content-Length %d body %d", got, len(body))
		}
		handlers.Wait()
		snap := snapMetrics()
		if len(snap.HTTPRejects) != 1 || snap.HTTPRejects[0] != l1metrics.ReasonRejectBadTarget {
			t.Fatalf("rejects %#v", snap.HTTPRejects)
		}
	})

	t.Run("https-past-socket-buffer", func(t *testing.T) {
		resetMetrics()
		c, err := net.Dial("tcp", l1)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		payload := bytes.Repeat([]byte("B"), unreadPastReceiveBuffer(t, c))
		head := fmt.Sprintf("POST https://example.com/a HTTP/1.1\r\nHost: h\r\nContent-Length: %d\r\nProxy-Authorization: Basic %s\r\n\r\n",
			len(payload), token)
		respHead, respBody := writeAndReadEOF(t, "", append([]byte(head), payload...), c)
		mustStatus(t, respHead, 400)
		if string(respBody) != httpsHintBody {
			t.Fatalf("hint %s", strconv.Quote(string(respBody)))
		}
		if got := contentLengthOf(t, respHead); got != len(httpsHintBody) || got != len(respBody) {
			t.Fatalf("Content-Length %d body %d", got, len(respBody))
		}
		handlers.Wait()
		snap := snapMetrics()
		if len(snap.HTTPRejects) != 1 || snap.HTTPRejects[0] != l1metrics.ReasonRejectBadTarget {
			t.Fatalf("rejects %#v", snap.HTTPRejects)
		}
	})

	t.Run("oversize-head-trailing", func(t *testing.T) {
		resetMetrics()
		c, err := net.Dial("tcp", l1)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		trailing := bytes.Repeat([]byte("T"), unreadPastReceiveBuffer(t, c))
		raw := append([]byte(sizedProxyHead(t, "http://example/ok", maxHTTPHead+1)), trailing...)
		head, body := writeAndReadEOF(t, "", raw, c)
		mustStatus(t, head, 400)
		if len(body) != 0 || contentLengthOf(t, head) != 0 {
			t.Fatalf("want empty 400, body %d cl %d", len(body), contentLengthOf(t, head))
		}
		if !bytes.Contains(head, []byte("\r\nConnection: close\r\n")) {
			t.Fatalf("connection: %s", strconv.Quote(string(head)))
		}
		handlers.Wait()
		snap := snapMetrics()
		if len(snap.HTTPRejects) != 1 || snap.HTTPRejects[0] != l1metrics.ReasonRejectBadTarget {
			t.Fatalf("rejects %#v", snap.HTTPRejects)
		}
		if len(snap.Rejects) != 0 {
			t.Fatalf("counted as connect %#v", snap.Rejects)
		}
	})
}

func TestSingleLineHeadLimit(t *testing.T) {
	lim := defaultLimits()
	lim.headerDeadline = 300 * time.Millisecond
	l1 := startL1Limited(t, mapResolver{m: map[string]string{"node-a": "127.0.0.1:9"}}, lim)

	// 正好 65536 且末尾是 \n：bufio 能切出这一行，不会返回 ErrBufferFull。
	// 这一行已经用满上限，结尾空行放不下，必须马上 bad_target，不能空等 header_deadline。
	t.Run("exact-with-newline", func(t *testing.T) {
		resetMetrics()
		line := bytes.Repeat([]byte("A"), maxHTTPHead)
		line[len(line)-1] = '\n'
		copy(line, []byte("GET "))
		if len(line) != maxHTTPHead || line[len(line)-1] != '\n' {
			t.Fatalf("line len %d", len(line))
		}
		head, body := writeAndReadEOF(t, l1, line)
		mustStatus(t, head, 400)
		if len(body) != 0 || contentLengthOf(t, head) != 0 {
			t.Fatalf("body %d", len(body))
		}
		if !bytes.Contains(head, []byte("\r\nConnection: close\r\n")) {
			t.Fatalf("connection: %s", strconv.Quote(string(head)))
		}
		handlers.Wait()
		snap := snapMetrics()
		if len(snap.HTTPRejects) != 1 || snap.HTTPRejects[0] != l1metrics.ReasonRejectBadTarget {
			t.Fatalf("rejects %#v", snap.HTTPRejects)
		}
		for _, r := range snap.HTTPRejects {
			if r == l1metrics.ReasonRejectHeaderTimeout {
				t.Fatal("exact line counted as header_timeout")
			}
		}
	})

	// 65536 字节且没有换行：ReadSlice 返回 ErrBufferFull，同样是 bad_target。
	t.Run("buffer-full", func(t *testing.T) {
		resetMetrics()
		line := bytes.Repeat([]byte("B"), maxHTTPHead)
		copy(line, []byte("GET "))
		if bytes.Contains(line, []byte("\n")) {
			t.Fatal("line contains newline")
		}
		head, body := writeAndReadEOF(t, l1, line)
		mustStatus(t, head, 400)
		if len(body) != 0 || contentLengthOf(t, head) != 0 {
			t.Fatalf("body %d", len(body))
		}
		handlers.Wait()
		snap := snapMetrics()
		if len(snap.HTTPRejects) != 1 || snap.HTTPRejects[0] != l1metrics.ReasonRejectBadTarget {
			t.Fatalf("rejects %#v", snap.HTTPRejects)
		}
	})
}

// writeAndReadEOF writes raw and reads one response. The next read must be a
// clean EOF, which is the server half-close, not ECONNRESET.
// c may be nil; otherwise it is the already-dialed client (addr is ignored).
func writeAndReadEOF(t *testing.T, addr string, raw []byte, c ...net.Conn) (head, body []byte) {
	t.Helper()
	var conn net.Conn
	var err error
	if len(c) > 0 && c[0] != nil {
		conn = c[0]
	} else {
		conn, err = net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
	}
	_ = conn.SetDeadline(time.Now().Add(4 * time.Second))
	if _, err := conn.Write(raw); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)
	head, err = readHTTPHead(br)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	body, err = readMessageBody(br, head, false)
	if err != nil {
		t.Fatal(err)
	}
	var extra [8]byte
	n, err := br.Read(extra[:])
	if n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("after response want EOF, n=%d err=%v", n, err)
	}
	if errors.Is(err, syscall.ECONNRESET) {
		t.Fatal("ECONNRESET")
	}
	return head, body
}

// unreadPastReceiveBuffer is larger than this socket's advertised receive
// buffer and still within the server's 256KiB drain cap.
func unreadPastReceiveBuffer(t *testing.T, c net.Conn) int {
	t.Helper()
	tcp, ok := c.(*net.TCPConn)
	if !ok {
		t.Fatalf("conn %T", c)
	}
	sc, err := tcp.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var rcv int
	ctrlErr := sc.Control(func(fd uintptr) {
		rcv, err = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF)
	})
	if ctrlErr != nil || err != nil {
		t.Fatal(ctrlErr, err)
	}
	n := rcv + 64<<10
	if n > httpErrorDrainMax {
		n = httpErrorDrainMax
	}
	if n <= rcv {
		t.Fatalf("drain cap %d does not exceed SO_RCVBUF %d", httpErrorDrainMax, rcv)
	}
	return n
}

// sizedProxyHead builds a plain absolute-form request whose head, including the
// terminating CRLFCRLF, is exactly n bytes. The padding lives in one X-Pad line.
func sizedProxyHead(t *testing.T, target string, n int) string {
	t.Helper()
	prefix := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\nX-Pad: ",
		target, basicToken("app@node-a", "s3cret"))
	const suffix = "\r\n\r\n"
	pad := n - len(prefix) - len(suffix)
	if pad < 1 {
		t.Fatalf("fixed head %d already exceeds %d", len(prefix)+len(suffix), n)
	}
	return prefix + strings.Repeat("a", pad) + suffix
}

func splitRawMessage(t *testing.T, raw []byte) (head, body []byte) {
	t.Helper()
	i := bytes.Index(raw, []byte("\r\n\r\n"))
	if i < 0 {
		t.Fatalf("no header terminator: %s", strconv.Quote(string(raw)))
	}
	return raw[:i+4], raw[i+4:]
}

func contentLengthOf(t *testing.T, head []byte) int {
	t.Helper()
	_, headers, err := splitHead(head)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	n := 0
	for _, ln := range headers {
		name, _, ok := headerName(ln)
		if !ok || !strings.EqualFold(string(name), "Content-Length") {
			continue
		}
		v, err := strconv.Atoi(headerValue(ln))
		if err != nil {
			t.Fatal(err)
		}
		if found && v != n {
			t.Fatalf("conflicting content-length in %s", strconv.Quote(string(head)))
		}
		found = true
		n = v
	}
	if !found {
		t.Fatalf("no content-length in %s", strconv.Quote(string(head)))
	}
	return n
}

func TestBadCredentialDoesNotDial(t *testing.T) {
	resetMetrics()
	var dials atomic.Int32
	lim := defaultLimits()
	lim.dial = countingDial(&dials)
	l1 := startL1Limited(t, mapResolver{m: map[string]string{"node-a": "127.0.0.1:9"}}, lim)
	token := base64.StdEncoding.EncodeToString([]byte("app@node-a:bad\nsecret"))
	req := fmt.Sprintf("GET http://127.0.0.1/a HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n", token)
	resp := doRequest(t, l1, req)
	mustStatus(t, resp, 400)
	if dials.Load() != 0 {
		t.Fatalf("dialed %d", dials.Load())
	}
	handlers.Wait()
	snap := snapMetrics()
	if len(snap.HTTPRejects) != 1 || snap.HTTPRejects[0] != l1metrics.ReasonRejectBadCredential {
		t.Fatalf("rejects %#v", snap.HTTPRejects)
	}
	if len(snap.Events) != 0 || len(snap.HTTPObserves) != 0 {
		t.Fatalf("bad credential emitted %+v observes %#v", snap.Events, snap.HTTPObserves)
	}
	assertNoConnectMetrics(t, snap)
}

func TestL2Auth407(t *testing.T) {
	requireGost(t)
	resetMetrics()
	l1 := startL1(t, mapResolver{m: map[string]string{"node-a": gostBad}})
	req := fmt.Sprintf("GET http://127.0.0.1/a HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
		basicToken("app@node-a", "s3cret"))
	resp := doRequest(t, l1, req)
	mustStatus(t, resp, 407)
	if !bytes.Contains(resp, []byte("Proxy-Authenticate:")) {
		t.Fatalf("challenge not passed: %s", strconv.Quote(string(resp)))
	}
	handlers.Wait()
	snap := snapMetrics()
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
	resetMetrics()
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
	snap := snapMetrics()
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
		resetMetrics()
		l1 := startL1(t, mapResolver{m: map[string]string{"node-a": closed}})
		resp := doRequest(t, l1, fmt.Sprintf("GET http://127.0.0.1/a HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
			basicToken("app@node-a", "s3cret")))
		mustStatus(t, resp, 502)
		handlers.Wait()
		snap := snapMetrics()
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
		resetMetrics()
		l1 := startL1(t, staticResolver{err: ErrUnknownNode})
		resp := doRequest(t, l1, fmt.Sprintf("GET http://127.0.0.1/a HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
			basicToken("app@node-a", "s3cret")))
		mustStatus(t, resp, 502)
		handlers.Wait()
		snap := snapMetrics()
		if len(snap.HTTPFails) != 1 || snap.HTTPFails[0] != [2]string{l1metrics.NodeUnknown, l1metrics.ReasonUnknownNode} {
			t.Fatalf("fails %#v", snap.HTTPFails)
		}
		if len(snap.HTTPObserves) != 0 {
			t.Fatalf("unknown_node observed %#v", snap.HTTPObserves)
		}
		assertSetup(t, snap)
		assertBalanced(t, snap)
		if len(snap.Events) != 1 || snap.Events[0].Result != l1metrics.ReasonUnknownNode || snap.Events[0].Protocol != l1metrics.ProtocolHTTP {
			t.Fatalf("event %+v", snap.Events)
		}
		assertNoConnectMetrics(t, snap)
	})
	t.Run("no-healthy", func(t *testing.T) {
		resetMetrics()
		l1 := startL1(t, staticResolver{err: ErrNoHealthyAddr})
		resp := doRequest(t, l1, fmt.Sprintf("GET http://127.0.0.1/a HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
			basicToken("app@node-a", "s3cret")))
		mustStatus(t, resp, 502)
		handlers.Wait()
		snap := snapMetrics()
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
		resetMetrics()
		l1 := startL1(t, staticResolver{})
		resp := doRequest(t, l1, fmt.Sprintf("GET http://127.0.0.1/a HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
			basicToken("app@node-a", "s3cret")))
		mustStatus(t, resp, 502)
		handlers.Wait()
		snap := snapMetrics()
		if len(snap.HTTPFails) != 1 || snap.HTTPFails[0] != [2]string{l1metrics.NodeUnknown, l1metrics.ReasonEmptyAddr} {
			t.Fatalf("fails %#v", snap.HTTPFails)
		}
		if len(snap.HTTPObserves) != 0 {
			t.Fatalf("empty_addr observed %#v", snap.HTTPObserves)
		}
		assertSetup(t, snap)
		assertBalanced(t, snap)
		if len(snap.Events) != 1 || snap.Events[0].Result != l1metrics.ReasonEmptyAddr {
			t.Fatalf("event %+v", snap.Events)
		}
		assertNoConnectMetrics(t, snap)
	})
	t.Run("resolver", func(t *testing.T) {
		resetMetrics()
		l1 := startL1(t, staticResolver{err: errors.New("etcd unavailable")})
		resp := doRequest(t, l1, fmt.Sprintf("GET http://127.0.0.1/a HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
			basicToken("app@node-a", "s3cret")))
		mustStatus(t, resp, 502)
		handlers.Wait()
		snap := snapMetrics()
		if len(snap.HTTPFails) != 1 || snap.HTTPFails[0] != [2]string{l1metrics.NodeUnknown, l1metrics.ReasonResolverError} {
			t.Fatalf("fails %#v", snap.HTTPFails)
		}
		if len(snap.HTTPObserves) != 0 {
			t.Fatalf("resolver_error observed %#v", snap.HTTPObserves)
		}
		if len(snap.Events) != 1 || snap.Events[0].Result != l1metrics.ReasonResolverError || snap.Events[0].NodeID != "node-a" {
			t.Fatalf("event %+v", snap.Events)
		}
		assertNoConnectMetrics(t, snap)
	})
}

func TestL2ResponseTimeout(t *testing.T) {
	hold := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-hold:
		default:
			close(hold)
		}
	})
	blackhole := serveRaw(t, func(c net.Conn) {
		defer c.Close()
		select {
		case <-hold:
		case <-time.After(5 * time.Second):
		}
	})
	resetMetrics()
	lim := defaultLimits()
	lim.l2Response = 300 * time.Millisecond
	l1 := startL1Limited(t, mapResolver{m: map[string]string{"node-a": blackhole}}, lim)
	start := time.Now()
	resp := doRequest(t, l1, fmt.Sprintf("GET http://127.0.0.1/a HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
		basicToken("app@node-a", "s3cret")))
	if time.Since(start) > 2*time.Second {
		t.Fatalf("timeout too slow: %s", time.Since(start))
	}
	mustStatus(t, resp, 502)
	handlers.Wait()
	snap := snapMetrics()
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
	resetMetrics()
	lim := defaultLimits()
	lim.tunnelIdle = 200 * time.Millisecond
	lim.headerDeadline = 2 * time.Second
	l1 := startL1Limited(t, staticResolver{}, lim)
	c, err := net.Dial("tcp", l1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	start := time.Now()
	buf := make([]byte, 1)
	_, err = c.Read(buf)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected idle close")
	}
	if elapsed > time.Second {
		t.Fatalf("zero-byte wait used header deadline: %s", elapsed)
	}
	handlers.Wait()
	snap := snapMetrics()
	if len(snap.HTTPRejects) != 0 || len(snap.Rejects) != 0 || len(snap.HTTPObserves) != 0 || len(snap.HTTPSetup) != 0 || len(snap.HTTPFails) != 0 || len(snap.Events) != 0 || snap.Drops != 0 || snap.HTTPBodyAborts != 0 || snap.HTTPEntered != 0 {
		t.Fatalf("zero-byte idle timeout was counted: %+v", snap)
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

func TestConnectionListedHostSurvives(t *testing.T) {
	cases := []struct {
		name    string
		headers string
		want    string
	}{
		{
			name:    "host-token",
			headers: "hOsT: KeepMe.Example\r\nConnection: host\r\n",
			want:    "GET /h HTTP/1.1\r\nhOsT: KeepMe.Example\r\nConnection: host\r\n\r\n",
		},
		{
			name:    "mixed-case-with-other-hop",
			headers: "hOsT: KeepMe.Example\r\nConnection: Host, x-hop\r\nx-hop: secret\r\nX-Keep: yes\r\n",
			want:    "GET /h HTTP/1.1\r\nhOsT: KeepMe.Example\r\nConnection: Host\r\nX-Keep: yes\r\n\r\n",
		},
		{
			name:    "upper-token",
			headers: "Host: KeepMe.Example\r\nConnection: HOST, X-Hop\r\nX-Hop: secret\r\n",
			want:    "GET /h HTTP/1.1\r\nHost: KeepMe.Example\r\nConnection: HOST\r\n\r\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotCh := make(chan []byte, 1)
			fake := serveRaw(t, func(c net.Conn) {
				defer c.Close()
				_ = c.SetDeadline(time.Now().Add(3 * time.Second))
				br := bufio.NewReader(c)
				if _, err := readHTTPHead(br); err != nil {
					return
				}
				if _, err := io.WriteString(c, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
					return
				}
				raw, err := readOriginRequest(br)
				if err != nil {
					return
				}
				gotCh <- raw
				_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
			})
			l1 := startL1(t, mapResolver{m: map[string]string{"node-a": fake}})
			req := fmt.Sprintf("GET http://%s/h HTTP/1.1\r\n%sProxy-Authorization: Basic %s\r\n\r\n",
				fake, tc.headers, basicToken("app@node-a", "s3cret"))
			resp := doRequest(t, l1, req)
			mustStatus(t, resp, 200)
			select {
			case got := <-gotCh:
				if string(got) != tc.want {
					t.Fatalf("origin head\n got %s\nwant %s", strconv.Quote(string(got)), strconv.Quote(tc.want))
				}
				if bytes.Contains(bytes.ToLower(got), []byte("\r\nx-hop:")) {
					t.Fatalf("connection-listed hop survived: %s", strconv.Quote(string(got)))
				}
			default:
				t.Fatal("fake l2 saw no origin request")
			}
		})
	}
}

func TestUnauthenticatedHTTPSHasNoConnectHint(t *testing.T) {
	resetMetrics()
	var dials atomic.Int32
	lim := defaultLimits()
	lim.dial = countingDial(&dials)
	l1 := startL1Limited(t, mapResolver{m: map[string]string{"node-a": "127.0.0.1:9"}}, lim)
	req := "GET https://example.com/a HTTP/1.1\r\nHost: example.com\r\n\r\n"
	resp := doRequest(t, l1, req)
	mustStatus(t, resp, http.StatusProxyAuthRequired)
	if bytes.Contains(resp, []byte(httpsHintBody)) {
		t.Fatalf("hint leaked: %s", strconv.Quote(string(resp)))
	}
	head, body := splitRawMessage(t, resp)
	if len(body) != 0 {
		t.Fatalf("body %s", strconv.Quote(string(body)))
	}
	if got := contentLengthOf(t, head); got != 0 {
		t.Fatalf("Content-Length %d", got)
	}
	if !bytes.Contains(head, []byte("Proxy-Authenticate:")) {
		t.Fatalf("missing challenge: %s", strconv.Quote(string(head)))
	}
	if dials.Load() != 0 {
		t.Fatalf("dialed %d", dials.Load())
	}
	handlers.Wait()
	snap := snapMetrics()
	if len(snap.HTTPRejects) != 1 || snap.HTTPRejects[0] != l1metrics.ReasonRejectAuth {
		t.Fatalf("rejects %#v", snap.HTTPRejects)
	}
	assertNoConnectMetrics(t, snap)
}

func TestHopHeaderStripAndHEAD(t *testing.T) {
	requireGost(t)
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
	requireGost(t)
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
	requireGost(t)
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
	handlers.Wait()
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
		resetMetrics()
		lim := defaultLimits()
		lim.dial = scriptedDial(t, func(c net.Conn) net.Conn {
			return &scriptConn{Conn: c, write: func([]byte) (int, error) { return 0, io.ErrClosedPipe }}
		})
		l1 := startL1Limited(t, mapResolver{m: map[string]string{"node-a": "192.0.2.1:1"}}, lim)
		resp := doRequest(t, l1, req)
		mustStatus(t, resp, 502)
		handlers.Wait()
		snap := snapMetrics()
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
		resetMetrics()
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
		var connectWrite atomic.Bool
		lim := defaultLimits()
		lim.dial = func(string) (net.Conn, error) {
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
		l1 := startL1Limited(t, mapResolver{m: map[string]string{"node-a": ln.Addr().String()}}, lim)
		resp := doRequest(t, l1, req)
		mustStatus(t, resp, 502)
		handlers.Wait()
		snap := snapMetrics()
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
		resetMetrics()
		lim := defaultLimits()
		lim.dial = scriptedDial(t, func(c net.Conn) net.Conn {
			return &scriptConn{Conn: c, readDead: func(time.Time) error { return io.ErrClosedPipe }}
		})
		l1 := startL1Limited(t, mapResolver{m: map[string]string{"node-a": "192.0.2.1:1"}}, lim)
		resp := doRequest(t, l1, req)
		mustStatus(t, resp, 502)
		handlers.Wait()
		snap := snapMetrics()
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
	requireGost(t)
	resetMetrics()
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
	snap := snapMetrics()
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
	requireGost(t)
	resetMetrics()
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
	snap := snapMetrics()
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
	resetMetrics()
	l1metrics.RejectHTTP("typo")
	l1metrics.Fail502("node-z", "typo")
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
	snap := snapMetrics()
	if len(snap.Fails) != 1 || snap.Fails[0] != [2]string{"node-z", l1metrics.ReasonOther} {
		t.Fatalf("Fail502 fallback %#v", snap.Fails)
	}
	if len(snap.HTTPFails) != 2 || snap.HTTPFails[0] != [2]string{"node-a", l1metrics.ReasonOther} || snap.HTTPFails[1] != [2]string{"node-a", l1metrics.ReasonOther} {
		t.Fatalf("FailHTTP fallback %#v", snap.HTTPFails)
	}
	if len(snap.HTTPRejects) != 0 || len(snap.HTTPAborts) != 0 || len(snap.HTTPObserves) != 0 || len(snap.HTTPSetup) != 0 || snap.HTTPUp != 0 || len(snap.Events) != 0 || snap.Drops != 1 {
		t.Fatalf("closed set created series: %+v", snap)
	}
}

func TestKeepAliveSameHostGauge(t *testing.T) {
	requireGost(t)
	resetMetrics()
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
		snap := waitCalls(t, func(s metricSnap) bool {
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
	handlers.Wait()
	snap := waitCalls(t, func(s metricSnap) bool {
		return s.HTTPLeft == 1 && s.HTTPTunnels == 1
	})
	assertBalanced(t, snap)
	if len(snap.HTTPObserves) != 4 || snap.HTTPEntered != 1 {
		t.Fatalf("observes %#v entered %d", snap.HTTPObserves, snap.HTTPEntered)
	}
	assertNoConnectMetrics(t, snap)
}

func TestStaleKeepAliveRetry(t *testing.T) {
	requireGost(t)
	resetMetrics()
	var accepts atomic.Int32
	idleClosed := make(chan struct{}, 1)
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
		select {
		case idleClosed <- struct{}{}:
		default:
		}
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
	select {
	case <-idleClosed:
	case <-time.After(8 * time.Second):
		t.Fatal("origin idle close did not happen")
	}
	second := send()
	mustStatus(t, second, 200)
	if accepts.Load() != 2 {
		t.Fatalf("origin accepts=%d want 2", accepts.Load())
	}
	snap := waitCalls(t, func(s metricSnap) bool {
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
	handlers.Wait()
	snap = waitCalls(t, func(s metricSnap) bool {
		return s.HTTPLeft == 2 && s.HTTPTunnels == 2
	})
	assertBalanced(t, snap)
}

func TestStaleNonIdempotentPOST(t *testing.T) {
	requireGost(t)
	resetMetrics()
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
	snap := snapMetrics()
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
	requireGost(t)
	resetMetrics()
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
	snap := snapMetrics()
	if len(snap.HTTPObserves) != 1 || snap.HTTPObserves[0] != l1metrics.ResultOK {
		t.Fatalf("observes %#v", snap.HTTPObserves)
	}
	if len(snap.HTTPFails) != 0 || snap.HTTPBodyAborts != 1 || snap.BodyByReason["origin_reset"] != 1 {
		t.Fatalf("fails %#v bodyAborts %d by %v", snap.HTTPFails, snap.HTTPBodyAborts, snap.BodyByReason)
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
			resetMetrics()
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
				_ = serveProxyConn(c, staticResolver{})
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
			snap := snapMetrics()
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

// TestMeterIdleWaitsForLog 发出一批事件后立刻取基线之后的日志长度。
// 通道被取空和写入 evLog 之间不能被看成空闲，否则下一则测试会看见漏过来的事件。
func TestMeterIdleWaitsForLog(t *testing.T) {
	resetMetrics()
	const n = 400
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l1metrics.TryEmit(l1metrics.MeterEvent{Protocol: l1metrics.ProtocolHTTP, Result: l1metrics.ResultOK})
		}()
	}
	wg.Wait()
	waitMeterIdle()
	evMu.Lock()
	got := len(evLog) - evBase
	evMu.Unlock()
	if got != n {
		t.Fatalf("logged %d want %d", got, n)
	}
}
