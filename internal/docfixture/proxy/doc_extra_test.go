//go:build docfixture

package proxy

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"

	"github.com/f-ishelly/relay-proxy/internal/docfixture/l1metrics"
)

func TestDocFencesMatchSources(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller")
	}
	root := filepath.Join(filepath.Dir(file), "..", "..", "..")
	doc, err := os.ReadFile(filepath.Join(root, "docs", "openspec-proxy-design.md"))
	if err != nil {
		t.Fatal(err)
	}
	fences := goFences(doc)
	if len(fences) != 3 {
		t.Fatalf("go fences %d", len(fences))
	}
	if !bytes.HasPrefix(fences[0], []byte("package l1metrics\n")) ||
		!bytes.HasPrefix(fences[1], []byte("// ErrUnknownNode")) ||
		!bytes.HasPrefix(fences[2], []byte("// maxHTTPHead")) {
		t.Fatal("fence order is not metrics, CONNECT, HTTP")
	}
	metrics, err := os.ReadFile(filepath.Join(root, "internal", "docfixture", "l1metrics", "metrics.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(metrics, fences[0]) {
		t.Fatalf("l1metrics/metrics.go is not the §7 fence byte-for-byte (%d vs %d)", len(metrics), len(fences[0]))
	}
	matchTail(t, filepath.Join(root, "internal", "docfixture", "proxy", "sec10_gen.go"), []byte("// ErrUnknownNode"), fences[1])
	matchTail(t, filepath.Join(root, "internal", "docfixture", "proxy", "httpflow.go"), []byte("// maxHTTPHead"), fences[2])
}

func matchTail(t *testing.T, path string, marker, fence []byte) {
	t.Helper()
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	i := bytes.Index(src, marker)
	if i < 0 {
		t.Fatalf("%s missing %s", path, marker)
	}
	if !bytes.Equal(src[i:], fence) {
		got, want := src[i:], fence
		n := len(got)
		if len(want) < n {
			n = len(want)
		}
		for j := 0; j < n; j++ {
			if got[j] != want[j] {
				t.Fatalf("%s diverges from the doc fence at byte %d", path, j)
			}
		}
		t.Fatalf("%s fence length %d file tail %d", path, len(want), len(got))
	}
}

func goFences(doc []byte) [][]byte {
	var out [][]byte
	rest := doc
	for {
		i := bytes.Index(rest, []byte("\n```go\n"))
		if i < 0 {
			break
		}
		rest = rest[i+len("\n```go\n"):]
		j := bytes.Index(rest, []byte("\n```"))
		if j < 0 {
			break
		}
		out = append(out, append([]byte(nil), rest[:j+1]...))
		rest = rest[j+1:]
	}
	return out
}

func TestHistogramInfAndClosedSetup(t *testing.T) {
	resetMetrics()
	node := fmt.Sprintf("card-%d", time.Now().UnixNano())
	l1metrics.ObserveHTTP(node, l1metrics.ResultOK, time.Now())
	before, _ := expositionSeries("proxy_http_tunnel_setup_ms", node)
	l1metrics.ObserveHTTPSetup(node, l1metrics.ReasonClientGone, time.Now())
	l1metrics.ObserveHTTPSetup(node, l1metrics.ReasonStaleTunnel, time.Now())
	l1metrics.AddHTTPBodyAbort(node, "nope")
	if got, _ := expositionSeries("proxy_http_tunnel_setup_ms", node); got != before {
		t.Fatalf("client_gone or stale_tunnel created setup series: %d", got)
	}
	if got, _ := expositionSeries("proxy_http_body_abort_total", node); got != 0 {
		t.Fatalf("unknown body-abort reason created %d series", got)
	}
	l1metrics.ObserveHTTPSetup(node, l1metrics.ResultOK, time.Now())
	if got, inf := expositionSeries("proxy_http_request_latency_ms", node); got != 13 || !inf {
		t.Fatalf("request latency series %d +Inf %v, want 13 including le=\"+Inf\"", got, inf)
	}
	if got, inf := expositionSeries("proxy_http_tunnel_setup_ms", node); got != 13 || !inf {
		t.Fatalf("setup series %d +Inf %v, want 13 including le=\"+Inf\"", got, inf)
	}
	raw, err := json.Marshal(l1metrics.MeterEvent{V: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte(`"protocol":""`)) {
		t.Fatalf("protocol omitempty still drops the field: %s", raw)
	}
	if bytes.Contains(raw, []byte("handshake_ms")) {
		t.Fatalf("handshake_ms is still a field: %s", raw)
	}
}

// expositionSeries counts the text series a scrape would emit for one node.
// A histogram label set is 10 explicit buckets, the implicit le="+Inf" bucket, _sum and _count.
func expositionSeries(name, node string) (int, bool) {
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return -1, false
	}
	var buf bytes.Buffer
	enc := expfmt.NewEncoder(&buf, expfmt.FmtText)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		if err := enc.Encode(mf); err != nil {
			return -1, false
		}
	}
	n := 0
	sawInf := false
	needle := `node_id="` + node + `"`
	for _, line := range strings.Split(buf.String(), "\n") {
		if line == "" || strings.HasPrefix(line, "#") || !strings.Contains(line, needle) {
			continue
		}
		n++
		if strings.Contains(line, `le="+Inf"`) {
			sawInf = true
		}
	}
	return n, sawInf
}

type netTimeout struct{}

func (netTimeout) Error() string   { return "i/o timeout" }
func (netTimeout) Timeout() bool   { return true }
func (netTimeout) Temporary() bool { return true }

func dialWrapped(t *testing.T, addr string, wrap func(net.Conn) net.Conn) func(string) (net.Conn, error) {
	t.Helper()
	return func(string) (net.Conn, error) {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			return nil, err
		}
		if wrap != nil {
			return wrap(c), nil
		}
		return c, nil
	}
}

func serveConnectThen(t *testing.T, fn func(net.Conn, *bufio.Reader)) string {
	t.Helper()
	return serveRaw(t, func(c net.Conn) {
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(4 * time.Second))
		br := bufio.NewReader(c)
		if _, err := readHTTPHead(br); err != nil {
			return
		}
		if _, err := io.WriteString(c, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
			return
		}
		fn(c, br)
	})
}

func TestPartialHeaderWriteDoesNotRetry(t *testing.T) {
	resetMetrics()
	up := serveConnectThen(t, func(c net.Conn, br *bufio.Reader) {
		if _, err := readOriginRequest(br); err != nil {
			return
		}
		_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
		_, _ = io.Copy(io.Discard, c)
	})
	lim := defaultLimits()
	lim.dial = dialWrapped(t, up, func(c net.Conn) net.Conn {
		var httpWrites int
		return &scriptConn{Conn: c, write: func(p []byte) (int, error) {
			if bytes.HasPrefix(p, []byte("CONNECT ")) {
				return c.Write(p)
			}
			httpWrites++
			if httpWrites >= 2 {
				if len(p) == 0 {
					return 0, syscall.EPIPE
				}
				return 1, syscall.EPIPE
			}
			return c.Write(p)
		}}
	})
	l1 := startL1Limited(t, staticResolver{addr: "127.0.0.1:9"}, lim)
	c, err := net.Dial("tcp", l1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(4 * time.Second))
	br := bufio.NewReader(c)
	get := fmt.Sprintf("GET http://example/a HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
		basicToken("app@node-a", "s3cret"))
	if _, err := io.WriteString(c, get); err != nil {
		t.Fatal(err)
	}
	head, err := readHTTPHead(br)
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, head, 200)
	if _, err := io.WriteString(c, get); err != nil {
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
		t.Fatalf("partial header write retried: %d", snap.HTTPRetries)
	}
	if len(snap.HTTPFails) != 1 || snap.HTTPFails[0][1] != l1metrics.ReasonStaleTunnel {
		t.Fatalf("fails %#v", snap.HTTPFails)
	}
	if len(snap.HTTPObserves) != 2 || snap.HTTPObserves[0] != l1metrics.ResultOK || snap.HTTPObserves[1] != l1metrics.ReasonStaleTunnel {
		t.Fatalf("observes %#v", snap.HTTPObserves)
	}
	assertSetup(t, snap, l1metrics.ResultOK)
}

func TestReusedReadTimeoutDoesNotRetry(t *testing.T) {
	resetMetrics()
	up := serveConnectThen(t, func(c net.Conn, br *bufio.Reader) {
		if _, err := readOriginRequest(br); err != nil {
			return
		}
		_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
		_, _ = io.Copy(io.Discard, c)
	})
	lim := defaultLimits()
	lim.dial = dialWrapped(t, up, func(c net.Conn) net.Conn {
		var httpWrites int
		return &scriptConn{
			Conn: c,
			write: func(p []byte) (int, error) {
				if !bytes.HasPrefix(p, []byte("CONNECT ")) {
					httpWrites++
				}
				return c.Write(p)
			},
			read: func(p []byte) (int, error) {
				if httpWrites >= 2 {
					return 0, netTimeout{}
				}
				return c.Read(p)
			},
		}
	})
	l1 := startL1Limited(t, staticResolver{addr: "127.0.0.1:9"}, lim)
	c, err := net.Dial("tcp", l1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(4 * time.Second))
	br := bufio.NewReader(c)
	get := fmt.Sprintf("GET http://example/a HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
		basicToken("app@node-a", "s3cret"))
	if _, err := io.WriteString(c, get); err != nil {
		t.Fatal(err)
	}
	head, err := readHTTPHead(br)
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, head, 200)
	if _, err := io.WriteString(c, get); err != nil {
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
		t.Fatalf("read timeout retried: %d", snap.HTTPRetries)
	}
	if len(snap.HTTPFails) != 1 || snap.HTTPFails[0][1] != l1metrics.ReasonL2Response {
		t.Fatalf("fails %#v", snap.HTTPFails)
	}
	if len(snap.HTTPObserves) != 2 || snap.HTTPObserves[1] != l1metrics.ReasonL2Response {
		t.Fatalf("observes %#v", snap.HTTPObserves)
	}
}

func TestPutRetryIsOptIn(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		assertPutRetry(t, defaultLimits(), false)
	})
	t.Run("enabled", func(t *testing.T) {
		lim := defaultLimits()
		lim.retryPutDelete = true
		assertPutRetry(t, lim, true)
	})
	t.Run("config-file", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "proxy.json")
		body := []byte(`{"retry_put_delete":true,"tunnel_idle":"5m","half_close_idle":"30s","l2_response_timeout":"10s"}`)
		if err := os.WriteFile(path, body, 0o644); err != nil {
			t.Fatal(err)
		}
		lim, err := LimitsFromConfig(path, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !lim.retryPutDelete || lim.tunnelIdle != 5*time.Minute || lim.l2Response != 10*time.Second || lim.headerDeadline != 10*time.Second {
			t.Fatalf("config: %+v", lim)
		}
		over, err := LimitsFromConfig(path, []string{"-retry-put-delete=false", "-l2-response-timeout=2s"})
		if err != nil {
			t.Fatal(err)
		}
		if over.retryPutDelete || over.l2Response != 2*time.Second {
			t.Fatalf("flag override: %+v", over)
		}
		assertPutRetry(t, lim, true)
	})
}

func assertPutRetry(t *testing.T, lim proxyLimits, wantRetry bool) {
	t.Helper()
	resetMetrics()
	up := serveConnectThen(t, func(c net.Conn, br *bufio.Reader) {
		if _, err := readOriginRequest(br); err != nil {
			return
		}
		_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
		_, _ = io.Copy(io.Discard, c)
	})
	lim.dial = dialWrapped(t, up, func(c net.Conn) net.Conn {
		var httpWrites int
		return &scriptConn{
			Conn: c,
			write: func(p []byte) (int, error) {
				if bytes.HasPrefix(p, []byte("CONNECT ")) {
					return c.Write(p)
				}
				httpWrites++
				if httpWrites >= 2 {
					return len(p), nil
				}
				return c.Write(p)
			},
			read: func(p []byte) (int, error) {
				if httpWrites >= 2 {
					return 0, io.EOF
				}
				return c.Read(p)
			},
		}
	})
	l1 := startL1Limited(t, staticResolver{addr: "127.0.0.1:9"}, lim)
	c, err := net.Dial("tcp", l1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(4 * time.Second))
	br := bufio.NewReader(c)
	get := fmt.Sprintf("GET http://example/a HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
		basicToken("app@node-a", "s3cret"))
	if _, err := io.WriteString(c, get); err != nil {
		t.Fatal(err)
	}
	head, err := readHTTPHead(br)
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, head, 200)
	put := fmt.Sprintf("PUT http://example/a HTTP/1.1\r\nHost: h\r\nContent-Length: 0\r\nProxy-Authorization: Basic %s\r\n\r\n",
		basicToken("app@node-a", "s3cret"))
	if _, err := io.WriteString(c, put); err != nil {
		t.Fatal(err)
	}
	resp, err := readHTTPHead(br)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.Close()
	handlers.Wait()
	snap := snapMetrics()
	if wantRetry {
		mustStatus(t, resp, 200)
		if snap.HTTPRetries != 1 || len(snap.HTTPFails) != 0 {
			t.Fatalf("retries %d fails %#v", snap.HTTPRetries, snap.HTTPFails)
		}
		if len(snap.HTTPObserves) != 2 || snap.HTTPObserves[0] != l1metrics.ResultOK || snap.HTTPObserves[1] != l1metrics.ResultOK {
			t.Fatalf("observes %#v", snap.HTTPObserves)
		}
		assertSetup(t, snap, l1metrics.ResultOK, l1metrics.ResultOK)
		if len(snap.Events) == 0 || snap.Events[len(snap.Events)-1].RequestMs <= 0 || snap.Events[len(snap.Events)-1].SetupMs <= 0 {
			t.Fatalf("retry event missing clocks: %+v", snap.Events)
		}
		return
	}
	mustStatus(t, resp, 502)
	if snap.HTTPRetries != 0 {
		t.Fatalf("PUT retried by default: %d", snap.HTTPRetries)
	}
	if len(snap.HTTPFails) != 1 || snap.HTTPFails[0][1] != l1metrics.ReasonStaleTunnel {
		t.Fatalf("fails %#v", snap.HTTPFails)
	}
}

func TestReadSideReset(t *testing.T) {
	t.Run("get", func(t *testing.T) {
		assertReadReset(t, "GET", true)
	})
	t.Run("put", func(t *testing.T) {
		assertReadReset(t, "PUT", false)
	})
}

func assertReadReset(t *testing.T, method string, wantRetry bool) {
	t.Helper()
	resetMetrics()
	up := serveConnectThen(t, func(c net.Conn, br *bufio.Reader) {
		if _, err := readOriginRequest(br); err != nil {
			return
		}
		_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
		_, _ = io.Copy(io.Discard, c)
	})
	lim := defaultLimits()
	lim.dial = dialWrapped(t, up, func(c net.Conn) net.Conn {
		var httpWrites int
		return &scriptConn{
			Conn: c,
			write: func(p []byte) (int, error) {
				if !bytes.HasPrefix(p, []byte("CONNECT ")) {
					httpWrites++
				}
				return c.Write(p)
			},
			read: func(p []byte) (int, error) {
				if httpWrites >= 2 {
					return 0, fmt.Errorf("origin read: %w", syscall.ECONNRESET)
				}
				return c.Read(p)
			},
		}
	})
	l1 := startL1Limited(t, staticResolver{addr: "127.0.0.1:9"}, lim)
	c, err := net.Dial("tcp", l1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(4 * time.Second))
	br := bufio.NewReader(c)
	token := basicToken("app@node-a", "s3cret")
	first := fmt.Sprintf("GET http://example/a HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n", token)
	if _, err := io.WriteString(c, first); err != nil {
		t.Fatal(err)
	}
	head, err := readHTTPHead(br)
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, head, 200)
	second := fmt.Sprintf("%s http://example/a HTTP/1.1\r\nHost: h\r\nContent-Length: 0\r\nProxy-Authorization: Basic %s\r\n\r\n", method, token)
	if _, err := io.WriteString(c, second); err != nil {
		t.Fatal(err)
	}
	resp, err := readHTTPHead(br)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.Close()
	handlers.Wait()
	snap := snapMetrics()
	if wantRetry {
		mustStatus(t, resp, 200)
		if snap.HTTPRetries != 1 || len(snap.HTTPFails) != 0 {
			t.Fatalf("retries %d fails %#v", snap.HTTPRetries, snap.HTTPFails)
		}
		return
	}
	mustStatus(t, resp, 502)
	if snap.HTTPRetries != 0 {
		t.Fatalf("%s retried on read reset: %d", method, snap.HTTPRetries)
	}
	if len(snap.HTTPFails) != 1 || snap.HTTPFails[0][1] != l1metrics.ReasonStaleTunnel {
		t.Fatalf("fails %#v", snap.HTTPFails)
	}
}

func TestBodyIdleIsIdleTimeout(t *testing.T) {
	resetMetrics()
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	up := serveConnectThen(t, func(c net.Conn, br *bufio.Reader) {
		if _, err := readOriginRequest(br); err != nil {
			return
		}
		_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 20\r\nConnection: close\r\n\r\n")
		select {
		case <-done:
		case <-time.After(3 * time.Second):
		}
	})
	lim := defaultLimits()
	lim.tunnelIdle = 80 * time.Millisecond
	lim.dial = dialWrapped(t, up, nil)
	l1 := startL1Limited(t, staticResolver{addr: "127.0.0.1:9"}, lim)
	c, err := net.Dial("tcp", l1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	req := fmt.Sprintf("GET http://example/a HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
		basicToken("app@node-a", "s3cret"))
	if _, err := io.WriteString(c, req); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 256)
	_, _ = c.Read(buf)
	handlers.Wait()
	snap := snapMetrics()
	if snap.BodyByReason[l1metrics.ReasonAbortIdleTimeout] != 1 {
		t.Fatalf("body abort %#v", snap.BodyByReason)
	}
	if snap.BodyByReason[l1metrics.ReasonAbortOriginReset] != 0 || snap.BodyByReason[l1metrics.ReasonAbortClientCancel] != 0 {
		t.Fatalf("idle close mislabeled %#v", snap.BodyByReason)
	}
	if len(snap.HTTPObserves) != 1 || snap.HTTPObserves[0] != l1metrics.ResultOK {
		t.Fatalf("observes %#v", snap.HTTPObserves)
	}
}

func TestHalfCloseIdleIsNotAbort(t *testing.T) {
	resetMetrics()
	up := serveConnectThen(t, func(c net.Conn, br *bufio.Reader) {
		if _, err := readOriginRequest(br); err != nil {
			return
		}
		if _, err := io.WriteString(c, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n"); err != nil {
			return
		}
		if cw, ok := c.(interface{ CloseWrite() error }); ok {
			_ = cw.CloseWrite()
		}
		_, _ = io.Copy(io.Discard, c)
	})
	lim := defaultLimits()
	lim.halfCloseIdle = 80 * time.Millisecond
	lim.tunnelIdle = 5 * time.Second
	lim.dial = dialWrapped(t, up, nil)
	l1 := startL1Limited(t, staticResolver{addr: "127.0.0.1:9"}, lim)
	c, err := net.Dial("tcp", l1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	req := fmt.Sprintf("GET http://example/ws HTTP/1.1\r\nHost: h\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nProxy-Authorization: Basic %s\r\n\r\n",
		basicToken("app@node-a", "s3cret"))
	if _, err := io.WriteString(c, req); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	head, err := readHTTPHead(br)
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, head, 101)
	handlers.Wait()
	snap := snapMetrics()
	if snap.HTTPBodyAborts != 0 {
		t.Fatalf("half-close idle counted as abort: %#v", snap.BodyByReason)
	}
	if len(snap.HTTPObserves) != 1 || snap.HTTPObserves[0] != l1metrics.ResultOK {
		t.Fatalf("observes %#v", snap.HTTPObserves)
	}
}

func TestUpgradeRSTReason(t *testing.T) {
	resetMetrics()
	release := make(chan struct{})
	up := serveConnectThen(t, func(c net.Conn, br *bufio.Reader) {
		if _, err := readOriginRequest(br); err != nil {
			return
		}
		if _, err := io.WriteString(c, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n"); err != nil {
			return
		}
		<-release
		if tcp, ok := c.(*net.TCPConn); ok {
			_ = tcp.SetLinger(0)
		}
		_ = c.Close()
	})
	lim := defaultLimits()
	lim.dial = dialWrapped(t, up, nil)
	l1 := startL1Limited(t, staticResolver{addr: "127.0.0.1:9"}, lim)
	c, err := net.Dial("tcp", l1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	req := fmt.Sprintf("GET http://example/ws HTTP/1.1\r\nHost: h\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nProxy-Authorization: Basic %s\r\n\r\n",
		basicToken("app@node-a", "s3cret"))
	if _, err := io.WriteString(c, req); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	head, err := readHTTPHead(br)
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, head, 101)
	close(release)
	handlers.Wait()
	snap := snapMetrics()
	if snap.BodyByReason[l1metrics.ReasonAbortOriginReset] != 1 {
		t.Fatalf("RST reason %#v", snap.BodyByReason)
	}
	if snap.BodyByReason[l1metrics.ReasonAbortClientCancel] != 0 || snap.BodyByReason[l1metrics.ReasonAbortRequestBody] != 0 {
		t.Fatalf("collateral reason %#v", snap.BodyByReason)
	}
}

func TestPartialHeaderTimeout(t *testing.T) {
	resetMetrics()
	lim := defaultLimits()
	lim.headerDeadline = 120 * time.Millisecond
	lim.tunnelIdle = 5 * time.Second
	l1 := startL1Limited(t, staticResolver{}, lim)
	c, err := net.Dial("tcp", l1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.WriteString(c, "GET http://127.0.0.1/a HTTP/1.1\r\nHost: ex"); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 512)
	n, _ := c.Read(buf)
	handlers.Wait()
	snap := snapMetrics()
	if n > 0 && !bytes.Contains(buf[:n], []byte("400")) {
		t.Fatalf("status %s", strconv.Quote(string(buf[:n])))
	}
	if len(snap.HTTPRejects) != 1 || snap.HTTPRejects[0] != l1metrics.ReasonRejectHeaderTimeout {
		t.Fatalf("http rejects %#v", snap.HTTPRejects)
	}
	if len(snap.Rejects) != 0 || len(snap.HTTPObserves) != 0 || len(snap.HTTPFails) != 0 {
		t.Fatalf("partial header timeout counted elsewhere: %+v", snap)
	}

	resetMetrics()
	c2, err := net.Dial("tcp", l1)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	_ = c2.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.WriteString(c2, "CONNECT example.com:443 HTTP/1.1\r\nHost: "); err != nil {
		t.Fatal(err)
	}
	n, _ = c2.Read(buf)
	handlers.Wait()
	snap = snapMetrics()
	if n > 0 && !bytes.Contains(buf[:n], []byte("400")) {
		t.Fatalf("connect status %s", strconv.Quote(string(buf[:n])))
	}
	if len(snap.Rejects) != 1 || snap.Rejects[0] != l1metrics.ReasonRejectHeaderTimeout {
		t.Fatalf("connect rejects %#v body %s", snap.Rejects, strconv.Quote(string(buf[:n])))
	}
	if len(snap.HTTPRejects) != 0 {
		t.Fatalf("connect header timeout counted as http %#v", snap.HTTPRejects)
	}
}

func TestCachedGostRejectsBadHash(t *testing.T) {
	t.Run("version", func(t *testing.T) {
		bin := filepath.Join(t.TempDir(), "fake-gost")
		script := "#!/bin/sh\necho 'gost v3.3.10 (go1.22)'\n"
		if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := gostVersionOK(bin); err == nil {
			t.Fatal("v3.3.10 was accepted as v3.3.1")
		}
		okBin := filepath.Join(t.TempDir(), "ok-gost")
		if err := os.WriteFile(okBin, []byte("#!/bin/sh\necho 'gost v3.3.1 (go1.22)'\n"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := gostVersionOK(okBin); err != nil {
			t.Fatal(err)
		}
	})
	asset, ok := gostAssets[runtime.GOOS+"/"+runtime.GOARCH]
	if !ok {
		msg := fmt.Sprintf("no pinned gost asset for %s/%s", runtime.GOOS, runtime.GOARCH)
		if os.Getenv("GOST_REQUIRED") == "1" {
			t.Fatal(msg)
		}
		t.Skip(msg)
	}
	root := t.TempDir()
	oldRoot := gostCacheRoot
	oldGet := gostGet
	gostCacheRoot = root
	t.Cleanup(func() {
		gostCacheRoot = oldRoot
		gostGet = oldGet
	})
	path, err := gostCachePath()
	if err != nil {
		t.Fatal(err)
	}
	if gostBin != "" && path == gostBin {
		t.Fatal("cache path is the binary TestMain is running")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("truncated"), 0o755); err != nil {
		t.Fatal(err)
	}
	if reusableGost(path, asset.binarySHA) {
		t.Fatal("truncated cache file was accepted")
	}
	gostGet = func(url string) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader([]byte("not-a-gost-archive"))),
			Header:     make(http.Header),
		}, nil
	}
	if err := downloadGost(asset, path); err == nil || !strings.Contains(err.Error(), "archive sha256") {
		t.Fatalf("archive mismatch: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "truncated" {
		t.Fatalf("download overwrote cache: %q", got)
	}
}

func TestGostDownloadRetries504AndTimeout(t *testing.T) {
	asset, ok := gostAssets[runtime.GOOS+"/"+runtime.GOARCH]
	if !ok {
		t.Fatalf("no pinned gost asset for %s/%s", runtime.GOOS, runtime.GOARCH)
	}
	oldGet := gostGet
	oldDelay := gostRetryDelay
	t.Cleanup(func() {
		gostGet = oldGet
		gostRetryDelay = oldDelay
	})
	gostRetryDelay = 0
	dest := filepath.Join(t.TempDir(), "gost")

	var calls int
	gostGet = func(url string) (*http.Response, error) {
		calls++
		if url != gostReleaseBase+asset.archive {
			t.Errorf("url %s", url)
		}
		if calls <= 2 {
			return &http.Response{
				Status:     "504 Gateway Timeout",
				StatusCode: http.StatusGatewayTimeout,
				Body:       io.NopCloser(strings.NewReader("")),
				Header:     make(http.Header),
			}, nil
		}
		return nil, &net.DNSError{Err: "i/o timeout", Name: "github.com", IsTimeout: true}
	}
	err := downloadGost(asset, dest)
	if err == nil || calls != gostDownloadAttempts || !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("calls %d err %v", calls, err)
	}

	calls = 0
	gostGet = func(string) (*http.Response, error) {
		calls++
		return &http.Response{
			Status:     "404 Not Found",
			StatusCode: http.StatusNotFound,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     make(http.Header),
		}, nil
	}
	err = downloadGost(asset, dest)
	if err == nil || calls != 1 || !strings.Contains(err.Error(), "404") {
		t.Fatalf("404 calls %d err %v", calls, err)
	}

	calls = 0
	gostGet = func(string) (*http.Response, error) {
		calls++
		if calls == 1 {
			return &http.Response{
				Status:     "504 Gateway Timeout",
				StatusCode: http.StatusGatewayTimeout,
				Body:       io.NopCloser(strings.NewReader("")),
				Header:     make(http.Header),
			}, nil
		}
		return &http.Response{
			Status:     "200 OK",
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("not-a-gost-archive")),
			Header:     make(http.Header),
		}, nil
	}
	err = downloadGost(asset, dest)
	if err == nil || calls != 2 || !strings.Contains(err.Error(), "archive sha256") {
		t.Fatalf("checksum after 504: calls %d err %v", calls, err)
	}
}

func TestGostDownloadFollows302(t *testing.T) {
	asset, ok := gostAssets[runtime.GOOS+"/"+runtime.GOARCH]
	if !ok {
		t.Fatalf("no pinned gost asset for %s/%s", runtime.GOOS, runtime.GOARCH)
	}
	oldGet := gostGet
	oldDelay := gostRetryDelay
	t.Cleanup(func() {
		gostGet = oldGet
		gostRetryDelay = oldDelay
	})
	gostRetryDelay = 0
	dest := filepath.Join(t.TempDir(), "gost")
	release := gostReleaseBase + asset.archive
	cdn := "https://cdn.example/gost.tar.gz"

	var calls int
	gostGet = func(rawURL string) (*http.Response, error) {
		calls++
		if rawURL == release {
			hdr := make(http.Header)
			hdr.Set("Location", cdn)
			return &http.Response{
				Status:     "302 Found",
				StatusCode: http.StatusFound,
				Header:     hdr,
				Body:       io.NopCloser(strings.NewReader("")),
			}, nil
		}
		if rawURL != cdn {
			t.Errorf("url %s", rawURL)
		}
		return &http.Response{
			Status:     "200 OK",
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("not-a-gost-archive")),
		}, nil
	}
	err := downloadGost(asset, dest)
	if err == nil || calls != 2 || !strings.Contains(err.Error(), "archive sha256") || strings.Contains(err.Error(), "302") {
		t.Fatalf("302 followed: calls %d err %v", calls, err)
	}

	calls = 0
	gostGet = func(rawURL string) (*http.Response, error) {
		calls++
		if rawURL == release {
			hdr := make(http.Header)
			hdr.Set("Location", cdn)
			return &http.Response{
				Status:     "302 Found",
				StatusCode: http.StatusFound,
				Header:     hdr,
				Body:       io.NopCloser(strings.NewReader("")),
			}, nil
		}
		return &http.Response{
			Status:     "504 Gateway Timeout",
			StatusCode: http.StatusGatewayTimeout,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("")),
		}, nil
	}
	err = downloadGost(asset, dest)
	if err == nil || calls != gostDownloadAttempts*2 || !strings.Contains(err.Error(), "504") || strings.Contains(err.Error(), "302") {
		t.Fatalf("final 504: calls %d err %v", calls, err)
	}
}

func TestPrepareGostUsesBinOverride(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "gost")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\necho 'gost v3.3.1'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOST_BIN", bin)
	oldGet := gostGet
	gostGet = func(string) (*http.Response, error) {
		t.Fatal("downloaded despite GOST_BIN")
		return nil, nil
	}
	t.Cleanup(func() { gostGet = oldGet })
	got, err := prepareGost()
	if err != nil || got != bin {
		t.Fatalf("got %s err %v", got, err)
	}
}

func TestResolveReasonLabelsConnect(t *testing.T) {
	cases := []struct {
		name   string
		res    Resolver
		node   string
		reason string
	}{
		{"unknown", staticResolver{err: ErrUnknownNode}, l1metrics.NodeUnknown, l1metrics.ReasonUnknownNode},
		{"empty", staticResolver{}, l1metrics.NodeUnknown, l1metrics.ReasonEmptyAddr},
		{"resolver", staticResolver{err: errors.New("etcd unavailable")}, l1metrics.NodeUnknown, l1metrics.ReasonResolverError},
		{"no-healthy", staticResolver{err: ErrNoHealthyAddr}, "node-a", l1metrics.ReasonNoUpstream},
	}
	req := fmt.Sprintf("CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\nProxy-Authorization: Basic %s\r\n\r\n",
		basicToken("app@node-a", "s3cret"))
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetMetrics()
			l1 := startL1(t, tc.res)
			resp := doRequest(t, l1, req)
			mustStatus(t, resp, 502)
			handlers.Wait()
			snap := snapMetrics()
			if len(snap.Fails) != 1 || snap.Fails[0] != [2]string{tc.node, tc.reason} {
				t.Fatalf("fails %#v", snap.Fails)
			}
			if len(snap.HTTPFails) != 0 || len(snap.Observes) != 0 {
				t.Fatalf("leaked %#v %#v", snap.HTTPFails, snap.Observes)
			}
			if len(snap.Events) != 1 || snap.Events[0].Result != tc.reason || snap.Events[0].Protocol != l1metrics.ProtocolConnect || snap.Events[0].NodeID != "node-a" {
				t.Fatalf("event %+v", snap.Events)
			}
		})
	}
}

func TestConnectIdleTimeout(t *testing.T) {
	resetMetrics()
	origin := serveRaw(t, func(c net.Conn) {
		defer c.Close()
		br := bufio.NewReader(c)
		_ = c.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := readHTTPHead(br); err != nil {
			return
		}
		if _, err := io.WriteString(c, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
			return
		}
		_ = c.SetDeadline(time.Time{})
		buf := make([]byte, 1)
		_, _ = c.Read(buf)
	})
	lim := defaultLimits()
	lim.tunnelIdle = 150 * time.Millisecond
	l1 := startL1Limited(t, mapResolver{m: map[string]string{"node-a": origin}}, lim)
	c, err := net.Dial("tcp", l1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	req := fmt.Sprintf("CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\nProxy-Authorization: Basic %s\r\n\r\n",
		basicToken("app@node-a", "s3cret"))
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	resp := writeAndRead(t, c, req)
	mustStatus(t, resp, 200)
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 8)
	if _, err := c.Read(buf); err == nil {
		t.Fatal("expected idle close")
	}
	handlers.Wait()
	snap := snapMetrics()
	if snap.TunnelByReason[l1metrics.ReasonAbortIdleTimeout] != 1 {
		t.Fatalf("connect idle %#v entered %d", snap.TunnelByReason, snap.Entered)
	}
	if snap.Entered < 1 || len(snap.Observes) == 0 || snap.Observes[0] != l1metrics.ResultOK {
		t.Fatalf("tunnel was not established: %+v", snap)
	}
	if snap.BodyByReason[l1metrics.ReasonAbortIdleTimeout] != 0 || len(snap.HTTPFails) != 0 {
		t.Fatalf("counted on http %#v %#v", snap.BodyByReason, snap.HTTPFails)
	}
}

func TestSlowlorisHeaderDeadline(t *testing.T) {
	resetMetrics()
	lim := defaultLimits()
	lim.headerDeadline = 200 * time.Millisecond
	lim.tunnelIdle = 5 * time.Second
	l1 := startL1Limited(t, staticResolver{}, lim)
	c, err := net.Dial("tcp", l1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	start := time.Now()
	go func() {
		if _, err := c.Write([]byte("GET ")); err != nil {
			return
		}
		tick := time.NewTicker(40 * time.Millisecond)
		defer tick.Stop()
		for range tick.C {
			if _, err := c.Write([]byte("x")); err != nil {
				return
			}
		}
	}()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 256)
	n, _ := c.Read(buf)
	elapsed := time.Since(start)
	if elapsed < 150*time.Millisecond || elapsed > time.Second {
		t.Fatalf("header deadline cut at %s", elapsed)
	}
	handlers.Wait()
	snap := snapMetrics()
	if len(snap.HTTPRejects) != 1 || snap.HTTPRejects[0] != l1metrics.ReasonRejectHeaderTimeout {
		t.Fatalf("rejects %#v status %s", snap.HTTPRejects, strconv.Quote(string(buf[:n])))
	}
	if len(snap.Rejects) != 0 || len(snap.HTTPFails) != 0 {
		t.Fatalf("counted elsewhere %+v", snap)
	}
}

func serveKeepAliveOrigin(t *testing.T) string {
	t.Helper()
	return serveRaw(t, func(c net.Conn) {
		defer c.Close()
		br := bufio.NewReader(c)
		_ = c.SetDeadline(time.Now().Add(4 * time.Second))
		if _, err := readHTTPHead(br); err != nil {
			return
		}
		if _, err := io.WriteString(c, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
			return
		}
		for {
			if _, err := readOriginRequest(br); err != nil {
				return
			}
			if _, err := io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"); err != nil {
				return
			}
		}
	})
}

func TestKeepAliveIdleSurvivesHeaderDeadline(t *testing.T) {
	resetMetrics()
	origin := serveKeepAliveOrigin(t)
	lim := defaultLimits()
	lim.headerDeadline = 200 * time.Millisecond
	lim.tunnelIdle = time.Second
	l1 := startL1Limited(t, mapResolver{m: map[string]string{"node-a": origin}}, lim)
	c, err := net.Dial("tcp", l1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	br := bufio.NewReader(c)
	req := fmt.Sprintf("GET http://%s/a HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
		origin, basicToken("app@node-a", "s3cret"))
	if _, err := io.WriteString(c, req); err != nil {
		t.Fatal(err)
	}
	head, err := readHTTPHead(br)
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, head, 200)
	time.Sleep(450 * time.Millisecond)
	if _, err := io.WriteString(c, req); err != nil {
		t.Fatalf("keep-alive closed inside header deadline: %v", err)
	}
	head, err = readHTTPHead(br)
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, head, 200)
	_ = c.Close()
	handlers.Wait()
	snap := snapMetrics()
	if len(snap.HTTPRejects) != 0 {
		t.Fatalf("idle keep-alive counted a reject %#v", snap.HTTPRejects)
	}
	if len(snap.HTTPObserves) != 2 {
		t.Fatalf("observes %#v", snap.HTTPObserves)
	}
}

func TestKeepAliveTrickleHeader(t *testing.T) {
	resetMetrics()
	origin := serveKeepAliveOrigin(t)
	lim := defaultLimits()
	lim.headerDeadline = 200 * time.Millisecond
	lim.tunnelIdle = 5 * time.Second
	l1 := startL1Limited(t, mapResolver{m: map[string]string{"node-a": origin}}, lim)
	c, err := net.Dial("tcp", l1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	br := bufio.NewReader(c)
	req := fmt.Sprintf("GET http://%s/a HTTP/1.1\r\nHost: h\r\nProxy-Authorization: Basic %s\r\n\r\n",
		origin, basicToken("app@node-a", "s3cret"))
	if _, err := io.WriteString(c, req); err != nil {
		t.Fatal(err)
	}
	head, err := readHTTPHead(br)
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, head, 200)
	start := time.Now()
	go func() {
		if _, err := c.Write([]byte("GET ")); err != nil {
			return
		}
		tick := time.NewTicker(40 * time.Millisecond)
		defer tick.Stop()
		for range tick.C {
			if _, err := c.Write([]byte("x")); err != nil {
				return
			}
		}
	}()
	buf := make([]byte, 256)
	n, _ := c.Read(buf)
	elapsed := time.Since(start)
	if elapsed < 150*time.Millisecond || elapsed > time.Second {
		t.Fatalf("second header cut at %s", elapsed)
	}
	handlers.Wait()
	snap := snapMetrics()
	if len(snap.HTTPRejects) != 1 || snap.HTTPRejects[0] != l1metrics.ReasonRejectHeaderTimeout {
		t.Fatalf("rejects %#v status %s", snap.HTTPRejects, strconv.Quote(string(buf[:n])))
	}
	if len(snap.HTTPObserves) != 1 || snap.HTTPObserves[0] != l1metrics.ResultOK {
		t.Fatalf("first request %#v", snap.HTTPObserves)
	}
}

func TestConnectHalfClosePreemptsTunnelIdle(t *testing.T) {
	resetMetrics()
	origin := serveRaw(t, func(c net.Conn) {
		defer c.Close()
		br := bufio.NewReader(c)
		_ = c.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := readHTTPHead(br); err != nil {
			return
		}
		if _, err := io.WriteString(c, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
			return
		}
		if cw, ok := c.(interface{ CloseWrite() error }); ok {
			_ = cw.CloseWrite()
		}
		_, _ = io.Copy(io.Discard, c)
	})
	lim := defaultLimits()
	lim.tunnelIdle = 120 * time.Millisecond
	lim.halfCloseIdle = 700 * time.Millisecond
	l1 := startL1Limited(t, mapResolver{m: map[string]string{"node-a": origin}}, lim)
	c, err := net.Dial("tcp", l1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	req := fmt.Sprintf("CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\nProxy-Authorization: Basic %s\r\n\r\n",
		basicToken("app@node-a", "s3cret"))
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	resp := writeAndRead(t, c, req)
	mustStatus(t, resp, 200)
	start := time.Now()
	handlers.Wait()
	elapsed := time.Since(start)
	if elapsed < 400*time.Millisecond {
		t.Fatalf("tunnel idle closed the half-close at %s", elapsed)
	}
	snap := snapMetrics()
	if snap.TunnelByReason[l1metrics.ReasonAbortIdleTimeout] != 0 {
		t.Fatalf("half-close recorded as abort %#v", snap.TunnelByReason)
	}
	if snap.Entered < 1 || len(snap.Observes) == 0 || snap.Observes[0] != l1metrics.ResultOK {
		t.Fatalf("tunnel was not established: %+v", snap)
	}
}

func TestLimitsFromConfigBounds(t *testing.T) {
	dir := t.TempDir()
	write := func(body string) string {
		t.Helper()
		p := filepath.Join(dir, "proxy.json")
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	if _, err := LimitsFromConfig(write(`{"nope":true}`), nil); err == nil {
		t.Fatal("unknown field accepted")
	}
	if _, err := LimitsFromConfig(write(`{"tunnel_idle":"0s"}`), nil); err == nil {
		t.Fatal("zero duration accepted")
	}
	if _, err := LimitsFromConfig(write(`{"tunnel_idle":"-1s"}`), nil); err == nil {
		t.Fatal("negative duration accepted")
	}
	if _, err := LimitsFromConfig(write(`{"tunnel_idle":"1h1s"}`), nil); err == nil {
		t.Fatal("tunnel idle above 1h accepted")
	}
	if _, err := LimitsFromConfig(write(`{"header_deadline":"1m1s"}`), nil); err == nil {
		t.Fatal("header deadline above 1m accepted")
	}
	if _, err := LimitsFromConfig(write(`{"l2_response_timeout":"2m1s"}`), nil); err == nil {
		t.Fatal("l2 response above 2m accepted")
	}
	if _, err := LimitsFromConfig(write(`{"tunnel_idle":"20s","half_close_idle":"30s"}`), nil); err == nil {
		t.Fatal("half-close idle above tunnel idle accepted")
	}
	if _, err := LimitsFromConfig("", []string{"-tunnel-idle=10s"}); err == nil {
		t.Fatal("flag left half-close idle above tunnel idle")
	}
	if _, err := LimitsFromConfig(write("{\"tunnel_idle\":\"5m\"}\n"), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := LimitsFromConfig(write(`{"tunnel_idle":"5m"}{"header_deadline":"1s"}`), nil); err == nil {
		t.Fatal("trailing JSON object accepted")
	}
	for _, arg := range []string{"-tunnel-idle=1h1s", "-half-close-idle=1h1s", "-l2-response-timeout=2m1s", "-header-deadline=1m1s", "-tunnel-idle=0s", "-header-deadline=-1s"} {
		if _, err := LimitsFromConfig("", []string{arg}); err == nil {
			t.Fatalf("flag %s accepted", arg)
		}
	}
	lim, err := LimitsFromConfig(write(`{"tunnel_idle":"1h","half_close_idle":"1h","l2_response_timeout":"2m","header_deadline":"1m"}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	if lim.tunnelIdle != time.Hour || lim.halfCloseIdle != time.Hour || lim.l2Response != 2*time.Minute || lim.headerDeadline != time.Minute {
		t.Fatalf("boundary %+v", lim)
	}
	lim, err = LimitsFromConfig("", []string{"-header-deadline=10s", "-half-close-idle=10s", "-tunnel-idle=10s"})
	if err != nil {
		t.Fatal(err)
	}
	if lim.headerDeadline != 10*time.Second || lim.halfCloseIdle != 10*time.Second || lim.tunnelIdle != 10*time.Second {
		t.Fatalf("flags %+v", lim)
	}
}
