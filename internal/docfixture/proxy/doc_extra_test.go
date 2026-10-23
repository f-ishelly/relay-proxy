//go:build docfixture

package proxy

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
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
	wantMetrics := append([]byte("//go:build docfixture\n\n"), fences[0]...)
	if !bytes.Equal(metrics, wantMetrics) {
		t.Fatalf("l1metrics/metrics.go is not the §7 fence byte-for-byte (%d vs %d)", len(metrics), len(wantMetrics))
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

func dialWrapped(t *testing.T, addr string, wrap func(net.Conn) net.Conn) {
	t.Helper()
	orig := dialUpstream
	dialUpstream = func(string) (net.Conn, error) {
		c, err := net.Dial("tcp", addr)
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
	dialWrapped(t, up, func(c net.Conn) net.Conn {
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
	l1 := startL1(t, staticResolver{addr: "127.0.0.1:9"})
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
	dialWrapped(t, up, func(c net.Conn) net.Conn {
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
	l1 := startL1(t, staticResolver{addr: "127.0.0.1:9"})
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
		assertPutRetry(t, false)
	})
	t.Run("enabled", func(t *testing.T) {
		old := retryPutDelete
		retryPutDelete = true
		t.Cleanup(func() {
			handlers.Wait()
			retryPutDelete = old
		})
		assertPutRetry(t, true)
	})
}

func assertPutRetry(t *testing.T, wantRetry bool) {
	t.Helper()
	resetMetrics()
	up := serveConnectThen(t, func(c net.Conn, br *bufio.Reader) {
		if _, err := readOriginRequest(br); err != nil {
			return
		}
		_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
		_, _ = io.Copy(io.Discard, c)
	})
	dialWrapped(t, up, func(c net.Conn) net.Conn {
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
	l1 := startL1(t, staticResolver{addr: "127.0.0.1:9"})
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
