package proxy

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestNewFromArgsDefaults(t *testing.T) {
	srv, opt, err := NewFromArgs(nil)
	if err != nil {
		t.Fatal(err)
	}
	assertDefaultLimits(t, srv)
	if opt.Listen != ":8080" || opt.Metrics != "127.0.0.1:9090" {
		t.Fatalf("options %+v", opt)
	}
	if _, err := srv.resolver.Resolve("missing"); !errors.Is(err, ErrUnknownNode) {
		t.Fatalf("missing route: %v", err)
	}
	assertPackageDefaults(t)
}

func TestNewFromArgsEmptyObjectKeepsDefaults(t *testing.T) {
	path := writeProxyConfig(t, `{}`+"\n")
	srv, opt, err := NewFromArgs([]string{"-proxy-config", path, "-metrics="})
	if err != nil {
		t.Fatal(err)
	}
	assertDefaultLimits(t, srv)
	if opt.Metrics != "" || opt.Listen != ":8080" {
		t.Fatalf("options %+v", opt)
	}
	assertPackageDefaults(t)
}

func TestNewFromArgsFileAndFlagOverride(t *testing.T) {
	path := writeProxyConfig(t, `{
		"retry_put_delete": true,
		"tunnel_idle": "5m",
		"half_close_idle": "30s",
		"l2_response_timeout": "10s",
		"header_deadline": "1s"
	}`)
	srv, _, err := NewFromArgs([]string{
		"-proxy-config=" + path,
		"-header-deadline=200ms",
		"-retry-put-delete=false",
		"-listen", "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	tunnel, half, l2, header, retry := srv.Limits()
	if tunnel != 5*time.Minute || half != 30*time.Second || l2 != 10*time.Second || header != 200*time.Millisecond || retry {
		t.Fatalf("limits %s %s %s %s retry=%v", tunnel, half, l2, header, retry)
	}
	assertPackageDefaults(t)
}

func TestNewFromArgsAcceptsUpperBounds(t *testing.T) {
	path := writeProxyConfig(t, `{"tunnel_idle":"1h","half_close_idle":"1h","l2_response_timeout":"2m","header_deadline":"1m"}`)
	srv, _, err := NewFromArgs([]string{"-proxy-config=" + path})
	if err != nil {
		t.Fatal(err)
	}
	tunnel, half, l2, header, retry := srv.Limits()
	if tunnel != time.Hour || half != time.Hour || l2 != 2*time.Minute || header != time.Minute || retry {
		t.Fatalf("limits %s %s %s %s retry=%v", tunnel, half, l2, header, retry)
	}
}

func TestNewFromArgsRejectsInvalidLimits(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name string
		body string
		want string
	}{
		{name: "unknown field", body: `{"nope":true}`, want: "unknown field"},
		{name: "zero", body: `{"tunnel_idle":"0s"}`, want: "tunnel_idle"},
		{name: "negative", body: `{"header_deadline":"-1s"}`, want: "header_deadline"},
		{name: "tunnel too long", body: `{"tunnel_idle":"1h1s"}`, want: "tunnel_idle"},
		{name: "header too long", body: `{"header_deadline":"1m1s"}`, want: "header_deadline"},
		{name: "l2 too long", body: `{"l2_response_timeout":"2m1s"}`, want: "l2_response_timeout"},
		{name: "half above tunnel", body: `{"tunnel_idle":"20s","half_close_idle":"30s"}`, want: "must be <="},
		{name: "trailing", body: `{"tunnel_idle":"5m"}{"header_deadline":"1s"}`, want: "trailing data"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(dir, tc.name+".json")
			if err := os.WriteFile(path, []byte(tc.body), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, _, err := NewFromArgs([]string{"-proxy-config=" + path}); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v want %q", err, tc.want)
			}
		})
	}
	flags := []struct {
		arg  string
		want string
	}{
		{"-tunnel-idle=10s", "must be <="},
		{"-tunnel-idle=1h1s", "tunnel-idle"},
		{"-half-close-idle=1h1s", "half-close-idle"},
		{"-l2-response-timeout=2m1s", "l2-response-timeout"},
		{"-header-deadline=1m1s", "header-deadline"},
		{"-tunnel-idle=0s", "tunnel-idle"},
		{"-header-deadline=-1s", "header-deadline"},
		{"-retry-put-delete=maybe", "retry-put-delete"},
	}
	for _, tc := range flags {
		t.Run(tc.arg, func(t *testing.T) {
			if _, _, err := NewFromArgs([]string{tc.arg}); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v want %q", err, tc.want)
			}
		})
	}
	path := writeProxyConfig(t, `{"tunnel_idle":"1h","half_close_idle":"1h"}`)
	if _, _, err := NewFromArgs([]string{"-proxy-config=" + path, "-tunnel-idle=30s"}); err == nil || !strings.Contains(err.Error(), "must be <=") {
		t.Fatalf("flag override did not revalidate: %v", err)
	}
	if _, _, err := NewFromArgs([]string{"-proxy-config=" + filepath.Join(dir, "missing.json")}); err == nil {
		t.Fatal("missing config accepted")
	}
	if _, _, err := NewFromArgs([]string{"-nope"}); err == nil {
		t.Fatal("unknown flag accepted")
	}
	if _, _, err := NewFromArgs([]string{"-listen=:8080", "extra"}); err == nil {
		t.Fatal("positional argument accepted")
	}
	assertPackageDefaults(t)
}

func TestNewFromArgsRoutes(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "routes.json")
	if err := os.WriteFile(good, []byte("{\n  \"node-a\": \"10.0.8.26:8080\"\n}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	srv, _, err := NewFromArgs([]string{"-routes=" + good})
	if err != nil {
		t.Fatal(err)
	}
	addr, err := srv.resolver.Resolve("node-a")
	if err != nil || addr != "10.0.8.26:8080" {
		t.Fatalf("route %q %v", addr, err)
	}
	if _, err := srv.resolver.Resolve("other"); !errors.Is(err, ErrUnknownNode) {
		t.Fatalf("other: %v", err)
	}
	for _, body := range []string{`null`, `[]`, `{"n":"10.0.0.1"}`, `{"n":"10.0.0.1:1"}{"n":"10.0.0.2:1"}`, ``, `{"": "10.0.0.1:1"}`} {
		path := filepath.Join(dir, "bad.json")
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, _, err := NewFromArgs([]string{"-routes=" + path}); err == nil {
			t.Fatalf("accepted routes %q", body)
		}
	}
	if _, _, err := NewFromArgs([]string{"-routes=" + filepath.Join(dir, "nope.json")}); err == nil {
		t.Fatal("missing routes accepted")
	}
}

func TestCheckedInExamples(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller")
	}
	root := filepath.Join(filepath.Dir(file), "..", "..", "..")
	srv, _, err := NewFromArgs([]string{
		"-proxy-config=" + filepath.Join(root, "configs", "l1.example.json"),
		"-routes=" + filepath.Join(root, "configs", "l1.routes.example.json"),
	})
	if err != nil {
		t.Fatal(err)
	}
	assertDefaultLimits(t, srv)
	addr, err := srv.resolver.Resolve("node-az01-003")
	if err != nil || addr != "10.0.8.26:8080" {
		t.Fatalf("example route %q %v", addr, err)
	}
}

func TestNewRejectsNilResolver(t *testing.T) {
	if _, err := New(nil, "", nil); err == nil {
		t.Fatal("nil resolver accepted")
	}
}

func assertDefaultLimits(t *testing.T, srv *Server) {
	t.Helper()
	tunnel, half, l2, header, retry := srv.Limits()
	if tunnel != 5*time.Minute || half != 30*time.Second || l2 != 10*time.Second || header != 10*time.Second || retry {
		t.Fatalf("defaults %s %s %s %s retry=%v", tunnel, half, l2, header, retry)
	}
}

func assertPackageDefaults(t *testing.T) {
	t.Helper()
	if tunnelIdleTimeout != 5*time.Minute || halfCloseIdleTimeout != 30*time.Second || l2ResponseTimeout != 10*time.Second || headerDeadline != 10*time.Second || retryPutDelete {
		t.Fatalf("package defaults mutated: %s %s %s %s retry=%v", tunnelIdleTimeout, halfCloseIdleTimeout, l2ResponseTimeout, headerDeadline, retryPutDelete)
	}
}

func writeProxyConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "proxy.json")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}
