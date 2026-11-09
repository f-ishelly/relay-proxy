package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"
)

// Server 是生产 L1。每条连接用的超时和 retry_put_delete 只来自 LimitsFromConfig，
// 不写包级变量，也不在进程里另放一套默认值。
type Server struct {
	lim      proxyLimits
	resolver Resolver
}

// Options 是限制以外的进程参数。限制仍由 LimitsFromConfig 校验。
type Options struct {
	// Listen 是客户端入口。默认 :8080。
	Listen string
	// Metrics 是 Prometheus 管理地址，与代理端口分开。空字符串表示不监听。
	// 默认 127.0.0.1:9090。
	Metrics string
}

// New 用 LimitsFromConfig 装入限制。path 与 args 的含义与该函数相同：
// args 里的 -proxy-config 优先于 path；没写的时长用文档里的默认值。
// 配置不合法时返回错误，调用方不要监听。resolver 不能是 nil。
func New(resolver Resolver, path string, args []string) (*Server, error) {
	if resolver == nil {
		return nil, errors.New("nil resolver")
	}
	lim, err := LimitsFromConfig(path, args)
	if err != nil {
		return nil, err
	}
	return &Server{lim: lim, resolver: resolver}, nil
}

// NewFromArgs 是 cmd/l1 的构造入口。args 通常是 os.Args[1:]。
// 进程旗标是 -listen、-metrics、-routes。其余旗标原样交给 LimitsFromConfig：
// -proxy-config、-retry-put-delete、-tunnel-idle、-half-close-idle、
// -l2-response-timeout、-header-deadline。
// 限制不合法，或路由文件打不开、不是对象、地址不是 host:port，都返回错误，不监听。
// 没给 -routes 时，每个 node_id 都是 ErrUnknownNode。
func NewFromArgs(args []string) (*Server, Options, error) {
	fs := flag.NewFlagSet("l1", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.Usage = func() {}
	listen := fs.String("listen", ":8080", "client listen address")
	metrics := fs.String("metrics", "127.0.0.1:9090", "Prometheus listen address; empty disables")
	routes := fs.String("routes", "", "JSON object of node_id to L2 host:port")
	fs.String("proxy-config", "", "limits JSON file")
	fs.String("retry-put-delete", "", "true retries PUT and DELETE")
	fs.String("tunnel-idle", "", "tunnel idle timeout")
	fs.String("half-close-idle", "", "idle timeout after half-close")
	fs.String("l2-response-timeout", "", "L2 CONNECT response timeout")
	fs.String("header-deadline", "", "absolute request-header deadline")
	if err := fs.Parse(args); err != nil {
		return nil, Options{}, err
	}
	if extra := fs.Args(); len(extra) > 0 {
		return nil, Options{}, fmt.Errorf("unexpected arguments: %s", strings.Join(extra, " "))
	}
	opt := Options{Listen: *listen, Metrics: *metrics}
	limArgs := configuredLimitArgs(fs)
	// 先拒绝坏的限制，避免一份坏路由文件把真正的配置错误盖住。
	if _, err := LimitsFromConfig("", limArgs); err != nil {
		return nil, Options{}, err
	}
	table, err := loadRoutes(*routes)
	if err != nil {
		return nil, Options{}, err
	}
	srv, err := New(routeTable{m: table}, "", limArgs)
	if err != nil {
		return nil, Options{}, err
	}
	return srv, opt, nil
}

func configuredLimitArgs(fs *flag.FlagSet) []string {
	var out []string
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "proxy-config", "retry-put-delete", "tunnel-idle", "half-close-idle", "l2-response-timeout", "header-deadline":
			out = append(out, "-"+f.Name+"="+f.Value.String())
		}
	})
	return out
}

// Limits 返回已经校验过的时长和重试开关。
func (s *Server) Limits() (tunnelIdle, halfCloseIdle, l2Response, headerDeadline time.Duration, retryPutDelete bool) {
	return s.lim.tunnelIdle, s.lim.halfCloseIdle, s.lim.l2Response, s.lim.headerDeadline, s.lim.retryPutDelete
}

// Serve 在 ln 上接受连接，并把这份限制传给 serveProxyConnLimited。
// 关闭 ln 后返回。进行中的连接自己结束。
func (s *Server) Serve(ln net.Listener) error {
	for {
		c, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go func(conn net.Conn) {
			_ = serveProxyConnLimited(conn, s.resolver, s.lim)
		}(c)
	}
}

// routeTable 是 -routes 里的静态表。启动之后不要改 m，Resolve 会并发读。
// etcd Watch 仍按设计文档 §8.3，本进程不接。
type routeTable struct {
	m map[string]string
}

func (r routeTable) Resolve(id string) (string, error) {
	a, ok := r.m[id]
	if !ok {
		return "", ErrUnknownNode
	}
	if a == "" {
		return "", ErrNoHealthyAddr
	}
	return a, nil
}

func loadRoutes(path string) (map[string]string, error) {
	if path == "" {
		return nil, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	trim := bytes.TrimSpace(b)
	if len(trim) == 0 || trim[0] != '{' {
		return nil, errors.New("routes: JSON object required")
	}
	dec := json.NewDecoder(bytes.NewReader(trim))
	dec.DisallowUnknownFields()
	var m map[string]string
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("routes: %w", err)
	}
	if dec.More() {
		return nil, errors.New("routes: trailing data after JSON object")
	}
	for id, addr := range m {
		if id == "" || addr == "" {
			return nil, errors.New("routes: empty node_id or address")
		}
		if _, _, err := net.SplitHostPort(addr); err != nil {
			return nil, fmt.Errorf("routes: %s: %w", id, err)
		}
	}
	return m, nil
}
