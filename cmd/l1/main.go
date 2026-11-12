// Command l1 是生产 L1。超时和 retry_put_delete 只从 LimitsFromConfig 来。
// 没写的项用设计文档里的默认值。配置不合法就退出，不监听。
package main

import (
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/f-ishelly/relay-proxy/internal/docfixture/l1metrics"
	"github.com/f-ishelly/relay-proxy/internal/docfixture/proxy"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	rt, err := start(args)
	if err != nil {
		return err
	}
	defer rt.stop()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sig)
	<-sig
	return nil
}

type running struct {
	addr string
	stop func()
}

func start(args []string) (*running, error) {
	srv, opt, err := proxy.NewFromArgs(args)
	if err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", opt.Listen)
	if err != nil {
		return nil, err
	}
	var metricsLn net.Listener
	if opt.Metrics != "" {
		metricsLn, err = net.Listen("tcp", opt.Metrics)
		if err != nil {
			_ = ln.Close()
			return nil, err
		}
	}
	go func() { _ = srv.Serve(ln) }()
	if metricsLn != nil {
		mux := http.NewServeMux()
		l1metrics.Mount(mux)
		go func() { _ = http.Serve(metricsLn, mux) }()
	}
	tunnel, half, l2, header, retry := srv.Limits()
	log.Printf("listen %s metrics %q tunnel_idle=%s half_close_idle=%s l2_response_timeout=%s header_deadline=%s retry_put_delete=%v",
		ln.Addr(), opt.Metrics, tunnel, half, l2, header, retry)
	return &running{
		addr: ln.Addr().String(),
		stop: func() {
			_ = ln.Close()
			if metricsLn != nil {
				_ = metricsLn.Close()
			}
		},
	}, nil
}
