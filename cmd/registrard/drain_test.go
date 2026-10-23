package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/f-ishelly/relay-proxy/internal/etcdcluster"
	"github.com/f-ishelly/relay-proxy/registrar"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func TestRejectZeroJitterUnlessDevFlag(t *testing.T) {
	zero := 0
	neg := -1
	if _, err := applyFile(fileConfig{RedialJitterMS: &zero}, false); err == nil {
		t.Fatal("production config accepted jitter 0")
	}
	if _, err := applyFile(fileConfig{RedialJitterMS: &neg}, true); err == nil {
		t.Fatal("negative jitter accepted")
	}
	cfg, err := applyFile(fileConfig{RedialJitterMS: &zero}, true)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RedialJitter != 0 {
		t.Fatalf("dev flag left jitter %s", cfg.RedialJitter)
	}
	cfg, err = applyFile(fileConfig{}, false)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RedialJitter != registrar.DefaultRedialJitter {
		t.Fatalf("omitted jitter %s", cfg.RedialJitter)
	}
	ms := 1500
	cfg, err = applyFile(fileConfig{RedialJitterMS: &ms}, false)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RedialJitter != 1500*time.Millisecond {
		t.Fatalf("jitter %s", cfg.RedialJitter)
	}
}

func TestDrainProceedsWhenClientAlreadyGone(t *testing.T) {
	cl, err := etcdcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
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
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 1024)
				_, _ = c.Read(buf)
				_, _ = c.Write([]byte("HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n"))
			}(c)
		}
	}()

	cfg := registrar.DefaultConfig()
	cfg.Endpoints = cl.Endpoints()
	cfg.NodeID = "626220"
	cfg.IntranetAddr = "10.0.8.26:8080"
	cfg.ProxyPort = 8080
	cfg.ProbeAddr = ln.Addr().String()
	cfg.ProbeUser = "appuser@626220"
	cfg.ProbePassword = "s3cret"
	cfg.StateFile = filepath.Join(t.TempDir(), "state.json")
	cfg.MachineID = "host-a"
	cfg.LocalIP = func() string { return "100.64.0.1" }
	cfg.ProbeInterval = 20 * time.Millisecond
	cfg.ProbeTimeout = 200 * time.Millisecond
	cfg.ProbeRiseThreshold = 1
	cfg.RedialJitter = 300 * time.Millisecond
	reg, err := registrar.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reg.Stop(context.Background()) })
	if err := reg.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && !reg.Status().ProbeOK {
		time.Sleep(20 * time.Millisecond)
	}
	if err := reg.Ready(context.Background(), "100.64.0.1", ""); err != nil {
		t.Fatal(err)
	}

	h := newHandler(reg)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodPost, "/v1/drain", nil).WithContext(ctx)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status %d body %s", rr.Code, rr.Body.String())
	}

	cli, err := clientv3.New(clientv3.Config{Endpoints: cl.Endpoints(), DialTimeout: 2 * time.Second, Logger: zap.NewNop()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	key := registrar.Key("626220")
	deadline = time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		c, cancel := context.WithTimeout(context.Background(), time.Second)
		resp, err := cli.Get(c, key)
		cancel()
		if err == nil && len(resp.Kvs) == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("cancelled client left the business key in etcd")
}
