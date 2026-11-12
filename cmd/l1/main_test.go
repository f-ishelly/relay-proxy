package main

import (
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestStartRejectsBadConfigBeforeListen(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = start([]string{"-listen=" + addr, "-metrics=", "-header-deadline=0s"})
	if err == nil || !strings.Contains(err.Error(), "header-deadline") {
		t.Fatalf("err=%v", err)
	}
	again, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("bad config left %s bound: %v", addr, err)
	}
	again.Close()
}

func TestStartEnforcesHeaderDeadline(t *testing.T) {
	rt, err := start([]string{"-listen=127.0.0.1:0", "-metrics=", "-header-deadline=200ms"})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.stop()
	got, elapsed := trickleHeader(t, rt.addr, 2*time.Second)
	if elapsed < 150*time.Millisecond || elapsed > time.Second {
		t.Fatalf("header deadline cut at %s body %q", elapsed, got)
	}
	if !strings.Contains(got, "HTTP/1.1 400") || !strings.Contains(got, "Content-Length: 0") || !strings.Contains(got, "Connection: close") {
		t.Fatalf("response %q", got)
	}
	if strings.Contains(got, "use CONNECT") {
		t.Fatalf("https hint on a partial GET: %q", got)
	}
}

func TestStartDefaultHeaderDeadlineIsNotShort(t *testing.T) {
	rt, err := start([]string{"-listen=127.0.0.1:0", "-metrics="})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.stop()
	c, err := net.Dial("tcp", rt.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := io.WriteString(c, "GET "); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	_ = c.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	buf := make([]byte, 256)
	n, err := c.Read(buf)
	if n != 0 || err == nil {
		t.Fatalf("default header deadline responded early: n=%d err=%v %q", n, err, buf[:n])
	}
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("expected read timeout, got %v %q", err, buf[:n])
	}
}

func trickleHeader(t *testing.T, addr string, readFor time.Duration) (string, time.Duration) {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	done := make(chan struct{})
	defer close(done)
	started := time.Now()
	go func() {
		if _, err := c.Write([]byte("GET ")); err != nil {
			return
		}
		tick := time.NewTicker(40 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-done:
				return
			case <-tick.C:
				if _, err := c.Write([]byte("x")); err != nil {
					return
				}
			}
		}
	}()
	_ = c.SetReadDeadline(time.Now().Add(readFor))
	buf := make([]byte, 512)
	n, _ := c.Read(buf)
	return string(buf[:n]), time.Since(started)
}
