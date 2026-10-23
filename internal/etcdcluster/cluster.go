// Package etcdcluster starts a loopback etcd for tests and the load generator.
// Clients must be given every member URL, including after one member is stopped.
package etcdcluster

import (
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

// Cluster is a 1-node or 3-node etcd on 127.0.0.1.
type Cluster struct {
	mu      sync.Mutex
	members []*member
	dir     string
}

type member struct {
	name      string
	clientURL string
	peerURL   string
	srv       *embed.Etcd
	closed    bool
}

// Start launches n members (1 or 3) under dir. Endpoints stay stable for the
// life of the cluster so callers can configure every member up front.
func Start(dir string, n int) (*Cluster, error) {
	if n != 1 && n != 3 {
		return nil, fmt.Errorf("etcdcluster: n must be 1 or 3, got %d", n)
	}
	c := &Cluster{dir: dir}
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
		c.members = append(c.members, &member{
			name:      fmt.Sprintf("e%d", i),
			clientURL: clients[i],
			peerURL:   peers[i],
		})
	}
	var initial []string
	for i := 0; i < n; i++ {
		initial = append(initial, fmt.Sprintf("%s=%s", c.members[i].name, peers[i]))
	}
	token := fmt.Sprintf("relay-%d", time.Now().UnixNano())
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		i := i
		cfg := embed.NewConfig()
		cfg.Name = c.members[i].name
		cfg.Dir = filepath.Join(dir, cfg.Name)
		cfg.Logger = "zap"
		cfg.LogLevel = "error"
		cfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(zap.NewNop())
		cfg.ListenPeerUrls = mustURLs(peers[i])
		cfg.AdvertisePeerUrls = mustURLs(peers[i])
		cfg.ListenClientUrls = mustURLs(clients[i])
		cfg.AdvertiseClientUrls = mustURLs(clients[i])
		cfg.InitialCluster = join(initial)
		cfg.InitialClusterToken = token
		cfg.ClusterState = embed.ClusterStateFlagNew
		cfg.TickMs = 100
		// 5×tick 是 etcd 允许的最短选举超时，切主时少把 lease 再续上一大段。
		cfg.ElectionMs = 500
		go func() {
			srv, err := embed.StartEtcd(cfg)
			if err != nil {
				errCh <- fmt.Errorf("%s: %w", cfg.Name, err)
				return
			}
			select {
			case <-srv.Server.ReadyNotify():
				c.members[i].srv = srv
				errCh <- nil
			case <-time.After(30 * time.Second):
				srv.Close()
				errCh <- fmt.Errorf("%s: ready timeout", cfg.Name)
			}
		}()
	}
	var first error
	for i := 0; i < n; i++ {
		if err := <-errCh; err != nil && first == nil {
			first = err
		}
	}
	if first != nil {
		c.Close()
		return nil, first
	}
	return c, nil
}

// Endpoints is every member's client URL, including members that were later stopped.
func (c *Cluster) Endpoints() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.members))
	for i, m := range c.members {
		out[i] = m.clientURL
	}
	return out
}

// LeaderURLs is the current leader's client and peer URLs.
func (c *Cluster) LeaderURLs() (clientURL, peerURL string, err error) {
	m, err := c.leaderMember()
	if err != nil {
		return "", "", err
	}
	return m.clientURL, m.peerURL, nil
}

func (c *Cluster) leaderMember() (*member, error) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		var target *member
		for _, m := range c.members {
			if m.closed || m.srv == nil || m.srv.Server == nil {
				continue
			}
			if id := m.srv.Server.ID(); id != 0 && m.srv.Server.Leader() == id {
				target = m
				break
			}
		}
		c.mu.Unlock()
		if target != nil {
			return target, nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil, fmt.Errorf("etcdcluster: no leader")
}

// KillLeader stops the current leader process. The endpoint remains in Endpoints
// so clients configured with the full list must fail over by themselves.
func (c *Cluster) KillLeader() (string, error) {
	target, err := c.leaderMember()
	if err != nil {
		return "", err
	}
	// HardStop 相当于 kill -9：raft 立刻停，不等 HTTP 排空，也不做 leadership transfer。
	// 不调用 embed.Close，避免它在停掉的 leader 上再跑一轮优雅退出把选举拖长。
	if target.srv.Server != nil {
		target.srv.Server.HardStop()
	}
	for _, ln := range target.srv.Clients {
		_ = ln.Close()
	}
	for _, p := range target.srv.Peers {
		_ = p.Close()
	}
	c.mu.Lock()
	target.closed = true
	c.mu.Unlock()
	return target.clientURL, nil
}

// Close stops every member that is still running.
func (c *Cluster) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.members {
		if m.srv != nil && !m.closed {
			m.srv.Close()
			m.closed = true
		}
	}
}

func mustURLs(raw string) []url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		panic(err)
	}
	return []url.URL{*u}
}

func freePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	p := ln.Addr().(*net.TCPAddr).Port
	return p, ln.Close()
}

func join(parts []string) string {
	return strings.Join(parts, ",")
}
