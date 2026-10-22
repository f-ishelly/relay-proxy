package registrar

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/f-ishelly/relay-proxy/internal/etcdcluster"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func TestProbeGost(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				br := bufio.NewReader(c)
				_, _ = br.ReadString('\n')
				_, _ = io.WriteString(c, "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
			}(c)
		}
	}()
	if err := probeGost(ln.Addr().String(), "user", "pass", "127.0.0.1:1", 403, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := probeGost(ln.Addr().String(), "user", "pass", "127.0.0.1:1", 200, time.Second); err == nil {
		t.Fatal("expected status mismatch")
	}
	if err := probeGost("127.0.0.1:1", "user", "pass", "127.0.0.1:1", 403, 200*time.Millisecond); err == nil {
		t.Fatal("expected dial error")
	}
}

func TestNodeIDFileAndRejectBadConfig(t *testing.T) {
	dir := t.TempDir()
	idFile := filepath.Join(dir, "id")
	if err := os.WriteFile(idFile, []byte(" 626220\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.NodeIDFile = idFile
	cfg.Endpoints = []string{"127.0.0.1:1"}
	cfg.IntranetAddr = "10.0.8.26:8080"
	cfg.ProxyPort = 8080
	cfg.ProbeAddr = "127.0.0.1:8080"
	cfg.StateFile = filepath.Join(dir, "state.json")
	cfg.LocalIP = func() string { return "" }
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if r.cfg.NodeID != "626220" {
		t.Fatalf("node id %q", r.cfg.NodeID)
	}
	if r.cfg.AssumedDial != DefaultAssumedDial || r.cfg.AssumedEgressCheck != DefaultAssumedEgressCheck || r.cfg.InspectionInterval != DefaultInspectionInterval {
		t.Fatalf("planning defaults dial=%s check=%s inspect=%s", r.cfg.AssumedDial, r.cfg.AssumedEgressCheck, r.cfg.InspectionInterval)
	}
	cfg.NodeID = "bad/id"
	if _, err := New(cfg); err == nil {
		t.Fatal("expected invalid node id")
	}
}

type ipSrc struct{ v atomic.Value }

func (s *ipSrc) get() string {
	x, _ := s.v.Load().(string)
	return x
}
func (s *ipSrc) set(ip string) { s.v.Store(ip) }

type fakeGost struct {
	ln   net.Listener
	mode atomic.Value
}

func newFakeGost(t *testing.T) *fakeGost {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := &fakeGost{ln: ln}
	g.mode.Store("ok")
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go g.handle(c)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return g
}

func (g *fakeGost) handle(c net.Conn) {
	defer c.Close()
	mode, _ := g.mode.Load().(string)
	if mode == "hang" {
		time.Sleep(3 * time.Second)
		return
	}
	if mode == "down" {
		return
	}
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 512)
	_, _ = c.Read(buf)
	_, _ = io.WriteString(c, "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
}

func (g *fakeGost) set(mode string) { g.mode.Store(mode) }

type memLog struct {
	mu    sync.Mutex
	lines []string
}

func (l *memLog) Event(ev string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, ev)
}

func (l *memLog) has(ev string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, x := range l.lines {
		if x == ev {
			return true
		}
	}
	return false
}

type watchEv struct {
	typ   string
	key   string
	at    time.Time
	lease int64
}

type watchLog struct {
	mu  sync.Mutex
	evs []watchEv
}

func (w *watchLog) add(ev watchEv) {
	w.mu.Lock()
	w.evs = append(w.evs, ev)
	w.mu.Unlock()
}

func (w *watchLog) since(t0 time.Time, key, typ string) []watchEv {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []watchEv
	for _, ev := range w.evs {
		if !ev.at.Before(t0) && (key == "" || ev.key == key) && (typ == "" || ev.typ == typ) {
			out = append(out, ev)
		}
	}
	return out
}

type rig struct {
	t    *testing.T
	cl   *etcdcluster.Cluster
	cli  *clientv3.Client
	gost *fakeGost
	ip   ipSrc
	reg  *Registrar
	log  *memLog
	w    *watchLog
}

func startCluster(t *testing.T, n int) *etcdcluster.Cluster {
	t.Helper()
	cl, err := etcdcluster.Start(t.TempDir(), n)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func dial(t *testing.T, eps []string) *clientv3.Client {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: eps, DialTimeout: 2 * time.Second, Logger: zap.NewNop()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

func (r *rig) kv(key string) (lease int64, create int64, val Value, ok bool) {
	r.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
		resp, err := r.cli.Get(ctx, key)
		cancel()
		if err != nil {
			last = err
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if len(resp.Kvs) == 0 {
			return 0, 0, Value{}, false
		}
		item := resp.Kvs[0]
		if err := json.Unmarshal(item.Value, &val); err != nil {
			r.t.Fatal(err)
		}
		return item.Lease, item.CreateRevision, val, true
	}
	r.t.Fatalf("etcd get %s: %v", key, last)
	return 0, 0, Value{}, false
}

func (r *rig) waitKey(want bool, d time.Duration) (int64, int64, Value) {
	r.t.Helper()
	var lease, create int64
	var val Value
	var ok bool
	waitUntil(r.t, d, func() bool {
		lease, create, val, ok = r.kv(r.reg.key)
		return ok == want
	})
	return lease, create, val
}

func waitUntil(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", d)
}

func watchPrefix(t *testing.T, cli *clientv3.Client, prefix string) *watchLog {
	t.Helper()
	w := &watchLog{}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ready := make(chan struct{})
	go func() {
		ch := cli.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithCreatedNotify())
		first := true
		for wr := range ch {
			if first {
				first = false
				close(ready)
			}
			for _, ev := range wr.Events {
				typ := "PUT"
				if ev.Type == clientv3.EventTypeDelete {
					typ = "DELETE"
				}
				w.add(watchEv{typ: typ, key: string(ev.Kv.Key), at: time.Now(), lease: ev.Kv.Lease})
			}
		}
	}()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("watch not created")
	}
	return w
}

func newRig(t *testing.T, nodes int, nodeID string, tune func(*Config)) *rig {
	t.Helper()
	g := &rig{t: t, cl: startCluster(t, nodes), gost: newFakeGost(t), log: &memLog{}}
	g.ip.set("100.64.0.1")
	g.cli = dial(t, g.cl.Endpoints())
	g.w = watchPrefix(t, g.cli, "/relay/l2/")
	cfg := DefaultConfig()
	cfg.Endpoints = g.cl.Endpoints()
	cfg.NodeID = nodeID
	cfg.IntranetAddr = "10.0.8.26:8080"
	cfg.ProxyPort = 8080
	cfg.ControlPort = 17001
	cfg.AZ = "az01"
	cfg.Version = "gost-3.3.1"
	cfg.ProbeAddr = g.gost.ln.Addr().String()
	cfg.ProbeUser = "appuser@" + nodeID
	cfg.ProbePassword = "s3cret"
	cfg.StateFile = filepath.Join(t.TempDir(), "state.json")
	cfg.LocalIP = g.ip.get
	cfg.Logger = g.log
	cfg.ProbeInterval = 40 * time.Millisecond
	cfg.ProbeTimeout = 200 * time.Millisecond
	cfg.ProbeRiseThreshold = 1
	cfg.ProbeFailThreshold = 2
	cfg.RenewInterval = time.Second
	if tune != nil {
		tune(&cfg)
	}
	reg, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	g.reg = reg
	t.Cleanup(func() { _ = reg.Stop(context.Background()) })
	if err := reg.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, func() bool { return reg.Status().ProbeOK })
	return g
}

func (r *rig) ready(ip string) {
	r.t.Helper()
	r.ip.set(ip)
	if err := r.reg.Ready(context.Background(), ip); err != nil {
		r.t.Fatal(err)
	}
}

func TestAcceptance1_PlannedRedial(t *testing.T) {
	g := newRig(t, 1, "626220", nil)
	g.ready("100.64.0.1")
	lease, _, val := g.waitKey(true, 2*time.Second)
	if val.Addr != "10.0.8.26:8080" || val.EgressIP != "100.64.0.1" || val.NodeID != "626220" || val.Capability != "http-connect" {
		t.Fatalf("value %+v", val)
	}
	for i := 0; i < 5; i++ {
		t0 := time.Now()
		if err := g.reg.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, _, _, ok := g.kv(g.reg.key); ok {
			t.Fatal("key still present after Drain, before hangup")
		}
		hangup := time.Now()
		dels := g.w.since(t0, g.reg.key, "DELETE")
		if len(dels) == 0 || dels[0].at.After(hangup.Add(time.Second)) {
			t.Fatalf("L1 delete not observed before hangup+1s: %+v", dels)
		}
		next := "100.64.1." + itoa(i+2)
		g.ip.set(next)
		tr := time.Now()
		if err := g.reg.Ready(context.Background(), next); err != nil {
			t.Fatal(err)
		}
		puts := g.w.since(tr, g.reg.key, "PUT")
		waitUntil(t, time.Second, func() bool { return len(g.w.since(tr, g.reg.key, "PUT")) > 0 })
		puts = g.w.since(tr, g.reg.key, "PUT")
		if puts[0].at.Sub(tr) > time.Second {
			t.Fatalf("PUT latency %s", puts[0].at.Sub(tr))
		}
		gotLease, _, got := g.waitKey(true, time.Second)
		if gotLease != lease {
			t.Fatalf("lease changed %d -> %d", lease, gotLease)
		}
		if got.Addr != val.Addr || got.EgressIP != next {
			t.Fatalf("addr/egress = %s %s", got.Addr, got.EgressIP)
		}
		t.Logf("redial %d drain_to_absent=%s ready_to_put=%s", i, hangup.Sub(t0), puts[0].at.Sub(tr))
	}
}

func TestAcceptance2_EgressFailAndRestart(t *testing.T) {
	g := newRig(t, 1, "626220", nil)
	g.ready("100.64.0.1")
	g.waitKey(true, 2*time.Second)
	if err := g.reg.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if _, _, _, ok := g.kv(g.reg.key); ok {
		t.Fatal("registered during failed egress check")
	}
	if err := g.reg.Ready(context.Background(), "100.64.9.9"); !errors.Is(err, ErrEgressMismatch) {
		t.Fatalf("err=%v", err)
	}
	if _, _, _, ok := g.kv(g.reg.key); ok {
		t.Fatal("mismatch Ready wrote a key")
	}

	// 拨号中崩溃：drained 已落盘。新进程在 Ready 之前不得注册。
	g.reg.abandon()
	cfg := g.reg.cfg
	again, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = again.Stop(context.Background()) })
	if err := again.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond)
	if _, _, _, ok := g.kv(again.key); ok {
		t.Fatal("restart registered before egress check")
	}
	if !again.Status().Drained {
		t.Fatal("drained flag not restored")
	}
	g.ip.set("100.64.0.8")
	if err := again.Ready(context.Background(), "100.64.0.8"); err != nil {
		t.Fatal(err)
	}
	g.reg = again
	if _, _, val, ok := g.kv(again.key); !ok || val.EgressIP != "100.64.0.8" {
		t.Fatalf("ready after restart: ok=%v val=%+v", ok, val)
	}
}

func TestDrainedRestartRevokesLeftoverKey(t *testing.T) {
	g := newRig(t, 1, "626220", nil)
	g.ready("100.64.0.1")
	g.waitKey(true, 2*time.Second)
	g.reg.abandon()
	// 模拟 Drain 已经把 drained 落盘，但进程在 Delete 前消失：key 还在旧 lease 上。
	st, ok := loadState(g.reg.cfg.StateFile, "626220")
	if !ok || st.Lease == 0 {
		t.Fatalf("state %+v ok=%v", st, ok)
	}
	st.Drained = true
	st.IP = ""
	b, _ := json.Marshal(st)
	if err := os.WriteFile(g.reg.cfg.StateFile, b, 0o600); err != nil {
		t.Fatal(err)
	}
	again, err := New(g.reg.cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = again.Stop(context.Background()) })
	t0 := time.Now()
	if err := again.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 2*time.Second, func() bool {
		_, _, _, ok := g.kv(again.key)
		return !ok
	})
	t.Logf("leftover key revoked in %s", time.Since(t0))
	time.Sleep(300 * time.Millisecond)
	if _, _, _, ok := g.kv(again.key); ok {
		t.Fatal("registered before Ready")
	}
}

func TestTakeoverNoDelete(t *testing.T) {
	g := newRig(t, 1, "626220", nil)
	g.ready("100.64.0.1")
	oldLease, oldCreate, _ := g.waitKey(true, 2*time.Second)
	g.reg.abandon()
	again, err := New(g.reg.cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = again.Stop(context.Background()) })
	t0 := time.Now()
	if err := again.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, func() bool {
		lease, _, _, ok := g.kv(again.key)
		return ok && lease == int64(again.Status().LeaseID) && again.Status().EgressIP == "100.64.0.1"
	})
	// 接管期间 key 不能消失。抽样覆盖探测升温的窗口。
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, _, _, ok := g.kv(again.key); !ok {
			t.Fatal("key disappeared during takeover")
		}
		time.Sleep(20 * time.Millisecond)
	}
	dels := g.w.since(t0, again.key, "DELETE")
	if len(dels) != 0 {
		t.Fatalf("takeover emitted DELETE: %+v", dels)
	}
	lease, create, val, ok := g.kv(again.key)
	if !ok || val.Addr != "10.0.8.26:8080" {
		t.Fatalf("after takeover ok=%v val=%+v", ok, val)
	}
	if create != oldCreate {
		t.Fatalf("create revision changed %d -> %d (key was deleted)", oldCreate, create)
	}
	if lease == oldLease {
		t.Fatal("expected a new lease attached by atomic put")
	}
	t.Logf("takeover old_lease=%d new_lease=%d", oldLease, lease)
}

func TestAcceptance3_UnannouncedIPChange(t *testing.T) {
	g := newRig(t, 1, "626220", func(cfg *Config) {
		cfg.IPGuardInterval = DefaultIPGuardInterval
	})
	g.ready("100.64.0.1")
	g.waitKey(true, 2*time.Second)

	t0 := time.Now()
	g.ip.set("100.64.2.2")
	waitUntil(t, time.Second, func() bool {
		_, _, _, ok := g.kv(g.reg.key)
		return !ok
	})
	elapsed := time.Since(t0)
	if elapsed > time.Second {
		t.Fatalf("ip change delete took %s", elapsed)
	}
	if !g.reg.Status().Drained {
		t.Fatal("guard did not drain")
	}
	t.Logf("ip change -> delete %s", elapsed)

	g.ip.set("100.64.0.1")
	if err := g.reg.Ready(context.Background(), "100.64.0.1"); err != nil {
		t.Fatal(err)
	}
	g.waitKey(true, 2*time.Second)
	t1 := time.Now()
	g.ip.set("")
	waitUntil(t, time.Second, func() bool {
		_, _, _, ok := g.kv(g.reg.key)
		return !ok
	})
	if d := time.Since(t1); d > time.Second {
		t.Fatalf("link down delete took %s", d)
	} else {
		t.Logf("link down -> delete %s", d)
	}
}

func TestAcceptance4_ProbeAndNetCheck(t *testing.T) {
	g := newRig(t, 1, "626220", func(cfg *Config) {
		cfg.ProbeInterval = DefaultProbeInterval
		cfg.ProbeTimeout = DefaultProbeTimeout
		cfg.ProbeFailThreshold = DefaultProbeFailThreshold
		cfg.ProbeRiseThreshold = DefaultProbeRiseThreshold
	})
	// 升温需要连续两次成功，间隔是 1s。
	waitUntil(t, 4*time.Second, func() bool { return g.reg.Status().ProbeOK })
	g.ready("100.64.0.1")
	lease, _, _ := g.waitKey(true, 2*time.Second)

	t0 := time.Now()
	g.gost.set("hang")
	waitUntil(t, 5*time.Second, func() bool {
		_, _, _, ok := g.kv(g.reg.key)
		return !ok
	})
	if d := time.Since(t0); d > 5*time.Second {
		t.Fatalf("probe fail delete took %s", d)
	} else {
		t.Logf("gost hang -> delete %s", d)
	}

	t1 := time.Now()
	g.gost.set("ok")
	waitUntil(t, 3*time.Second, func() bool {
		_, _, _, ok := g.kv(g.reg.key)
		return ok
	})
	if d := time.Since(t1); d > 3*time.Second {
		t.Fatalf("probe recovery took %s", d)
	} else {
		t.Logf("gost recover -> put %s", d)
	}
	if got, _, _, ok := g.kv(g.reg.key); !ok || got != lease {
		t.Fatalf("lease after probe recovery %d ok=%v want %d", got, ok, lease)
	}

	t2 := time.Now()
	if err := g.reg.Report(context.Background(), false, "100.64.0.1"); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, time.Second, func() bool {
		_, _, _, ok := g.kv(g.reg.key)
		return !ok
	})
	if d := time.Since(t2); d > time.Second {
		t.Fatalf("net check delete took %s", d)
	} else {
		t.Logf("net_ok=false -> delete %s", d)
	}
	if err := g.reg.Report(context.Background(), true, "100.64.0.1"); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, time.Second, func() bool {
		_, _, _, ok := g.kv(g.reg.key)
		return ok
	})
	if got, _, _, _ := g.kv(g.reg.key); got != lease {
		t.Fatalf("lease after net recovery %d want %d", got, lease)
	}
}

func TestAcceptance5_LeaderFailoverDrain(t *testing.T) {
	g := newRig(t, 3, "626220", nil)
	g.ready("100.64.0.1")
	steadyLease, steadyCreate, _ := g.waitKey(true, 2*time.Second)

	other := newRigOn(t, g, "626221")
	other.ready("100.64.0.1")
	other.waitKey(true, 2*time.Second)

	if _, err := g.cl.KillLeader(); err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	if err := other.reg.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(t0)
	if elapsed > 3*time.Second {
		t.Fatalf("Drain during leader loss took %s", elapsed)
	}
	if _, _, _, ok := g.kv(other.reg.key); ok {
		t.Fatal("drained key still present")
	}
	// 仍在续租的节点：leader 切换本身不能产生 DELETE。
	waitUntil(t, 3*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := g.cli.Get(ctx, g.reg.key)
		return err == nil
	})
	lease, create, _, ok := g.kv(g.reg.key)
	if !ok {
		t.Fatal("steady key deleted during leader change")
	}
	if lease != steadyLease || create != steadyCreate {
		t.Fatalf("steady key changed lease %d->%d create %d->%d", steadyLease, lease, steadyCreate, create)
	}
	if dels := g.w.since(t0, g.reg.key, "DELETE"); len(dels) != 0 {
		t.Fatalf("steady DELETE events: %+v", dels)
	}
	t.Logf("Drain during failover %s", elapsed)
}

func TestAcceptance6_CrashTTLAndLeader(t *testing.T) {
	t.Run("ttl", func(t *testing.T) {
		g := newRig(t, 1, "626220", nil)
		g.ready("100.64.0.1")
		g.waitKey(true, 2*time.Second)
		t0 := time.Now()
		g.reg.abandon()
		waitUntil(t, 12*time.Second, func() bool {
			_, _, _, ok := g.kv(g.reg.key)
			return !ok
		})
		d := time.Since(t0)
		t.Logf("process kill -> delete %s", d)
		if d > 12*time.Second {
			t.Fatalf("key outlived ttl window: %s", d)
		}
	})
	t.Run("leader", func(t *testing.T) {
		g := newRig(t, 3, "626220", nil)
		g.ready("100.64.0.1")
		lease, _, _ := g.waitKey(true, 2*time.Second)
		t0 := time.Now()
		g.reg.abandon()
		time.Sleep(3 * time.Second)
		if _, _, _, ok := g.kv(g.reg.key); !ok {
			t.Fatal("key gone before leader change; ttl case is the other subtest")
		}
		if _, err := g.cl.KillLeader(); err != nil {
			t.Fatal(err)
		}
		if ttl, err := leaseTTL(g.cli, lease); err == nil {
			t.Logf("ttl right after leader stop: %ds", ttl)
		}
		remain := 16*time.Second - time.Since(t0)
		waitUntil(t, remain, func() bool {
			_, _, _, ok := g.kv(g.reg.key)
			return !ok
		})
		d := time.Since(t0)
		t.Logf("kill + leader change -> delete %s", d)
		if d > 16*time.Second {
			t.Fatalf("key visible for %s", d)
		}
	})
}

func TestAcceptance8_NodeConflict(t *testing.T) {
	g := newRig(t, 1, "626220", nil)
	g.ready("100.64.0.1")
	lease, create, before := g.waitKey(true, 2*time.Second)

	alerts := make(chan Alert, 4)
	cfg := g.reg.cfg
	cfg.StateFile = filepath.Join(t.TempDir(), "other-state.json")
	cfg.OnAlert = func(a Alert) { alerts <- a }
	other, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Stop(context.Background()) })
	if err := other.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 2*time.Second, func() bool { return other.Status().ProbeOK })
	err = other.Ready(context.Background(), "100.64.0.1")
	if !errors.Is(err, ErrNodeConflict) {
		t.Fatalf("err=%v", err)
	}
	select {
	case a := <-alerts:
		if a.Kind != AlertNodeConflict || a.Key != g.reg.key || a.OtherLease != lease {
			t.Fatalf("alert %+v", a)
		}
		t.Logf("conflict alert: %s", a.Message)
	case <-time.After(2 * time.Second):
		t.Fatal("no conflict alert")
	}
	gotLease, gotCreate, after, ok := g.kv(g.reg.key)
	if !ok || gotLease != lease || gotCreate != create || after.EgressIP != before.EgressIP || after.Addr != before.Addr {
		t.Fatalf("owner key changed lease=%d create=%d val=%+v", gotLease, gotCreate, after)
	}

	t.Run("dead lease can be replaced", func(t *testing.T) {
		g.reg.abandon()
		waitUntil(t, 12*time.Second, func() bool {
			_, _, _, ok := g.kv(g.reg.key)
			return !ok
		})
		// other 之前冲突过，退避 5s。等退避结束再 Ready。
		time.Sleep(5 * time.Second)
		if err := other.Ready(context.Background(), "100.64.0.1"); err != nil {
			t.Fatal(err)
		}
		if _, _, val, ok := g.kv(g.reg.key); !ok || val.NodeID != "626220" {
			t.Fatalf("dead lease not taken over ok=%v val=%+v", ok, val)
		}
	})
}

func TestRenewKeepsKey(t *testing.T) {
	g := newRig(t, 1, "626220", func(cfg *Config) {
		cfg.TTL = 2 * time.Second
		cfg.RenewInterval = 400 * time.Millisecond
	})
	g.ready("100.64.0.1")
	lease, create, _ := g.waitKey(true, 2*time.Second)
	time.Sleep(5 * time.Second)
	got, gotCreate, _, ok := g.kv(g.reg.key)
	if !ok || got != lease || gotCreate != create {
		t.Fatalf("renew failed ok=%v lease %d->%d create %d->%d", ok, lease, got, create, gotCreate)
	}
}

func TestOwnLeasePutKeepsCreateRevision(t *testing.T) {
	g := newRig(t, 1, "626220", nil)
	g.ready("100.64.0.1")
	lease, create, _ := g.waitKey(true, 2*time.Second)
	g.reg.mu.Lock()
	g.reg.regIP = ""
	g.reg.mu.Unlock()
	if err := g.reg.Ready(context.Background(), "100.64.0.1"); err != nil {
		t.Fatal(err)
	}
	got, gotCreate, _, ok := g.kv(g.reg.key)
	if !ok || got != lease || gotCreate != create {
		t.Fatalf("overwrite changed identity lease %d->%d create %d->%d", lease, got, create, gotCreate)
	}
}

func TestIgnoreAdminNetCheck(t *testing.T) {
	g := newRig(t, 1, "626220", func(cfg *Config) { cfg.IgnoreAdminNetCheck = true })
	g.ready("100.64.0.1")
	g.waitKey(true, 2*time.Second)
	if err := g.reg.Report(context.Background(), false, "100.64.0.1"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if _, _, _, ok := g.kv(g.reg.key); !ok {
		t.Fatal("ignored net check still deleted the key")
	}
}

func TestStopRevokesImmediately(t *testing.T) {
	g := newRig(t, 1, "626220", nil)
	g.ready("100.64.0.1")
	g.waitKey(true, 2*time.Second)
	t0 := time.Now()
	if err := g.reg.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, time.Second, func() bool {
		_, _, _, ok := g.kv(g.reg.key)
		return !ok
	})
	t.Logf("Stop -> delete %s", time.Since(t0))
}

func newRigOn(t *testing.T, base *rig, nodeID string) *rig {
	t.Helper()
	g := &rig{t: t, cl: base.cl, cli: base.cli, gost: base.gost, w: base.w, log: &memLog{}}
	g.ip.set("100.64.0.1")
	cfg := DefaultConfig()
	cfg.Endpoints = base.cl.Endpoints()
	cfg.NodeID = nodeID
	cfg.IntranetAddr = "10.0.8.27:8080"
	cfg.ProxyPort = 8080
	cfg.ControlPort = 17002
	cfg.AZ = "az01"
	cfg.Version = "gost-3.3.1"
	cfg.ProbeAddr = base.gost.ln.Addr().String()
	cfg.ProbeUser = "appuser@" + nodeID
	cfg.ProbePassword = "s3cret"
	cfg.StateFile = filepath.Join(t.TempDir(), "state.json")
	cfg.LocalIP = g.ip.get
	cfg.Logger = g.log
	cfg.ProbeInterval = 40 * time.Millisecond
	cfg.ProbeTimeout = 200 * time.Millisecond
	cfg.ProbeRiseThreshold = 1
	cfg.ProbeFailThreshold = 2
	cfg.RenewInterval = time.Second
	reg, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	g.reg = reg
	t.Cleanup(func() { _ = reg.Stop(context.Background()) })
	if err := reg.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, func() bool { return reg.Status().ProbeOK })
	return g
}

func leaseTTL(cli *clientv3.Client, id int64) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := cli.TimeToLive(ctx, clientv3.LeaseID(id))
	if err != nil {
		return 0, err
	}
	return resp.TTL, nil
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [8]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
