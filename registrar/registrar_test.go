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
	if r.cfg.AssumedDial+r.cfg.AssumedEgressCheck != 5*time.Second || r.cfg.AssumedRedialFailureRate != DefaultAssumedRedialFailureRate {
		t.Fatalf("redial planning dial=%s check=%s rate=%v", r.cfg.AssumedDial, r.cfg.AssumedEgressCheck, r.cfg.AssumedRedialFailureRate)
	}
	if r.cfg.RedialJitter != DefaultRedialJitter || r.cfg.DrainAttempt != DefaultDrainAttempt {
		t.Fatalf("jitter=%s attempt=%s", r.cfg.RedialJitter, r.cfg.DrainAttempt)
	}
	if r.cfg.InspectionInterval != time.Minute || r.cfg.ResourceFaultAction != ResourceFaultDeregister {
		t.Fatalf("inspect=%s resource=%s", r.cfg.InspectionInterval, r.cfg.ResourceFaultAction)
	}
	cfg.NodeID = "bad/id"
	if _, err := New(cfg); err == nil {
		t.Fatal("expected invalid node id")
	}
	cfg.NodeID = "626220"
	cfg.ResourceFaultAction = "lower-weight"
	if _, err := New(cfg); err == nil {
		t.Fatal("expected invalid resource action")
	}
}

func TestNodeIDEnvOverridesFile(t *testing.T) {
	dir := t.TempDir()
	idFile := filepath.Join(dir, "id")
	if err := os.WriteFile(idFile, []byte("from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(NodeIDEnv, "from-env")
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
	if r.cfg.NodeID != "from-env" {
		t.Fatalf("node id %q", r.cfg.NodeID)
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
	if err := r.reg.Ready(context.Background(), ip, ""); err != nil {
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
		if err := g.reg.Ready(context.Background(), next, ""); err != nil {
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
	if err := g.reg.Ready(context.Background(), "100.64.9.9", ""); !errors.Is(err, ErrEgressMismatch) {
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
	if err := again.Ready(context.Background(), "100.64.0.8", ""); err != nil {
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
	st, res := loadState(g.reg.cfg.StateFile, "626220")
	if res != stateOK || st.Lease == 0 {
		t.Fatalf("state %+v res=%d", st, res)
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
	if err := g.reg.Ready(context.Background(), "100.64.0.1", ""); err != nil {
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
	if err := g.reg.Report(context.Background(), Report{NetFault: true, EgressIP: "100.64.0.1"}); err != nil {
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
	if err := g.reg.Report(context.Background(), Report{EgressIP: "100.64.0.1"}); err != nil {
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
	err = other.Ready(context.Background(), "100.64.0.1", "")
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
		if err := other.Ready(context.Background(), "100.64.0.1", ""); err != nil {
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
	if err := g.reg.Ready(context.Background(), "100.64.0.1", ""); err != nil {
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
	if err := g.reg.Report(context.Background(), Report{NetFault: true, EgressIP: "100.64.0.1"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if _, _, _, ok := g.kv(g.reg.key); !ok {
		t.Fatal("ignored net check still deleted the key")
	}
}

func TestRedialSameEgressNotRegistered(t *testing.T) {
	g := newRig(t, 1, "626220", nil)
	g.ready("100.64.0.1")
	g.waitKey(true, 2*time.Second)
	if err := g.reg.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := g.reg.Ready(context.Background(), "100.64.0.1", ""); !errors.Is(err, ErrEgressUnchanged) {
		t.Fatalf("err=%v", err)
	}
	if _, _, _, ok := g.kv(g.reg.key); ok {
		t.Fatal("unchanged egress registered")
	}
	if !g.reg.Status().Drained {
		t.Fatal("failed egress check cleared drained")
	}

	g.reg.abandon()
	again, err := New(g.reg.cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = again.Stop(context.Background()) })
	if err := again.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond)
	if _, _, _, ok := g.kv(again.key); ok {
		t.Fatal("restart registered the old egress")
	}
	if err := again.Ready(context.Background(), "100.64.0.1", ""); !errors.Is(err, ErrEgressUnchanged) {
		t.Fatalf("after restart err=%v", err)
	}
	g.ip.set("100.64.0.2")
	if err := again.Ready(context.Background(), "100.64.0.2", ""); err != nil {
		t.Fatal(err)
	}
	g.reg = again
	if _, _, val, ok := g.kv(again.key); !ok || val.EgressIP != "100.64.0.2" {
		t.Fatalf("new egress ok=%v val=%+v", ok, val)
	}
}

func TestResourceFaultDeregistersThenRestores(t *testing.T) {
	g := newRig(t, 1, "626220", nil)
	g.ready("100.64.0.1")
	lease, _, _ := g.waitKey(true, 2*time.Second)
	if err := g.reg.Report(context.Background(), Report{CPUFault: true, EgressIP: "100.64.0.1"}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, time.Second, func() bool {
		_, _, _, ok := g.kv(g.reg.key)
		return !ok
	})
	if g.reg.Status().ResourcesOK {
		t.Fatal("resources still ok")
	}
	if err := g.reg.Report(context.Background(), Report{EgressIP: "100.64.0.1"}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, time.Second, func() bool {
		_, _, _, ok := g.kv(g.reg.key)
		return ok
	})
	if got, _, _, _ := g.kv(g.reg.key); got != lease {
		t.Fatalf("lease after resource recovery %d want %d", got, lease)
	}
}

func TestResourceFaultIgnoreKeepsKey(t *testing.T) {
	g := newRig(t, 1, "626220", func(cfg *Config) { cfg.ResourceFaultAction = ResourceFaultIgnore })
	g.ready("100.64.0.1")
	g.waitKey(true, 2*time.Second)
	if err := g.reg.Report(context.Background(), Report{CPUFault: true, EgressIP: "100.64.0.1"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if _, _, _, ok := g.kv(g.reg.key); !ok {
		t.Fatal("ignored resource fault deleted the key")
	}
	if g.reg.Status().ResourcesOK {
		t.Fatal("status hid the resource fault")
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

func TestBlankNodeIDEnvDoesNotReadFile(t *testing.T) {
	dir := t.TempDir()
	idFile := filepath.Join(dir, "id")
	if err := os.WriteFile(idFile, []byte("from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(NodeIDEnv, " \n")
	cfg := DefaultConfig()
	cfg.NodeIDFile = idFile
	cfg.Endpoints = []string{"127.0.0.1:1"}
	cfg.IntranetAddr = "10.0.8.26:8080"
	cfg.ProxyPort = 8080
	cfg.ProbeAddr = "127.0.0.1:8080"
	cfg.StateFile = filepath.Join(dir, "state.json")
	cfg.LocalIP = func() string { return "100.64.0.1" }
	if _, err := New(cfg); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("err=%v", err)
	}
	cfg.NodeID = "   "
	t.Setenv(NodeIDEnv, "626220")
	if _, err := New(cfg); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("blank config node id err=%v", err)
	}
}

func TestZeroReportStaysRegistered(t *testing.T) {
	g := newRig(t, 1, "626220", nil)
	g.ready("100.64.0.1")
	g.waitKey(true, 2*time.Second)
	if err := g.reg.Report(context.Background(), Report{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if _, _, _, ok := g.kv(g.reg.key); !ok {
		t.Fatal("zero Report removed the key")
	}
	if !g.reg.Status().NetOK || !g.reg.Status().ResourcesOK {
		t.Fatalf("status %+v", g.reg.Status())
	}
}

func TestDrainBeforeRegisterNeedsPrevEgress(t *testing.T) {
	g := newRig(t, 1, "626220", nil)
	if err := g.reg.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := g.reg.Ready(context.Background(), "100.64.0.1", ""); !errors.Is(err, ErrEgressUnconfirmed) {
		t.Fatalf("err=%v", err)
	}
	if _, _, _, ok := g.kv(g.reg.key); ok {
		t.Fatal("unconfirmed Ready wrote a key")
	}
	if err := g.reg.Ready(context.Background(), "100.64.0.1", "100.64.0.1"); !errors.Is(err, ErrEgressUnchanged) {
		t.Fatalf("err=%v", err)
	}
	if err := g.reg.Ready(context.Background(), "100.64.0.1", "100.64.0.9"); err != nil {
		t.Fatal(err)
	}
	if _, _, val, ok := g.kv(g.reg.key); !ok || val.EgressIP != "100.64.0.1" {
		t.Fatalf("ok=%v val=%+v", ok, val)
	}
}

func TestCorruptStateRefusesUntilPrevConfirmed(t *testing.T) {
	g := newRig(t, 1, "626220", nil)
	if err := g.reg.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(g.reg.cfg.StateFile, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	again, err := New(g.reg.cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = again.Stop(context.Background()) })
	if err := again.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, func() bool { return again.Status().ProbeOK })
	if err := again.Report(context.Background(), Report{EgressIP: "100.64.0.1"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if _, _, _, ok := g.kv(again.key); ok {
		t.Fatal("Report registered over a corrupt state file")
	}
	if err := again.Ready(context.Background(), "100.64.0.1", ""); !errors.Is(err, ErrEgressUnconfirmed) {
		t.Fatalf("corrupt state err=%v", err)
	}
	if _, _, _, ok := g.kv(again.key); ok {
		t.Fatal("corrupt state registered without a previous egress")
	}
	if err := again.Ready(context.Background(), "100.64.0.1", "100.64.0.1"); !errors.Is(err, ErrEgressUnchanged) {
		t.Fatalf("same prev err=%v", err)
	}
	g.ip.set("100.64.0.2")
	if err := again.Ready(context.Background(), "100.64.0.2", "100.64.0.1"); err != nil {
		t.Fatal(err)
	}
	if _, _, val, ok := g.kv(again.key); !ok || val.EgressIP != "100.64.0.2" {
		t.Fatalf("ok=%v val=%+v", ok, val)
	}
}

func TestMissingStateFileLooksLikeFirstBoot(t *testing.T) {
	g := newRig(t, 1, "626220", nil)
	if err := g.reg.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(g.reg.cfg.StateFile); err != nil {
		t.Fatal(err)
	}
	again, err := New(g.reg.cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = again.Stop(context.Background()) })
	if err := again.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, func() bool { return again.Status().ProbeOK })
	if !again.Status().Drained {
		t.Fatal("missing state file was not treated as drained")
	}
	if err := again.Report(context.Background(), Report{EgressIP: "100.64.0.1"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if _, _, _, ok := g.kv(again.key); ok {
		t.Fatal("Report registered when the state file was missing")
	}
	if err := again.Ready(context.Background(), "100.64.0.1", "100.64.0.1"); !errors.Is(err, ErrEgressUnchanged) {
		t.Fatalf("caller prev err=%v", err)
	}
	if _, _, _, ok := g.kv(again.key); ok {
		t.Fatal("unchanged caller prev registered")
	}
	if err := again.Ready(context.Background(), "100.64.0.1", ""); err != nil {
		t.Fatal(err)
	}
	if _, _, _, ok := g.kv(again.key); !ok {
		t.Fatal("first boot with a missing state file did not register")
	}
}

func TestClientDeadlineReclaimsOwnLease(t *testing.T) {
	g := newRig(t, 1, "626220", nil)
	g.ready("100.64.0.1")
	lease, create, _ := g.waitKey(true, 2*time.Second)
	t0 := time.Now()
	// 服务端租约还活着、只是客户端截止时间到了。真正的 Revoke 会让 etcd 删掉 key，测不到这条「不能 DELETE」。
	g.reg.opMu.Lock()
	g.reg.mu.Lock()
	g.reg.loseLeaseLocked(errors.New("client deadline"))
	g.reg.mu.Unlock()
	g.reg.opMu.Unlock()
	if _, _, _, ok := g.kv(g.reg.key); !ok {
		t.Fatal("key deleted when the client declared the lease lost")
	}
	if err := g.reg.Ready(context.Background(), "100.64.0.1", ""); err != nil {
		t.Fatal(err)
	}
	got, gotCreate, val, ok := g.kv(g.reg.key)
	if !ok || gotCreate != create || val.InstanceID == "" {
		t.Fatalf("reclaim ok=%v lease %d->%d create %d->%d val=%+v", ok, lease, got, create, gotCreate, val)
	}
	if got == lease {
		t.Fatalf("reclaim kept the old lease %d", lease)
	}
	if dels := g.w.since(t0, g.reg.key, "DELETE"); len(dels) != 0 {
		t.Fatalf("reclaim deleted the key: %+v", dels)
	}
	ttl, err := leaseTTL(g.cli, lease)
	if err != nil && !leaseMissing(err) {
		t.Fatalf("old lease ttl: %v", err)
	}
	if err == nil && ttl > 0 {
		t.Fatalf("old lease %d still live ttl=%d", lease, ttl)
	}
}

func TestCloneDoesNotRevokeOriginal(t *testing.T) {
	startClone := func(t *testing.T, g *rig, raw []byte, tweak func(*Config, *diskState)) *Registrar {
		t.Helper()
		cfg := g.reg.cfg
		cfg.StateFile = filepath.Join(t.TempDir(), "clone.json")
		cfg.OnAlert = func(Alert) {}
		var st diskState
		if err := json.Unmarshal(raw, &st); err != nil {
			t.Fatal(err)
		}
		if tweak != nil {
			tweak(&cfg, &st)
		}
		b, err := json.Marshal(st)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(cfg.StateFile, b, 0o600); err != nil {
			t.Fatal(err)
		}
		clone, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = clone.Stop(context.Background()) })
		if err := clone.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		return clone
	}
	assertOriginal := func(t *testing.T, g *rig, lease, create int64) {
		t.Helper()
		time.Sleep(400 * time.Millisecond)
		got, gotCreate, _, ok := g.kv(g.reg.key)
		if !ok || got != lease || gotCreate != create {
			t.Fatalf("clone revoked the original ok=%v lease %d->%d create %d->%d", ok, lease, got, create, gotCreate)
		}
	}

	t.Run("different machine-id", func(t *testing.T) {
		g := newRig(t, 1, "626220", func(cfg *Config) { cfg.MachineID = "host-a" })
		g.ready("100.64.0.1")
		lease, create, _ := g.waitKey(true, 2*time.Second)
		raw, err := os.ReadFile(g.reg.cfg.StateFile)
		if err != nil {
			t.Fatal(err)
		}
		clone := startClone(t, g, raw, func(cfg *Config, st *diskState) {
			cfg.MachineID = "host-b"
		})
		assertOriginal(t, g, lease, create)
		waitUntil(t, 3*time.Second, func() bool { return clone.Status().ProbeOK })
		if err := clone.Ready(context.Background(), "100.64.0.1", ""); !errors.Is(err, ErrNodeConflict) {
			t.Fatalf("clone ready err=%v", err)
		}
		assertOriginal(t, g, lease, create)
	})

	t.Run("cloned machine-id different local ip", func(t *testing.T) {
		g := newRig(t, 1, "626220", func(cfg *Config) { cfg.MachineID = "host-a" })
		g.ready("100.64.0.1")
		lease, create, _ := g.waitKey(true, 2*time.Second)
		raw, err := os.ReadFile(g.reg.cfg.StateFile)
		if err != nil {
			t.Fatal(err)
		}
		clone := startClone(t, g, raw, func(cfg *Config, st *diskState) {
			cfg.MachineID = "host-a"
			cfg.LocalIP = func() string { return "100.64.9.9" }
			cfg.IntranetAddr = "10.9.9.9:8080"
			cfg.ProxyPort = 8080
		})
		assertOriginal(t, g, lease, create)
		waitUntil(t, 3*time.Second, func() bool { return clone.Status().ProbeOK })
		if err := clone.Ready(context.Background(), "100.64.9.9", ""); !errors.Is(err, ErrNodeConflict) {
			t.Fatalf("clone ready err=%v", err)
		}
		assertOriginal(t, g, lease, create)
	})

	t.Run("empty machine-id", func(t *testing.T) {
		g := newRig(t, 1, "626220", func(cfg *Config) { cfg.MachineID = "host-a" })
		g.ready("100.64.0.1")
		lease, create, _ := g.waitKey(true, 2*time.Second)
		raw, err := os.ReadFile(g.reg.cfg.StateFile)
		if err != nil {
			t.Fatal(err)
		}
		clone := startClone(t, g, raw, func(cfg *Config, st *diskState) {
			cfg.MachineID = "host-a"
			st.MachineID = ""
			cfg.LocalIP = func() string { return "100.64.9.9" }
		})
		assertOriginal(t, g, lease, create)
		waitUntil(t, 3*time.Second, func() bool { return clone.Status().ProbeOK })
		if err := clone.Ready(context.Background(), "100.64.9.9", ""); !errors.Is(err, ErrNodeConflict) {
			t.Fatalf("empty machine-id ready err=%v", err)
		}
		assertOriginal(t, g, lease, create)
	})

	t.Run("empty instance-id", func(t *testing.T) {
		g := newRig(t, 1, "626220", func(cfg *Config) { cfg.MachineID = "host-a" })
		g.ready("100.64.0.1")
		lease, create, _ := g.waitKey(true, 2*time.Second)
		raw, err := os.ReadFile(g.reg.cfg.StateFile)
		if err != nil {
			t.Fatal(err)
		}
		clone := startClone(t, g, raw, func(cfg *Config, st *diskState) {
			cfg.MachineID = "host-a"
			st.InstanceID = ""
			cfg.LocalIP = func() string { return "100.64.9.9" }
		})
		assertOriginal(t, g, lease, create)
		waitUntil(t, 3*time.Second, func() bool { return clone.Status().ProbeOK })
		if err := clone.Ready(context.Background(), "100.64.9.9", ""); !errors.Is(err, ErrNodeConflict) {
			t.Fatalf("empty instance-id ready err=%v", err)
		}
		assertOriginal(t, g, lease, create)
	})
}

func TestDrainEpochBlocksOtherInstance(t *testing.T) {
	g := newRig(t, 1, "626220", func(cfg *Config) { cfg.MachineID = "host-a" })
	g.ready("100.64.0.1")
	g.waitKey(true, 2*time.Second)
	if err := g.reg.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	resp, err := g.cli.Get(ctx, EpochKey("626220"))
	cancel()
	if err != nil || len(resp.Kvs) != 1 {
		t.Fatalf("epoch missing err=%v n=%d", err, len(resp.Kvs))
	}
	alerts := make(chan Alert, 2)
	cfg := g.reg.cfg
	cfg.MachineID = "host-b"
	cfg.StateFile = filepath.Join(t.TempDir(), "other.json")
	cfg.OnAlert = func(a Alert) { alerts <- a }
	other, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Stop(context.Background()) })
	if err := other.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, func() bool { return other.Status().ProbeOK })
	if err := other.Ready(context.Background(), "100.64.0.1", ""); !errors.Is(err, ErrNodeConflict) {
		t.Fatalf("takeover during drain err=%v", err)
	}
	select {
	case a := <-alerts:
		if a.Kind != AlertForeignInstance {
			t.Fatalf("alert %+v", a)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no foreign-instance alert")
	}
	if _, _, _, ok := g.kv(g.reg.key); ok {
		t.Fatal("other instance registered during drain")
	}
	g.ip.set("100.64.0.2")
	if err := g.reg.Ready(context.Background(), "100.64.0.2", ""); err != nil {
		t.Fatal(err)
	}
	if _, _, val, ok := g.kv(g.reg.key); !ok || val.EgressIP != "100.64.0.2" || val.InstanceID == "" {
		t.Fatalf("owner return ok=%v val=%+v", ok, val)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	resp, err = g.cli.Get(ctx, EpochKey("626220"))
	cancel()
	if err != nil || len(resp.Kvs) != 0 {
		t.Fatalf("epoch left behind err=%v n=%d", err, len(resp.Kvs))
	}
}

func TestDuplicateDrainDoesNotOverwriteEpoch(t *testing.T) {
	g := newRig(t, 1, "626220", func(cfg *Config) { cfg.MachineID = "host-a" })
	g.ready("100.64.0.1")
	g.waitKey(true, 2*time.Second)
	if err := g.reg.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	resp, err := g.cli.Get(ctx, EpochKey("626220"))
	cancel()
	if err != nil || len(resp.Kvs) != 1 {
		t.Fatalf("epoch missing err=%v n=%d", err, len(resp.Kvs))
	}
	original := string(resp.Kvs[0].Value)
	mainLease := g.reg.Status().LeaseID
	if resp.Kvs[0].Lease == 0 || resp.Kvs[0].Lease == mainLease {
		t.Fatalf("epoch lease %d should be its own lease, not the main lease %d", resp.Kvs[0].Lease, mainLease)
	}

	cfg := g.reg.cfg
	cfg.MachineID = "host-b"
	cfg.StateFile = filepath.Join(t.TempDir(), "other.json")
	other, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Stop(context.Background()) })
	if err := other.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, func() bool { return other.Status().ProbeOK })
	// key 已经不在，这个实例也没拿过它。Drain 是空操作，不能改标记，也不能报冲突。
	if err := other.Drain(context.Background()); err != nil {
		t.Fatalf("duplicate drain err=%v", err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	resp, err = g.cli.Get(ctx, EpochKey("626220"))
	cancel()
	if err != nil || len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != original {
		n := 0
		got := ""
		if resp != nil {
			n = len(resp.Kvs)
			if n == 1 {
				got = string(resp.Kvs[0].Value)
			}
		}
		t.Fatalf("epoch overwritten err=%v n=%d got=%s want=%s", err, n, got, original)
	}
	if err := other.Ready(context.Background(), "100.64.0.1", "100.64.0.9"); !errors.Is(err, ErrNodeConflict) {
		t.Fatalf("duplicate ready err=%v", err)
	}
	if _, _, _, ok := g.kv(g.reg.key); ok {
		t.Fatal("duplicate instance registered after its own drain")
	}
}

func TestOwnerDrainSurvivesDuplicate(t *testing.T) {
	g := newRig(t, 1, "626220", func(cfg *Config) { cfg.MachineID = "host-a" })
	g.ready("100.64.0.1")
	_, _, val := g.waitKey(true, 2*time.Second)
	ownerInst := val.InstanceID

	cfg := g.reg.cfg
	cfg.MachineID = "host-b"
	cfg.StateFile = filepath.Join(t.TempDir(), "other.json")
	cfg.IntranetAddr = "10.9.9.9:8080"
	cfg.LocalIP = func() string { return "100.64.9.9" }
	other, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Stop(context.Background()) })
	if err := other.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, func() bool { return other.Status().ProbeOK })
	if err := other.Drain(context.Background()); !errors.Is(err, ErrNodeConflict) {
		t.Fatalf("duplicate drain while owner is online err=%v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	resp, err := g.cli.Get(ctx, EpochKey("626220"))
	cancel()
	if err != nil || len(resp.Kvs) != 0 {
		n := 0
		if resp != nil {
			n = len(resp.Kvs)
		}
		t.Fatalf("duplicate planted an epoch marker err=%v n=%d", err, n)
	}

	// 租约丢失这条路径也不能让副本把标记写出来。撤销它的 lease，等续租自己发现。
	dupLease := other.Status().LeaseID
	if dupLease == 0 {
		t.Fatal("duplicate has no lease")
	}
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	_, err = g.cli.Revoke(ctx, clientv3.LeaseID(dupLease))
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 4*time.Second, func() bool { return other.Status().LeaseID == 0 })
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	resp, err = g.cli.Get(ctx, EpochKey("626220"))
	cancel()
	if err != nil || len(resp.Kvs) != 0 {
		n := 0
		if resp != nil {
			n = len(resp.Kvs)
		}
		t.Fatalf("lease loss planted an epoch marker err=%v n=%d", err, n)
	}

	// 即便已经有一条别人的标记，持有者的 Drain 也必须把 key 删掉，并把标记换成自己的。
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	_, err = g.cli.Put(ctx, EpochKey("626220"), `{"instance_id":"other","node_id":"626220","addr":"10.9.9.9:8080","machine_id":"host-b"}`)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if err := g.reg.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, _, ok := g.kv(g.reg.key); ok {
		t.Fatal("owner Drain left the key in place")
	}
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	resp, err = g.cli.Get(ctx, EpochKey("626220"))
	cancel()
	if err != nil || len(resp.Kvs) != 1 {
		t.Fatalf("owner epoch missing err=%v", err)
	}
	var ev epochValue
	if json.Unmarshal(resp.Kvs[0].Value, &ev) != nil || ev.InstanceID != ownerInst {
		t.Fatalf("epoch after owner drain %s", resp.Kvs[0].Value)
	}
}

func TestEpochSurvivesExternalLeaseRevoke(t *testing.T) {
	g := newRig(t, 1, "626220", func(cfg *Config) { cfg.MachineID = "host-a" })
	g.ready("100.64.0.1")
	g.waitKey(true, 2*time.Second)
	if err := g.reg.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	mainLease := g.reg.Status().LeaseID
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	resp, err := g.cli.Get(ctx, EpochKey("626220"))
	cancel()
	if err != nil || len(resp.Kvs) != 1 {
		t.Fatalf("epoch missing before revoke err=%v", err)
	}
	epochLease := resp.Kvs[0].Lease
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	_, err = g.cli.Revoke(ctx, clientv3.LeaseID(mainLease))
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 13*time.Second, func() bool { return g.reg.Status().LeaseID == 0 })
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	resp, err = g.cli.Get(ctx, EpochKey("626220"))
	cancel()
	if err != nil || len(resp.Kvs) != 1 {
		t.Fatalf("epoch disappeared with the revoked main lease err=%v n=%d", err, len(resp.Kvs))
	}
	if resp.Kvs[0].Lease == mainLease {
		t.Fatal("epoch still on the revoked main lease")
	}
	ttl, err := leaseTTL(g.cli, resp.Kvs[0].Lease)
	if err != nil || ttl <= 0 {
		t.Fatalf("epoch lease not live ttl=%d err=%v", ttl, err)
	}
	oldEpochTTL, oldErr := leaseTTL(g.cli, epochLease)
	if resp.Kvs[0].Lease != epochLease && oldErr == nil && oldEpochTTL > 0 {
		t.Fatalf("replaced epoch lease %d still live ttl=%d", epochLease, oldEpochTTL)
	}

	cfg := g.reg.cfg
	cfg.MachineID = "host-b"
	cfg.StateFile = filepath.Join(t.TempDir(), "other.json")
	other, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Stop(context.Background()) })
	if err := other.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, func() bool { return other.Status().ProbeOK })
	if err := other.Ready(context.Background(), "100.64.0.1", ""); !errors.Is(err, ErrNodeConflict) {
		t.Fatalf("takeover after real lease revoke err=%v", err)
	}
	g.ip.set("100.64.0.2")
	if err := g.reg.Ready(context.Background(), "100.64.0.2", ""); err != nil {
		t.Fatal(err)
	}
	if _, _, got, ok := g.kv(g.reg.key); !ok || got.EgressIP != "100.64.0.2" {
		t.Fatalf("owner return ok=%v val=%+v", ok, got)
	}
}

func TestEpochSurvivesMainLeaseLossWhileDrained(t *testing.T) {
	g := newRig(t, 1, "626220", func(cfg *Config) { cfg.MachineID = "host-a" })
	g.ready("100.64.0.1")
	mainLease, _, _ := g.waitKey(true, 2*time.Second)
	if err := g.reg.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_, err := g.cli.Revoke(ctx, clientv3.LeaseID(mainLease))
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 13*time.Second, func() bool { return g.reg.Status().LeaseID != mainLease })
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	resp, err := g.cli.Get(ctx, EpochKey("626220"))
	cancel()
	if err != nil || len(resp.Kvs) != 1 {
		t.Fatalf("epoch gone with the main lease err=%v n=%d", err, len(resp.Kvs))
	}
	var ev epochValue
	if err := json.Unmarshal(resp.Kvs[0].Value, &ev); err != nil || ev.InstanceID == "" {
		t.Fatalf("epoch value %s err=%v", resp.Kvs[0].Value, err)
	}
	if resp.Kvs[0].Lease == mainLease {
		t.Fatalf("epoch still attached to the lost main lease %d", mainLease)
	}
	ttl, err := leaseTTL(g.cli, resp.Kvs[0].Lease)
	if err != nil || ttl <= 0 {
		t.Fatalf("epoch lease not live ttl=%d err=%v", ttl, err)
	}

	cfg := g.reg.cfg
	cfg.MachineID = "host-b"
	cfg.StateFile = filepath.Join(t.TempDir(), "other.json")
	other, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Stop(context.Background()) })
	if err := other.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, func() bool { return other.Status().ProbeOK })
	if err := other.Ready(context.Background(), "100.64.0.1", ""); !errors.Is(err, ErrNodeConflict) {
		t.Fatalf("takeover after main lease loss err=%v", err)
	}
	if _, _, _, ok := g.kv(g.reg.key); ok {
		t.Fatal("other instance registered while the epoch survived")
	}
	g.ip.set("100.64.0.2")
	if err := g.reg.Ready(context.Background(), "100.64.0.2", ""); err != nil {
		t.Fatal(err)
	}
	if _, _, val, ok := g.kv(g.reg.key); !ok || val.EgressIP != "100.64.0.2" || val.InstanceID != ev.InstanceID {
		t.Fatalf("owner return ok=%v val=%+v instance=%s", ok, val, ev.InstanceID)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	resp, err = g.cli.Get(ctx, EpochKey("626220"))
	cancel()
	if err != nil || len(resp.Kvs) != 0 {
		t.Fatalf("epoch left behind err=%v n=%d", err, len(resp.Kvs))
	}
}

func TestReportSkipsRegistrationAfterLostState(t *testing.T) {
	g := newRig(t, 1, "626220", nil)
	g.ready("100.64.0.1")
	g.waitKey(true, 2*time.Second)
	if err := g.reg.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	g.reg.abandon()
	if err := os.Remove(g.reg.cfg.StateFile); err != nil {
		t.Fatal(err)
	}
	again, err := New(g.reg.cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = again.Stop(context.Background()) })
	if err := again.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, func() bool { return again.Status().ProbeOK })
	if err := again.Report(context.Background(), Report{EgressIP: "100.64.0.1"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if _, _, _, ok := g.kv(again.key); ok {
		t.Fatal("Report re-registered the old egress after the state file was lost")
	}
	// 状态文件丢了，标记上的内网地址和 machine-id 还是这台机器。不报 foreign_instance，
	// 但在标记过期前也不能接管，即使调用方给了一个不同的 prevEgress。
	alerts := make(chan Alert, 4)
	again.mu.Lock()
	again.cfg.OnAlert = func(a Alert) { alerts <- a }
	again.mu.Unlock()
	g.ip.set("100.64.0.8")
	if err := again.Ready(context.Background(), "100.64.0.8", ""); !errors.Is(err, ErrNodeConflict) {
		t.Fatalf("same-host epoch without prev err=%v", err)
	}
	if err := again.Ready(context.Background(), "100.64.0.8", "100.64.0.1"); !errors.Is(err, ErrNodeConflict) {
		t.Fatalf("same-host epoch with a new prev err=%v", err)
	}
	if _, _, _, ok := g.kv(again.key); ok {
		t.Fatal("same-host clone registered during the owner's redial gap")
	}
	select {
	case a := <-alerts:
		t.Fatalf("same-host leftover raised %s", a.Kind)
	default:
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	resp, err := g.cli.Get(ctx, EpochKey("626220"))
	cancel()
	if err != nil || len(resp.Kvs) != 1 {
		t.Fatalf("owner epoch removed by same-host clone err=%v n=%d", err, len(resp.Kvs))
	}
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

func TestExternalLeaseRevokeThenReady(t *testing.T) {
	g := newRig(t, 1, "626220", nil)
	g.ready("100.64.0.1")
	lease, _, _ := g.waitKey(true, 2*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_, err := g.cli.Revoke(ctx, clientv3.LeaseID(lease))
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	// KeepAliveOnce 在租约已撤销时会一直重试，直到本次调用超时。registrar 要等到自己记下的 deadline 才放弃这条 lease。
	// 放弃之后控制循环会再 Grant，所以新的 LeaseID 可能已经不是 0。
	waitUntil(t, 13*time.Second, func() bool { return g.reg.Status().LeaseID != lease })
	if err := g.reg.Ready(context.Background(), "100.64.0.1", ""); err != nil {
		t.Fatal(err)
	}
	got, _, val, ok := g.kv(g.reg.key)
	if !ok || got == lease || val.EgressIP != "100.64.0.1" {
		t.Fatalf("ready after revoke ok=%v lease %d->%d val=%+v", ok, lease, got, val)
	}
	ttl, err := leaseTTL(g.cli, lease)
	if err != nil && !leaseMissing(err) {
		t.Fatalf("old lease ttl: %v", err)
	}
	if err == nil && ttl > 0 {
		t.Fatalf("revoked lease %d still live ttl=%d", lease, ttl)
	}
}

func TestSameHostCloneDoesNotTakeOver(t *testing.T) {
	g := newRig(t, 1, "626220", func(cfg *Config) { cfg.MachineID = "host-a" })
	g.ready("100.64.0.1")
	g.waitKey(true, 2*time.Second)
	if err := g.reg.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	before, err := g.cli.Get(ctx, EpochKey("626220"))
	cancel()
	if err != nil || len(before.Kvs) != 1 {
		t.Fatalf("epoch missing err=%v", err)
	}
	original := string(before.Kvs[0].Value)

	cfg := g.reg.cfg
	cfg.StateFile = filepath.Join(t.TempDir(), "clone.json")
	cfg.MachineID = "host-a"
	cfg.LocalIP = func() string { return "100.64.0.8" }
	alerts := make(chan Alert, 2)
	cfg.OnAlert = func(a Alert) { alerts <- a }
	clone, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clone.Stop(context.Background()) })
	if err := clone.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, func() bool { return clone.Status().ProbeOK })
	if err := clone.Ready(context.Background(), "100.64.0.8", "100.64.0.1"); !errors.Is(err, ErrNodeConflict) {
		t.Fatalf("clone ready err=%v", err)
	}
	if _, _, _, ok := g.kv(g.reg.key); ok {
		t.Fatal("clone took the key during the redial gap")
	}
	select {
	case a := <-alerts:
		t.Fatalf("same-host clone raised %s", a.Kind)
	default:
	}
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	after, err := g.cli.Get(ctx, EpochKey("626220"))
	cancel()
	if err != nil || len(after.Kvs) != 1 || string(after.Kvs[0].Value) != original {
		t.Fatalf("owner marker changed err=%v", err)
	}
	g.ip.set("100.64.0.2")
	if err := g.reg.Ready(context.Background(), "100.64.0.2", ""); err != nil {
		t.Fatal(err)
	}
	if _, _, val, ok := g.kv(g.reg.key); !ok || val.EgressIP != "100.64.0.2" {
		t.Fatalf("owner return ok=%v val=%+v", ok, val)
	}
}

func TestCopiedStateDifferentAddrIsForeign(t *testing.T) {
	g := newRig(t, 1, "626220", func(cfg *Config) { cfg.MachineID = "host-a" })
	g.ready("100.64.0.1")
	_, _, owner := g.waitKey(true, 2*time.Second)
	if err := g.reg.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(g.reg.cfg.StateFile)
	if err != nil {
		t.Fatal(err)
	}
	cfg := g.reg.cfg
	cfg.StateFile = filepath.Join(t.TempDir(), "copied.json")
	cfg.MachineID = "host-a"
	cfg.IntranetAddr = "10.9.9.9:8080"
	cfg.ProxyPort = 8080
	cfg.LocalIP = func() string { return "100.64.9.9" }
	alerts := make(chan Alert, 2)
	cfg.OnAlert = func(a Alert) { alerts <- a }
	if err := os.WriteFile(cfg.StateFile, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	clone, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clone.Stop(context.Background()) })
	if err := clone.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, func() bool { return clone.Status().ProbeOK })
	if clone.instanceID != owner.InstanceID {
		t.Fatalf("copied state instance %s want %s", clone.instanceID, owner.InstanceID)
	}
	if err := clone.Ready(context.Background(), "100.64.9.9", "100.64.0.1"); !errors.Is(err, ErrNodeConflict) {
		t.Fatalf("copied state ready err=%v", err)
	}
	if _, _, _, ok := g.kv(g.reg.key); ok {
		t.Fatal("copied state registered on a different addr")
	}
	select {
	case a := <-alerts:
		if a.Kind != AlertForeignInstance {
			t.Fatalf("alert %+v", a)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no foreign alert for copied state on a different addr")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	resp, err := g.cli.Get(ctx, EpochKey("626220"))
	cancel()
	if err != nil || len(resp.Kvs) != 1 {
		t.Fatalf("marker missing err=%v", err)
	}
	var ev epochValue
	if json.Unmarshal(resp.Kvs[0].Value, &ev) != nil || ev.InstanceID != owner.InstanceID || ev.Addr != "10.0.8.26:8080" {
		t.Fatalf("marker rewritten: %s", resp.Kvs[0].Value)
	}
}

func TestMixedEpochMarkerFormats(t *testing.T) {
	t.Run("old marker without addr", func(t *testing.T) {
		g := newRig(t, 1, "626220", func(cfg *Config) { cfg.MachineID = "host-a" })
		g.ready("100.64.0.1")
		_, _, val := g.waitKey(true, 2*time.Second)
		if err := g.reg.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := g.cli.Get(ctx, EpochKey("626220"))
		if err != nil || len(resp.Kvs) != 1 {
			cancel()
			t.Fatalf("epoch missing err=%v", err)
		}
		old, err := json.Marshal(epochValue{InstanceID: val.InstanceID, NodeID: "626220"})
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		_, err = g.cli.Put(ctx, EpochKey("626220"), string(old), clientv3.WithLease(clientv3.LeaseID(resp.Kvs[0].Lease)))
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		g.ip.set("100.64.0.2")
		if err := g.reg.Ready(context.Background(), "100.64.0.2", ""); err != nil {
			t.Fatal(err)
		}
		if _, _, got, ok := g.kv(g.reg.key); !ok || got.EgressIP != "100.64.0.2" || got.InstanceID != val.InstanceID {
			t.Fatalf("old marker reclaim ok=%v val=%+v", ok, got)
		}
		ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
		resp, err = g.cli.Get(ctx, EpochKey("626220"))
		cancel()
		if err != nil || len(resp.Kvs) != 0 {
			t.Fatalf("old marker left behind n=%d err=%v", len(resp.Kvs), err)
		}
	})

	t.Run("same instance different addr", func(t *testing.T) {
		g := newRig(t, 1, "626220", func(cfg *Config) { cfg.MachineID = "host-a" })
		g.ready("100.64.0.1")
		_, _, val := g.waitKey(true, 2*time.Second)
		if err := g.reg.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := g.cli.Get(ctx, EpochKey("626220"))
		if err != nil || len(resp.Kvs) != 1 {
			cancel()
			t.Fatalf("epoch missing err=%v", err)
		}
		body, err := json.Marshal(epochValue{
			InstanceID: val.InstanceID,
			NodeID:     "626220",
			Addr:       "10.9.9.9:8080",
			MachineID:  "host-a",
		})
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		_, err = g.cli.Put(ctx, EpochKey("626220"), string(body), clientv3.WithLease(clientv3.LeaseID(resp.Kvs[0].Lease)))
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		alerts := make(chan Alert, 1)
		g.reg.mu.Lock()
		g.reg.cfg.OnAlert = func(a Alert) { alerts <- a }
		g.reg.mu.Unlock()
		g.ip.set("100.64.0.2")
		if err := g.reg.Ready(context.Background(), "100.64.0.2", ""); !errors.Is(err, ErrNodeConflict) {
			t.Fatalf("different addr ready err=%v", err)
		}
		if _, _, _, ok := g.kv(g.reg.key); ok {
			t.Fatal("different addr marker admitted the owner")
		}
		select {
		case a := <-alerts:
			if a.Kind != AlertForeignInstance {
				t.Fatalf("alert %+v", a)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("no foreign alert")
		}
		ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
		resp, err = g.cli.Get(ctx, EpochKey("626220"))
		cancel()
		if err != nil || len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != string(body) {
			t.Fatalf("marker changed err=%v", err)
		}
	})
}

func TestDrainRetriesWhenEpochLeaseRevoked(t *testing.T) {
	g := newRig(t, 1, "626220", func(cfg *Config) { cfg.MachineID = "host-a" })
	g.ready("100.64.0.1")
	g.waitKey(true, 2*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	gr, err := g.cli.Grant(ctx, 30)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if _, err := g.cli.Revoke(ctx, gr.ID); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	g.reg.mu.Lock()
	g.reg.epochLease = gr.ID
	g.reg.epochNextRenew = time.Now().Add(time.Hour)
	g.reg.epochDeadline = time.Now().Add(time.Hour)
	g.reg.mu.Unlock()
	if err := g.reg.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, _, ok := g.kv(g.reg.key); ok {
		t.Fatal("key still present after drain")
	}
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	resp, err := g.cli.Get(ctx, EpochKey("626220"))
	cancel()
	if err != nil || len(resp.Kvs) != 1 {
		t.Fatalf("marker missing after revoked epoch lease err=%v n=%d", err, len(resp.Kvs))
	}
	if resp.Kvs[0].Lease == int64(gr.ID) {
		t.Fatal("marker still on the revoked epoch lease")
	}
	ttl, err := leaseTTL(g.cli, resp.Kvs[0].Lease)
	if err != nil || ttl <= 0 {
		t.Fatalf("replacement epoch lease ttl=%d err=%v", ttl, err)
	}
}

func TestDrainFallsBackWhenEpochGrantFails(t *testing.T) {
	g := newRig(t, 1, "626220", func(cfg *Config) { cfg.MachineID = "host-a" })
	g.ready("100.64.0.1")
	g.waitKey(true, 2*time.Second)
	g.reg.opMu.Lock()
	g.reg.mu.Lock()
	id := g.reg.epochLease
	g.reg.clearEpochLeaseLocked()
	g.reg.epochGrant = func(context.Context, int64) (*clientv3.LeaseGrantResponse, error) {
		return nil, errors.New("grant failed")
	}
	g.reg.mu.Unlock()
	g.reg.opMu.Unlock()
	if id != 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _ = g.cli.Revoke(ctx, id)
		cancel()
	}
	if err := g.reg.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, _, ok := g.kv(g.reg.key); ok {
		t.Fatal("key still present when epoch grant failed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	resp, err := g.cli.Get(ctx, EpochKey("626220"))
	cancel()
	if err != nil || len(resp.Kvs) != 0 {
		n := 0
		if resp != nil {
			n = len(resp.Kvs)
		}
		t.Fatalf("fallback left a marker err=%v n=%d", err, n)
	}
	g.reg.mu.Lock()
	id = g.reg.epochLease
	g.reg.mu.Unlock()
	if id != 0 {
		t.Fatalf("failed grant left epoch lease %d", id)
	}
}

func TestConflictRevokesUnusedEpochLease(t *testing.T) {
	g := newRig(t, 1, "626220", func(cfg *Config) { cfg.MachineID = "host-a" })
	g.ready("100.64.0.1")
	g.waitKey(true, 2*time.Second)
	g.reg.opMu.Lock()
	g.reg.mu.Lock()
	pre := g.reg.epochLease
	g.reg.clearEpochLeaseLocked()
	g.reg.mu.Unlock()
	g.reg.opMu.Unlock()
	if pre != 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _ = g.cli.Revoke(ctx, pre)
		cancel()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	before, err := g.cli.Leases(ctx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	g.reg.opMu.Lock()
	g.reg.mu.Lock()
	err = g.reg.deleteKeyAndEpoch(context.Background(), 1)
	id := g.reg.epochLease
	g.reg.mu.Unlock()
	g.reg.opMu.Unlock()
	if !errors.Is(err, errLostRace) {
		t.Fatalf("stale mod err=%v", err)
	}
	if id != 0 {
		t.Fatalf("unused epoch lease %d still renewing", id)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	after, err := g.cli.Leases(ctx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Leases) != len(before.Leases) {
		t.Fatalf("leases %d -> %d", len(before.Leases), len(after.Leases))
	}
	if _, _, _, ok := g.kv(g.reg.key); !ok {
		t.Fatal("conflict path deleted the key")
	}
}

func TestDrainedEpochHoldExpires(t *testing.T) {
	alerts := make(chan Alert, 1)
	g := newRig(t, 1, "626220", func(cfg *Config) {
		cfg.MachineID = "host-a"
		cfg.TTL = time.Second
		cfg.RenewInterval = 400 * time.Millisecond
		cfg.EpochTTL = 2 * time.Second
		cfg.EpochRenewInterval = time.Second
		cfg.MaxDrained = 400 * time.Millisecond
		cfg.OnAlert = func(a Alert) {
			select {
			case alerts <- a:
			default:
			}
		}
	})
	g.ready("100.64.0.1")
	g.waitKey(true, 2*time.Second)
	if err := g.reg.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case a := <-alerts:
		if a.Kind != AlertEpochHoldExpired {
			t.Fatalf("alert %+v", a)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("drained holder kept renewing the marker")
	}
	waitUntil(t, 6*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		resp, err := g.cli.Get(ctx, EpochKey("626220"))
		return err == nil && len(resp.Kvs) == 0
	})
	cfg := g.reg.cfg
	cfg.MachineID = "host-b"
	cfg.StateFile = filepath.Join(t.TempDir(), "other.json")
	cfg.OnAlert = func(Alert) {}
	cfg.MaxDrained = 5 * time.Minute
	other, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Stop(context.Background()) })
	if err := other.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, func() bool { return other.Status().ProbeOK })
	if err := other.Ready(context.Background(), "100.64.0.1", ""); err != nil {
		t.Fatal(err)
	}
	if _, _, _, ok := g.kv(other.key); !ok {
		t.Fatal("other instance could not register after the marker expired")
	}
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
