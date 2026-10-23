package registrar

import (
	"context"
	"errors"
	"net/url"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestJitterBeforeDrainZeroAndCancel(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.Endpoints = []string{"127.0.0.1:1"}
	cfg.NodeID = "626220"
	cfg.IntranetAddr = "10.0.8.26:8080"
	cfg.ProxyPort = 8080
	cfg.ProbeAddr = "127.0.0.1:1"
	cfg.StateFile = dir + "/state.json"
	cfg.LocalIP = func() string { return "" }
	cfg.RedialJitter = 0
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if r.cfg.RedialJitter != 0 {
		t.Fatalf("explicit zero jitter became %s", r.cfg.RedialJitter)
	}
	if err := r.JitterBeforeDrain(context.Background()); err != nil {
		t.Fatal(err)
	}
	r.cfg.RedialJitter = time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = r.JitterBeforeDrain(ctx)
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("jitter ignored context, slept %s", elapsed)
	}
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}

func TestTxnTimeoutKeepsCommittedEpoch(t *testing.T) {
	g := newRig(t, 1, "626220", func(cfg *Config) { cfg.MachineID = "host-a" })
	g.ready("100.64.0.1")
	g.waitKey(true, 2*time.Second)
	g.reg.mu.Lock()
	g.reg.epochTxnHook = func(ctx context.Context, txn clientv3.Txn) (*clientv3.TxnResponse, error) {
		tr, err := txn.Commit()
		if err != nil {
			return tr, err
		}
		return tr, context.DeadlineExceeded
	}
	g.reg.mu.Unlock()
	if err := g.reg.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, _, ok := g.kv(g.reg.key); ok {
		t.Fatal("committed txn left the business key")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	resp, err := g.cli.Get(ctx, EpochKey("626220"))
	cancel()
	if err != nil || len(resp.Kvs) != 1 {
		n := 0
		if resp != nil {
			n = len(resp.Kvs)
		}
		t.Fatalf("marker missing after ambiguous commit err=%v n=%d", err, n)
	}
	g.reg.mu.Lock()
	id := g.reg.epochLease
	body, berr := g.reg.epochBody()
	g.reg.mu.Unlock()
	if berr != nil {
		t.Fatal(berr)
	}
	if int64(id) == 0 || int64(id) != resp.Kvs[0].Lease || string(resp.Kvs[0].Value) != body {
		t.Fatalf("revoked or replaced marker lease=%d id=%d", resp.Kvs[0].Lease, id)
	}
	ttl, err := leaseTTL(g.cli, resp.Kvs[0].Lease)
	if err != nil || ttl <= 0 {
		t.Fatalf("epoch lease ttl=%d err=%v", ttl, err)
	}
}

func TestRenewSurvivesSlowEtcdDuringDrain(t *testing.T) {
	const delay = 1500 * time.Millisecond
	g := newRig(t, 1, "626220", func(cfg *Config) {
		cfg.MachineID = "host-a"
		cfg.TTL = 10 * time.Second
		cfg.RenewInterval = time.Second
		cfg.OpTimeout = 8 * time.Second
		cfg.DrainAttempt = 8 * time.Second
		cfg.DrainBudget = 20 * time.Second
		cfg.StartGrantTimeout = 15 * time.Second
		cfg.BackgroundTimeout = 8 * time.Second
		cfg.DialTimeout = 8 * time.Second
		cfg.DialOptions = []grpc.DialOption{
			grpc.WithChainUnaryInterceptor(delayUnary(delay)),
			grpc.WithChainStreamInterceptor(delayStream(delay)),
		}
	})
	g.ready("100.64.0.1")
	g.waitKey(true, 15*time.Second)
	waitUntil(t, 12*time.Second, func() bool { return len(g.reg.RenewTimes()) >= 2 })
	lease := g.reg.Status().LeaseID
	if lease == 0 {
		t.Fatal("no business lease")
	}
	if err := g.reg.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	g.ip.set("100.64.0.2")
	if err := g.reg.Ready(context.Background(), "100.64.0.2", "100.64.0.1"); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 12*time.Second, func() bool { return len(g.reg.RenewTimes()) >= 4 })
	times := g.reg.RenewTimes()
	var maxGap time.Duration
	for i := 1; i < len(times); i++ {
		if d := times[i].Sub(times[i-1]); d > maxGap {
			maxGap = d
		}
	}
	if maxGap <= 0 || maxGap >= 7*time.Second {
		t.Fatalf("renew gap %s over %d samples, want >0 and <7s (ttl 10s)", maxGap, len(times))
	}
	if g.log.has("lease_lost") {
		t.Fatal("lease lost while etcd was slow")
	}
	if got := g.reg.Status().LeaseID; got != lease {
		t.Fatalf("business lease changed %d -> %d", lease, got)
	}
}

func TestDrainAttemptStretchesWithRTT(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.Endpoints = []string{"127.0.0.1:1"}
	cfg.NodeID = "626220"
	cfg.IntranetAddr = "10.0.8.26:8080"
	cfg.ProxyPort = 8080
	cfg.ProbeAddr = "127.0.0.1:1"
	cfg.StateFile = dir + "/state.json"
	cfg.LocalIP = func() string { return "" }
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := r.drainAttemptFor(6 * time.Second); got != DefaultDrainAttempt {
		t.Fatalf("no sample: %s", got)
	}
	r.lastRTT.Store(int64(1500 * time.Millisecond))
	got := r.drainAttemptFor(6 * time.Second)
	want := 2250 * time.Millisecond
	if got != want {
		t.Fatalf("attempt %s want %s", got, want)
	}
	if got >= 3*time.Second {
		t.Fatalf("one-round-trip attempt %s is not under the 3s drain target", got)
	}
	if got = r.drainAttemptFor(time.Second); got != time.Second {
		t.Fatalf("remain cap %s", got)
	}
}

func TestDrainDoesNotReuseUnpublishedEpochHold(t *testing.T) {
	alerts := make(chan Alert, 4)
	g := newRig(t, 1, "626220", func(cfg *Config) {
		cfg.MachineID = "host-a"
		cfg.EpochTTL = 30 * time.Second
		cfg.EpochRenewInterval = time.Second
		cfg.MaxDrained = 5 * time.Minute
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	resp, err := g.cli.Get(ctx, EpochKey("626220"))
	cancel()
	if err != nil || len(resp.Kvs) != 1 {
		n := 0
		if resp != nil {
			n = len(resp.Kvs)
		}
		t.Fatalf("marker after drain err=%v n=%d", err, n)
	}
	oldLease := resp.Kvs[0].Lease

	g.reg.opMu.Lock()
	g.reg.mu.Lock()
	if g.reg.pubEpoch.Load() != oldLease || int64(g.reg.epochLease) != oldLease {
		pub, field := g.reg.pubEpoch.Load(), int64(g.reg.epochLease)
		g.reg.mu.Unlock()
		g.reg.opMu.Unlock()
		t.Fatalf("published %d field %d marker %d", pub, field, oldLease)
	}
	since := g.reg.holdSince.Load()
	gen := g.reg.holdGen.Load()
	if !g.reg.pubEpoch.CompareAndSwap(oldLease, 0) {
		g.reg.mu.Unlock()
		g.reg.opMu.Unlock()
		t.Fatal("epoch lease was not published")
	}
	g.reg.queueHold(holdFail{id: oldLease, since: since, gen: gen})
	// MaxDrained 已经在锁外把 pubEpoch 清掉，drainSignals 还没跑。正在等锁的 Drain 从这里开始。
	g.reg.beginDrainedHold()
	if g.reg.holdStillCurrent(holdFail{id: oldLease, since: since, gen: gen}) {
		g.reg.mu.Unlock()
		g.reg.opMu.Unlock()
		t.Fatal("stale MaxDrained window still current for the new Drain")
	}
	if err := g.reg.deleteOwned(context.Background(), "drain", g.reg.cfg.DrainBudget); err != nil {
		g.reg.mu.Unlock()
		g.reg.opMu.Unlock()
		t.Fatal(err)
	}
	newLease := int64(g.reg.epochLease)
	pub := g.reg.pubEpoch.Load()
	released := g.reg.epochHoldReleased
	g.reg.mu.Unlock()
	g.reg.opMu.Unlock()

	if newLease == 0 || newLease == oldLease || pub != newLease {
		t.Fatalf("reused unpublished epoch lease old=%d new=%d pub=%d", oldLease, newLease, pub)
	}
	if released {
		t.Fatal("new Drain took the expired-hold path")
	}
	select {
	case a := <-alerts:
		t.Fatalf("false alert %+v", a)
	case <-time.After(500 * time.Millisecond):
	}
	if g.log.has("epoch_hold_expired") {
		t.Fatal("false epoch_hold_expired")
	}
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	resp, err = g.cli.Get(ctx, EpochKey("626220"))
	cancel()
	if err != nil || len(resp.Kvs) != 1 || resp.Kvs[0].Lease != newLease {
		t.Fatalf("marker lease err=%v resp=%v", err, resp)
	}
	ttl, err := leaseTTL(g.cli, newLease)
	if err != nil || ttl <= 0 {
		t.Fatalf("replacement ttl=%d err=%v", ttl, err)
	}
}

func TestDrainOneTxnUnderLatency(t *testing.T) {
	const delay = 1500 * time.Millisecond
	var txn, grant, rang atomic.Int64
	count := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		switch {
		case strings.Contains(method, "KV/Txn"):
			txn.Add(1)
		case strings.Contains(method, "LeaseGrant"):
			grant.Add(1)
		case strings.Contains(method, "KV/Range"):
			rang.Add(1)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	g := newRig(t, 1, "626220", func(cfg *Config) {
		cfg.MachineID = "host-a"
		cfg.DialOptions = []grpc.DialOption{
			grpc.WithChainUnaryInterceptor(delayUnary(delay), count),
			grpc.WithChainStreamInterceptor(delayStream(delay)),
		}
	})
	if g.reg.cfg.DrainAttempt != DefaultDrainAttempt || g.reg.cfg.DrainBudget != DefaultDrainBudget {
		t.Fatalf("attempt %s budget %s", g.reg.cfg.DrainAttempt, g.reg.cfg.DrainBudget)
	}
	g.ready("100.64.0.1")
	g.waitKey(true, 20*time.Second)
	txn.Store(0)
	grant.Store(0)
	rang.Store(0)
	t0 := time.Now()
	if err := g.reg.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(t0)
	if elapsed > 3*time.Second {
		t.Fatalf("Drain with %s etcd delay took %s (default attempt %s budget %s)", delay, elapsed, DefaultDrainAttempt, DefaultDrainBudget)
	}
	if txn.Load() != 1 || grant.Load() != 0 || rang.Load() != 0 {
		t.Fatalf("Drain RPCs txn=%d grant=%d range=%d elapsed=%s, want one txn", txn.Load(), grant.Load(), rang.Load(), elapsed)
	}
	if _, _, _, ok := g.kv(g.reg.key); ok {
		t.Fatal("key still present")
	}
	if g.log.has("lease_lost") {
		t.Fatal("lease lost")
	}
	t.Logf("Drain under %s delay %s", delay, elapsed)
}

func TestDrainLeaderBlackholeWithinBudget(t *testing.T) {
	g := newRig(t, 3, "626220", func(cfg *Config) { cfg.MachineID = "host-a" })
	g.ready("100.64.0.1")
	g.waitKey(true, 2*time.Second)
	lease := g.reg.Status().LeaseID
	undo := dropLeaderPackets(t, g.cl)
	t0 := time.Now()
	err := g.reg.Drain(context.Background())
	elapsed := time.Since(t0)
	undo()
	if err != nil {
		t.Fatalf("blackhole Drain after %s: %v", elapsed, err)
	}
	if elapsed > g.reg.cfg.DrainBudget {
		t.Fatalf("Drain %s exceeded budget %s", elapsed, g.reg.cfg.DrainBudget)
	}
	if _, _, _, ok := g.kv(g.reg.key); ok {
		t.Fatal("key still present")
	}
	if got := g.reg.Status().LeaseID; got != lease {
		t.Fatalf("business lease %d -> %d", lease, got)
	}
	if g.log.has("lease_lost") {
		t.Fatal("lease lost while leader was blackholed")
	}
	// 3s 是进程被杀掉（连接重置）时的目标。包被丢掉时，第一次尝试会坐满超时才换成员，不要求落在 3s 内。
	t.Logf("leader packet-drop Drain %s; within_3s=%v budget=%s", elapsed, elapsed <= 3*time.Second, g.reg.cfg.DrainBudget)
}

func dropLeaderPackets(t *testing.T, cl interface {
	LeaderURLs() (string, string, error)
}) func() {
	t.Helper()
	clientURL, peerURL, err := cl.LeaderURLs()
	if err != nil {
		t.Fatal(err)
	}
	ports := []string{urlPort(t, clientURL), urlPort(t, peerURL)}
	ipt := []string{"iptables"}
	if exec.Command("iptables", "-w", "-L", "OUTPUT", "-n").Run() != nil {
		ipt = []string{"sudo", "-n", "iptables"}
	}
	var rules [][]string
	for _, port := range ports {
		rules = append(rules,
			[]string{"-w", "-I", "OUTPUT", "-p", "tcp", "-d", "127.0.0.1", "--dport", port, "-j", "DROP"},
			[]string{"-w", "-I", "INPUT", "-p", "tcp", "-s", "127.0.0.1", "--sport", port, "-j", "DROP"},
		)
	}
	for _, args := range rules {
		cmd := exec.Command(ipt[0], append(append([]string{}, ipt[1:]...), args...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("iptables %v: %v %s", args, err, out)
		}
	}
	return func() {
		for i := len(rules) - 1; i >= 0; i-- {
			del := append([]string{}, rules[i]...)
			del[1] = "-D"
			cmd := exec.Command(ipt[0], append(append([]string{}, ipt[1:]...), del...)...)
			_ = cmd.Run()
		}
	}
}

func urlPort(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u.Port()
}

func delayUnary(d time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if err := sleepCtx(ctx, d); err != nil {
			return err
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func delayStream(d time.Duration) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if err := sleepCtx(ctx, d); err != nil {
			return nil, err
		}
		return streamer(ctx, desc, cc, method, opts...)
	}
}

func TestNotPrimaryLessorIsRetryableText(t *testing.T) {
	if !notPrimaryLessor(status.Error(codes.Unknown, "not a primary lessor")) {
		t.Fatal("missing not a primary lessor")
	}
	if !notPrimaryLessor(status.Error(codes.Unavailable, "etcdserver: leader changed")) {
		t.Fatal("missing leader changed")
	}
	if notPrimaryLessor(status.Error(codes.NotFound, "etcdserver: requested lease not found")) {
		t.Fatal("lease not found matched the handoff check")
	}
	if notPrimaryLessor(nil) {
		t.Fatal("nil")
	}
}

// KeepAlive 被拦截成 etcd 的 not a primary lessor，并且本地 deadline 已经过了。
// TimeToLive 仍看到 TTL>0 时，不能 lease_lost，业务 lease id 保持不变，恢复后续租还能成功。
func TestNotPrimaryPastDeadlineKeepsLiveLease(t *testing.T) {
	var failKA atomic.Bool
	var ttlCalls atomic.Int64
	g := newNotPrimaryRig(t, &failKA, nil, &ttlCalls)
	g.ready("100.64.0.1")
	g.waitKey(true, 5*time.Second)
	lease := g.reg.Status().LeaseID
	if lease == 0 {
		t.Fatal("no business lease")
	}
	baseTTL := ttlCalls.Load()
	baseOK := g.log.count("ka_ok")
	failKA.Store(true)
	forceDeadlinesUntil(t, g.reg, 8*time.Second, func() bool {
		return ttlCalls.Load() > baseTTL && g.log.has("ka_err")
	})
	if g.log.has("lease_lost") {
		t.Fatal("live lease declared lost on not a primary lessor")
	}
	if got := g.reg.Status().LeaseID; got != lease {
		t.Fatalf("lease id %d -> %d", lease, got)
	}
	if ttlCalls.Load() <= baseTTL {
		t.Fatal("TimeToLive was not called")
	}
	failKA.Store(false)
	g.reg.renewMu.Lock()
	g.reg.bizDue = time.Now().Add(-time.Millisecond)
	g.reg.epDue = time.Now().Add(-time.Millisecond)
	g.reg.renewMu.Unlock()
	waitUntil(t, 5*time.Second, func() bool { return g.log.count("ka_ok") > baseOK })
	if g.log.has("lease_lost") {
		t.Fatal("lease lost after keepalive recovered")
	}
	if got := g.reg.Status().LeaseID; got != lease {
		t.Fatalf("lease id after recover %d -> %d", lease, got)
	}
	got, _, _, ok := g.kv(g.reg.key)
	if !ok || got != lease {
		t.Fatalf("key lease=%d ok=%v want %d", got, ok, lease)
	}
}

// 租约已经被撤销。KeepAlive 仍只返回 not a primary lessor，本地 deadline 已过。
// TimeToLive 看到 not found 之后才允许 lease_lost。
func TestNotPrimaryPastDeadlineLosesRevokedLease(t *testing.T) {
	var failKA atomic.Bool
	var ttlCalls atomic.Int64
	g := newNotPrimaryRig(t, &failKA, nil, &ttlCalls)
	g.ready("100.64.0.1")
	g.waitKey(true, 5*time.Second)
	lease := g.reg.Status().LeaseID
	if lease == 0 {
		t.Fatal("no business lease")
	}
	baseTTL := ttlCalls.Load()
	failKA.Store(true)
	// 先等到拦截生效，再撤销。否则正在飞的那次真实 KeepAlive 会直接看到 not found，
	// 丢失通知就绕过了 TimeToLive。
	waitUntil(t, 5*time.Second, func() bool { return g.log.has("ka_err") })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_, err := g.cli.Revoke(ctx, clientv3.LeaseID(lease))
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	forceDeadlinesUntil(t, g.reg, 8*time.Second, func() bool {
		return g.log.has("lease_lost") && ttlCalls.Load() > baseTTL
	})
	if ttlCalls.Load() <= baseTTL {
		t.Fatal("lease_lost without TimeToLive")
	}
	ttl, err := leaseTTL(g.cli, lease)
	if err == nil && ttl > 0 {
		t.Fatalf("revoked lease %d still live ttl=%d", lease, ttl)
	}
}

// TimeToLive 自己也返回 not a primary lessor。本地 deadline 已过也不能 lease_lost。
func TestNotPrimaryProbeFailureDoesNotLoseLease(t *testing.T) {
	var failKA atomic.Bool
	var failTTL atomic.Bool
	var ttlCalls atomic.Int64
	g := newNotPrimaryRig(t, &failKA, &failTTL, &ttlCalls)
	g.ready("100.64.0.1")
	g.waitKey(true, 5*time.Second)
	lease := g.reg.Status().LeaseID
	if lease == 0 {
		t.Fatal("no business lease")
	}
	baseTTL := ttlCalls.Load()
	failKA.Store(true)
	failTTL.Store(true)
	forceDeadlinesUntil(t, g.reg, 8*time.Second, func() bool {
		return ttlCalls.Load() > baseTTL && g.log.has("ka_err")
	})
	if g.log.has("lease_lost") {
		t.Fatal("probe failure declared the lease lost")
	}
	if got := g.reg.Status().LeaseID; got != lease {
		t.Fatalf("lease id %d -> %d", lease, got)
	}
	got, _, _, ok := g.kv(g.reg.key)
	if !ok || got != lease {
		t.Fatalf("key lease=%d ok=%v want %d", got, ok, lease)
	}
}

func newNotPrimaryRig(t *testing.T, failKA, failTTL *atomic.Bool, ttlCalls *atomic.Int64) *rig {
	t.Helper()
	return newRig(t, 1, "626220", func(cfg *Config) {
		cfg.MachineID = "host-a"
		cfg.TTL = 30 * time.Second
		cfg.RenewInterval = time.Second
		cfg.DialOptions = []grpc.DialOption{
			grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				if strings.Contains(method, "LeaseTimeToLive") {
					ttlCalls.Add(1)
					if failTTL != nil && failTTL.Load() {
						return status.Error(codes.Unknown, "not a primary lessor")
					}
				}
				return invoker(ctx, method, req, reply, cc, opts...)
			}),
			grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				if failKA.Load() && strings.Contains(method, "LeaseKeepAlive") {
					return nil, status.Error(codes.Unknown, "not a primary lessor")
				}
				return streamer(ctx, desc, cc, method, opts...)
			}),
		}
	})
}

func forceDeadlinesUntil(t *testing.T, r *Registrar, d time.Duration, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if done() {
			return
		}
		past := time.Now().Add(-time.Second)
		r.renewMu.Lock()
		r.bizDead, r.bizDue = past, past
		r.epDead, r.epDue = past, past
		r.renewMu.Unlock()
		time.Sleep(30 * time.Millisecond)
	}
	if !done() {
		t.Fatal("condition not met")
	}
}
