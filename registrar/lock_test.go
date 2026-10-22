package registrar

import (
	"context"
	"errors"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
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
