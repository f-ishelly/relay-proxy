package registrar

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var (
	// ErrNodeConflict 表示 key 被别的存活 lease 占着，本进程没有覆盖。
	ErrNodeConflict = errors.New("nodeId conflict")
	// ErrEgressMismatch 表示 Ready 带来的出口地址和本机 ppp 地址不一致。
	ErrEgressMismatch = errors.New("egress ip mismatch")
	// ErrNotRegistered 表示探测未通过，或 etcd 里还没有这次 Ready 的 key。
	ErrNotRegistered = errors.New("not registered")
	// ErrStopped 表示已经 Stop。
	ErrStopped = errors.New("registrar stopped")
	// ErrDrainTimeout 表示 Drain 在预算内没有得到 etcd 确认。
	ErrDrainTimeout = errors.New("drain timeout")
	// ErrInvalidConfig 表示配置不完整。
	ErrInvalidConfig = errors.New("invalid registrar config")

	errLostRace = errors.New("lost race")
)

// Registrar 是一台 L2 主机的注册状态机。
type Registrar struct {
	cfg Config
	key string

	mu      sync.Mutex
	cli     *clientv3.Client
	root    context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started bool
	stopped bool

	lease    clientv3.LeaseID
	stale    []int64
	regIP    string
	resumeIP string
	drained  bool
	wantIP   string
	netOK    bool
	probeOK  bool
	fails    int
	oks      int

	deadline        time.Time
	nextRenew       time.Time
	retryNotBefore  time.Time
	lastErr         error
	conflictLease   int64
	conflictAlerted bool
}

// New 检查配置。还不会连接 etcd。
func New(cfg Config) (*Registrar, error) {
	if err := loadNodeID(&cfg); err != nil {
		return nil, err
	}
	applyDefaults(&cfg)
	if err := validate(&cfg); err != nil {
		return nil, err
	}
	return &Registrar{cfg: cfg, key: Key(cfg.NodeID), netOK: true}, nil
}

// Start 连接 etcd，Grant 一个  TTL 的 lease，并启动续租、探测和地址守卫。
// 只有探测通过且（上次在线且出口未变，或之后调用了 Ready/Report）才 Put。
func (r *Registrar) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return errors.New("registrar already started")
	}
	r.started = true
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.cfg.StartGrantTimeout)
		defer cancel()
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:            append([]string(nil), r.cfg.Endpoints...),
		DialTimeout:          r.cfg.DialTimeout,
		DialKeepAliveTime:    r.cfg.DialKeepAliveTime,
		DialKeepAliveTimeout: r.cfg.DialKeepAliveTimeout,
		AutoSyncInterval:     0,
		Logger:               zap.NewNop(),
	})
	if err != nil {
		r.started = false
		r.mu.Unlock()
		return err
	}
	r.cli = cli
	if err := r.bootstrap(ctx); err != nil {
		r.cli = nil
		r.started = false
		r.mu.Unlock()
		_ = cli.Close()
		return err
	}
	r.root, r.cancel = context.WithCancel(context.Background())
	r.wg.Add(2)
	go r.controlLoop()
	go r.probeLoop()
	if len(r.cfg.Endpoints) < 3 {
		r.event("endpoints_warning", "n", len(r.cfg.Endpoints), "msg", "生产环境必须配置全部 etcd 成员")
	}
	r.event("start", "key", r.key, "lease", int64(r.lease), "takeover", r.wantIP != "")
	r.mu.Unlock()
	return nil
}

// Drain 在挂断拨号前同步删除 key。drained 先落盘，etcd 确认后才返回。lease 继续续租。
func (r *Registrar) Drain(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return ErrStopped
	}
	r.drained = true
	r.saveLocked()
	r.event("api_drain")
	deadline := time.Now().Add(r.cfg.DrainBudget)
	var err error
	for {
		attempt := r.cfg.DrainAttempt
		if remain := time.Until(deadline); remain < attempt {
			attempt = remain
		}
		if attempt <= 0 {
			break
		}
		err = r.deleteOwned(ctx, "drain", attempt)
		if err == nil {
			r.saveLocked()
			return nil
		}
		if errors.Is(err, ErrNodeConflict) {
			return err
		}
		if ctx.Err() != nil {
			return fmt.Errorf("%w: %v", ErrDrainTimeout, ctx.Err())
		}
	}
	if err == nil {
		err = errors.New("budget exhausted")
	}
	return fmt.Errorf("%w: %v", ErrDrainTimeout, err)
}

// Ready 在出口校验通过后调用。本机 ppp 地址必须等于 egressIP，且 gost 探测已通过，才会 Put。
func (r *Registrar) Ready(ctx context.Context, egressIP string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return ErrStopped
	}
	lip := r.cfg.LocalIP()
	r.event("api_ready", "ip", egressIP, "local_ip", lip)
	if egressIP == "" || lip != egressIP {
		return fmt.Errorf("%w: local %q", ErrEgressMismatch, lip)
	}
	r.drained = false
	r.wantIP = egressIP
	r.netOK = true
	r.resumeIP = egressIP
	r.reconcile(ctx, "ready", r.cfg.OpTimeout)
	if errors.Is(r.lastErr, ErrNodeConflict) {
		return ErrNodeConflict
	}
	if r.regIP != egressIP {
		if r.lastErr != nil {
			return r.lastErr
		}
		return ErrNotRegistered
	}
	return nil
}

// Report 接收 admin 现有巡检。netOK 为 false 时删除 key，恢复后 Put。
// drained 期间忽略。上报正文不写 etcd；只有注册状态需要变化时才动 key。
func (r *Registrar) Report(ctx context.Context, netOK bool, egressIP string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return ErrStopped
	}
	ev := ReportEvent{NetOK: netOK, EgressIP: egressIP, Drained: r.drained, At: time.Now()}
	if r.cfg.OnReport != nil {
		cb := r.cfg.OnReport
		go cb(ev)
	}
	if r.drained {
		r.event("api_report", "ip", egressIP, "net_ok", netOK, "ignored", true)
		return nil
	}
	r.netOK = netOK
	if egressIP != "" {
		r.wantIP = egressIP
		r.resumeIP = egressIP
	}
	r.event("api_report", "ip", egressIP, "net_ok", netOK, "ignored", false)
	r.reconcile(ctx, "report", r.cfg.OpTimeout)
	if errors.Is(r.lastErr, ErrNodeConflict) {
		return ErrNodeConflict
	}
	return nil
}

// Stop 撤销 lease 并关闭客户端。key 随 lease 立即消失。
func (r *Registrar) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	if !r.started {
		r.stopped = true
		r.mu.Unlock()
		return nil
	}
	if r.stopped {
		r.mu.Unlock()
		r.wg.Wait()
		return nil
	}
	r.stopped = true
	cancel := r.cancel
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	r.wg.Wait()
	r.mu.Lock()
	defer r.mu.Unlock()
	err := r.revokeCurrent(ctx, "stop", r.cfg.OpTimeout)
	r.revokeStale(ctx, r.cfg.OpTimeout)
	r.saveLocked()
	if r.cli != nil {
		cerr := r.cli.Close()
		r.cli = nil
		if err == nil {
			err = cerr
		}
	}
	r.event("exit")
	return err
}

// Status 返回内存中的状态。
func (r *Registrar) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Status{
		Key:      r.key,
		NodeID:   r.cfg.NodeID,
		LeaseID:  int64(r.lease),
		Addr:     r.cfg.IntranetAddr,
		EgressIP: r.regIP,
		Drained:  r.drained,
		ProbeOK:  r.probeOK,
		NetOK:    r.netOK,
		WantIP:   r.wantIP,
	}
}

// abandon 模拟进程被杀死：停止续租，不 Revoke、不 Delete。测试用来覆盖断电和 TTL。
func (r *Registrar) abandon() {
	r.mu.Lock()
	r.stopped = true
	cancel := r.cancel
	cli := r.cli
	r.cli = nil
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
	}
	if cli != nil {
		_ = cli.Close()
	}
}

func (r *Registrar) bootstrap(ctx context.Context) error {
	st, ok := loadState(r.cfg.StateFile, r.cfg.NodeID)
	if ok {
		stale := uniqueLeases(append([]int64{st.Lease}, st.PrevLeases...))
		lip := r.cfg.LocalIP()
		if !st.Drained && st.IP != "" && st.IP == lip {
			r.wantIP = st.IP
			r.resumeIP = st.IP
			r.stale = stale
			r.drained = false
			r.event("takeover_candidate", "ip", st.IP, "prev_leases", stale)
		} else {
			r.drained = st.Drained || (st.IP != "" && st.IP != lip)
			r.stale = stale
			r.revokeStale(ctx, r.cfg.OpTimeout)
			if r.drained {
				r.resumeIP = ""
			}
			r.event("stale_not_resumed", "drained", r.drained, "state_ip", st.IP, "local_ip", lip)
		}
	}
	if err := r.grant(ctx); err != nil {
		return err
	}
	r.saveLocked()
	return nil
}

func (r *Registrar) grant(ctx context.Context) error {
	var last error
	for {
		c, cancel := context.WithTimeout(ctx, r.cfg.OpTimeout)
		resp, err := r.cli.Grant(c, int64(r.cfg.TTL/time.Second))
		cancel()
		if err == nil {
			r.lease = resp.ID
			r.deadline = time.Now().Add(time.Duration(resp.TTL) * time.Second)
			r.nextRenew = time.Now().Add(r.cfg.RenewInterval)
			r.event("granted", "lease", int64(resp.ID), "ttl", resp.TTL)
			return nil
		}
		last = err
		r.event("grant_err", "err", err.Error())
		select {
		case <-ctx.Done():
			return fmt.Errorf("grant: %w", last)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (r *Registrar) controlLoop() {
	defer r.wg.Done()
	tick := time.NewTicker(100 * time.Millisecond)
	guard := time.NewTicker(r.cfg.IPGuardInterval)
	defer tick.Stop()
	defer guard.Stop()
	for {
		select {
		case <-r.root.Done():
			return
		case <-tick.C:
			r.renew()
			r.mu.Lock()
			if r.stopped {
				r.mu.Unlock()
				return
			}
			if r.lease == 0 && !r.drained && r.wantIP != "" && !time.Now().Before(r.retryNotBefore) {
				r.reconcile(r.root, "retry", r.cfg.BackgroundTimeout)
			}
			if r.drained && len(r.stale) > 0 {
				r.revokeStale(r.root, r.cfg.BackgroundTimeout)
			}
			r.mu.Unlock()
		case <-guard.C:
			if !r.mu.TryLock() {
				continue
			}
			if r.stopped {
				r.mu.Unlock()
				return
			}
			r.reconcile(r.root, "guard", r.cfg.BackgroundTimeout)
			r.mu.Unlock()
		}
	}
}

func (r *Registrar) probeLoop() {
	defer r.wg.Done()
	r.runProbe()
	tk := time.NewTicker(r.cfg.ProbeInterval)
	defer tk.Stop()
	for {
		select {
		case <-r.root.Done():
			return
		case <-tk.C:
			r.runProbe()
		}
	}
}

func (r *Registrar) runProbe() {
	err := probeGost(r.cfg.ProbeAddr, r.cfg.ProbeUser, r.cfg.ProbePassword, r.cfg.ProbeTarget, r.cfg.ProbeExpectStatus, r.cfg.ProbeTimeout)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return
	}
	if err != nil {
		r.fails++
		r.oks = 0
		if r.probeOK && r.fails >= r.cfg.ProbeFailThreshold {
			r.probeOK = false
			r.event("unhealthy", "err", err.Error(), "fails", r.fails)
			r.reconcile(r.root, "probe", r.cfg.BackgroundTimeout)
		}
		return
	}
	r.oks++
	r.fails = 0
	if !r.probeOK && r.oks >= r.cfg.ProbeRiseThreshold {
		r.probeOK = true
		r.event("healthy", "oks", r.oks)
		r.reconcile(r.root, "probe", r.cfg.BackgroundTimeout)
	}
}

func (r *Registrar) renew() {
	r.mu.Lock()
	if r.stopped || r.cli == nil || r.lease == 0 || time.Now().Before(r.nextRenew) {
		r.mu.Unlock()
		return
	}
	lease := r.lease
	cli := r.cli
	timeout := r.cfg.OpTimeout
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t0 := time.Now()
	resp, err := cli.KeepAliveOnce(ctx, lease)
	cancel()

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cli == nil || r.lease != lease {
		return
	}
	if err == nil && resp != nil && resp.TTL > 0 {
		r.deadline = time.Now().Add(time.Duration(resp.TTL) * time.Second)
		r.nextRenew = t0.Add(r.cfg.RenewInterval)
		r.event("ka_ok", "ttl", resp.TTL, "rtt_ms", time.Since(t0).Milliseconds())
		return
	}
	if leaseMissing(err) || (err == nil && (resp == nil || resp.TTL <= 0)) || time.Now().After(r.deadline) {
		r.event("lease_lost", "err", fmt.Sprint(err))
		r.lease = 0
		r.regIP = ""
		r.saveLocked()
		return
	}
	r.event("ka_err", "err", err.Error())
	r.nextRenew = time.Now().Add(500 * time.Millisecond)
}

func (r *Registrar) reconcile(ctx context.Context, trigger string, timeout time.Duration) {
	if r.stopped || r.cli == nil {
		return
	}
	lip := r.cfg.LocalIP()
	if r.regIP != "" && lip != r.regIP {
		r.event("ip_guard", "registered_ip", r.regIP, "local_ip", lip)
		r.drained = true
		r.resumeIP = ""
		r.saveLocked()
		if err := r.deleteOwned(ctx, "local_ip_changed", timeout); err != nil {
			r.lastErr = err
			r.event("unregister_err", "err", err.Error(), "reason", "local_ip_changed")
		} else {
			r.lastErr = nil
			r.saveLocked()
		}
	}
	healthy := r.probeOK && (r.cfg.IgnoreAdminNetCheck || r.netOK)
	want := !r.drained && healthy && r.wantIP != "" && r.wantIP == lip
	switch {
	case want && r.regIP != r.wantIP:
		if !time.Now().Before(r.retryNotBefore) || trigger == "ready" || trigger == "report" {
			if err := r.put(ctx, r.wantIP, trigger, timeout); err != nil {
				r.lastErr = err
				if errors.Is(err, ErrNodeConflict) {
					r.retryNotBefore = time.Now().Add(5 * time.Second)
				}
			} else {
				r.lastErr = nil
				r.retryNotBefore = time.Time{}
			}
		}
	case !want && r.regIP != "":
		reason := trigger
		if !r.probeOK {
			reason = "probe_fail"
		} else if !r.cfg.IgnoreAdminNetCheck && !r.netOK {
			reason = "net_check_fail"
		} else if r.drained {
			reason = "drained"
		}
		if err := r.deleteOwned(ctx, reason, timeout); err != nil {
			r.lastErr = err
			r.event("unregister_err", "err", err.Error(), "reason", reason)
		} else {
			r.lastErr = nil
			r.saveLocked()
		}
	}
}

func (r *Registrar) put(ctx context.Context, ip, how string, timeout time.Duration) error {
	if r.lease == 0 {
		gctx, cancel := context.WithTimeout(ctx, timeout)
		err := r.grant(gctx)
		cancel()
		if err != nil {
			return err
		}
	}
	val := Value{
		Addr:         r.cfg.IntranetAddr,
		EgressIP:     ip,
		ProxyPort:    r.cfg.ProxyPort,
		ControlPort:  r.cfg.ControlPort,
		NodeID:       r.cfg.NodeID,
		AZ:           r.cfg.AZ,
		Weight:       r.cfg.Weight,
		Capability:   r.cfg.Capability,
		Version:      r.cfg.Version,
		RegisteredAt: time.Now().UnixMilli(),
	}
	body, err := json.Marshal(val)
	if err != nil {
		return err
	}
	op := clientv3.OpPut(r.key, string(body), clientv3.WithLease(r.lease))
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		c, cancel := context.WithTimeout(ctx, timeout)
		tr, err := r.cli.Txn(c).
			If(clientv3.Compare(clientv3.CreateRevision(r.key), "=", 0)).
			Then(op).
			Else(clientv3.OpGet(r.key)).
			Commit()
		if err != nil {
			cancel()
			return err
		}
		if tr.Succeeded {
			cancel()
			r.finishPut(ip, how, "create")
			return nil
		}
		kvs := tr.Responses[0].GetResponseRange().Kvs
		if len(kvs) == 0 {
			cancel()
			last = errLostRace
			continue
		}
		kv := kvs[0]
		reason, allow, aerr := r.allowWrite(c, kv.Lease)
		if aerr != nil {
			cancel()
			return aerr
		}
		if !allow {
			cancel()
			r.alertConflict(kv.Lease)
			return ErrNodeConflict
		}
		tr2, err := r.cli.Txn(c).
			If(clientv3.Compare(clientv3.ModRevision(r.key), "=", kv.ModRevision)).
			Then(op).
			Commit()
		cancel()
		if err != nil {
			return err
		}
		if !tr2.Succeeded {
			last = errLostRace
			continue
		}
		r.finishPut(ip, how, reason)
		return nil
	}
	if last == nil {
		last = errLostRace
	}
	return last
}

func (r *Registrar) finishPut(ip, how, reason string) {
	old := r.regIP
	r.regIP = ip
	r.resumeIP = ip
	r.conflictAlerted = false
	r.event("registered", "how", how, "reason", reason, "ip", ip, "old_ip", old, "lease", int64(r.lease))
	r.revokeStale(context.Background(), r.cfg.OpTimeout)
	r.saveLocked()
}

func (r *Registrar) allowWrite(ctx context.Context, lease int64) (string, bool, error) {
	switch {
	case lease == int64(r.lease):
		return "own_lease", true, nil
	case leaseIn(r.stale, lease):
		return "stale_lease", true, nil
	case lease == 0:
		return "", false, nil
	}
	resp, err := r.cli.TimeToLive(ctx, clientv3.LeaseID(lease))
	if leaseMissing(err) {
		return "dead_lease", true, nil
	}
	if err != nil {
		return "", false, err
	}
	if resp.TTL <= 0 {
		return "dead_lease", true, nil
	}
	return "", false, nil
}

func (r *Registrar) deleteOwned(ctx context.Context, reason string, timeout time.Duration) error {
	if r.cli == nil {
		return errors.New("etcd client closed")
	}
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		c, cancel := context.WithTimeout(ctx, timeout)
		resp, err := r.cli.Get(c, r.key)
		if err != nil {
			cancel()
			return err
		}
		if len(resp.Kvs) == 0 {
			cancel()
			if r.regIP != "" {
				r.event("unregistered", "reason", reason, "ip", r.regIP, "missing", true)
			}
			r.regIP = ""
			return nil
		}
		kv := resp.Kvs[0]
		_, allow, aerr := r.allowWrite(c, kv.Lease)
		if aerr != nil {
			cancel()
			return aerr
		}
		if !allow {
			cancel()
			r.alertConflict(kv.Lease)
			return ErrNodeConflict
		}
		tr, err := r.cli.Txn(c).
			If(clientv3.Compare(clientv3.ModRevision(r.key), "=", kv.ModRevision)).
			Then(clientv3.OpDelete(r.key)).
			Commit()
		cancel()
		if err != nil {
			return err
		}
		if !tr.Succeeded {
			last = errLostRace
			continue
		}
		r.event("unregistered", "reason", reason, "ip", r.regIP, "lease", int64(r.lease))
		r.regIP = ""
		return nil
	}
	if last == nil {
		last = errLostRace
	}
	return last
}

func (r *Registrar) revokeCurrent(ctx context.Context, reason string, timeout time.Duration) error {
	if r.lease == 0 || r.cli == nil {
		r.regIP = ""
		return nil
	}
	c, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	_, err := r.cli.Revoke(c, r.lease)
	if leaseMissing(err) {
		err = nil
	}
	r.event("revoked", "reason", reason, "lease", int64(r.lease), "err", fmt.Sprint(err))
	if err != nil {
		return err
	}
	r.lease = 0
	r.regIP = ""
	return nil
}

func (r *Registrar) revokeStale(ctx context.Context, timeout time.Duration) {
	if r.cli == nil || len(r.stale) == 0 {
		return
	}
	ids := append([]int64(nil), r.stale...)
	var left []int64
	for _, id := range ids {
		if id == 0 || id == int64(r.lease) {
			continue
		}
		c, cancel := context.WithTimeout(ctx, timeout)
		_, err := r.cli.Revoke(c, clientv3.LeaseID(id))
		cancel()
		if err != nil && !leaseMissing(err) && ctx.Err() == nil {
			left = append(left, id)
			r.event("stale_revoke_err", "lease", id, "err", err.Error())
			continue
		}
		r.event("stale_lease_revoked", "lease", id)
	}
	r.stale = append([]int64(nil), left...)
}

func (r *Registrar) alertConflict(other int64) {
	if r.conflictAlerted && r.conflictLease == other {
		return
	}
	r.conflictAlerted = true
	r.conflictLease = other
	msg := fmt.Sprintf("nodeId 冲突：拒绝覆盖 %s，对方 lease %d 仍有效", r.key, other)
	r.event("conflict", "key", r.key, "other_lease", other, "msg", msg)
	if r.cfg.OnAlert == nil {
		return
	}
	a := Alert{Kind: AlertNodeConflict, NodeID: r.cfg.NodeID, Key: r.key, OtherLease: other, Message: msg}
	cb := r.cfg.OnAlert
	go cb(a)
}

func (r *Registrar) event(ev string, args ...any) {
	if r.cfg.Logger == nil {
		return
	}
	r.cfg.Logger.Event(ev, args...)
}

func leaseMissing(err error) bool {
	return errors.Is(err, rpctypes.ErrLeaseNotFound)
}

func leaseIn(ids []int64, id int64) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}

func uniqueLeases(ids []int64) []int64 {
	var out []int64
	seen := map[int64]bool{}
	for _, id := range ids {
		if id == 0 || seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	return out
}
