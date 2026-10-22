package registrar

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
	// ErrEgressUnchanged 表示计划内重拨后，外部看到的出口 IP 和重拨前相同。视为校验失败，不注册。
	ErrEgressUnchanged = errors.New("egress ip unchanged")
	// ErrEgressUnconfirmed 表示这次 Ready 处在重拨中，但没有可靠的重拨前出口可以比对。
	ErrEgressUnconfirmed = errors.New("previous egress unconfirmed")
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

	lease           clientv3.LeaseID
	epochLease      clientv3.LeaseID
	stale           []int64
	instanceID      string
	machineID       string
	regIP           string
	resumeIP        string
	prevEgress      string
	expectNewEgress bool
	stateUntrusted  bool
	drained         bool
	// drainedSince 是这次进入 drained 的时间。超过 MaxDrained 就不再续租重拨标记。
	drainedSince time.Time
	// epochHoldReleased 表示已经因为超过 MaxDrained 停止续租，不要再把标记写回去。
	epochHoldReleased bool
	// mayRegister 为 false 时，探测、地址守卫和 Report 都不能创建 key。只有接管本机原 key，或 Ready 通过出口校验之后，才置真。
	mayRegister bool
	// priorHolder 表示本进程登记过这个 nodeId（写成功过，或状态文件里的身份就是我们）。没拿过 key 的实例不能写 epoch 标记。
	priorHolder bool
	// recognizedEpoch 是本实例自己的旧标记原文（instance_id 和地址都对得上，字节和现在的正文不同）。注册成功后按这个字节删掉它。
	// epochGrant 仅测试注入 Grant 失败。生产为 nil。
	epochGrant      func(context.Context, int64) (*clientv3.LeaseGrantResponse, error)
	recognizedEpoch string
	// staleEpoch 是被换掉的上一条 epoch lease，写完新标记后撤销。
	staleEpoch  clientv3.LeaseID
	wantIP      string
	netOK       bool
	resourcesOK bool
	probeOK     bool
	fails       int
	oks         int

	deadline        time.Time
	nextRenew       time.Time
	epochDeadline   time.Time
	epochNextRenew  time.Time
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
	return &Registrar{cfg: cfg, key: Key(cfg.NodeID), netOK: true, resourcesOK: true}, nil
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
	if r.regIP != "" {
		r.prevEgress = r.regIP
	} else if r.prevEgress == "" && r.resumeIP != "" {
		r.prevEgress = r.resumeIP
	}
	r.expectNewEgress = true
	r.beginDrainedHold()
	r.mayRegister = false
	r.saveLocked()
	r.event("api_drain", "prev_egress", r.prevEgress)
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

// Ready 在出口校验通过后调用。prevEgress 是调用方看到的重拨前出口，用来和状态文件互相确认。
// 本机 ppp 地址必须等于 egressIP，且 gost 探测已通过，才会 Put。
func (r *Registrar) Ready(ctx context.Context, egressIP, prevEgress string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return ErrStopped
	}
	lip := r.cfg.LocalIP()
	r.event("api_ready", "ip", egressIP, "local_ip", lip, "prev_egress", r.prevEgress, "given_prev", prevEgress)
	if egressIP == "" || lip != egressIP {
		return fmt.Errorf("%w: local %q", ErrEgressMismatch, lip)
	}
	if err := r.checkEgressChange(egressIP, prevEgress); err != nil {
		return err
	}
	if r.prevEgress == "" && prevEgress != "" {
		r.prevEgress = prevEgress
	}
	r.stateUntrusted = false
	r.drained = false
	r.mayRegister = true
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

// checkEgressChange 在锁内调用。重拨后出口必须变化；对不上或无法确认时不注册。
func (r *Registrar) checkEgressChange(egressIP, prevEgress string) error {
	needConfirm := r.expectNewEgress || r.stateUntrusted
	baseline := r.prevEgress
	if baseline != "" && prevEgress != "" && prevEgress != baseline {
		return fmt.Errorf("%w: state %q caller %q", ErrEgressUnconfirmed, baseline, prevEgress)
	}
	if baseline == "" {
		baseline = prevEgress
	}
	if needConfirm && baseline == "" {
		return fmt.Errorf("%w: pass the egress from before this redial", ErrEgressUnconfirmed)
	}
	if baseline != "" && egressIP == baseline && (r.expectNewEgress || prevEgress != "" || r.stateUntrusted) {
		return fmt.Errorf("%w: %s", ErrEgressUnchanged, egressIP)
	}
	return nil
}

// Report 接收 admin 每分钟一次的巡检。外网不通，或（默认）CPU/内存异常，都删除 key；恢复后 Put。
// drained，或还没有一次成功的 Ready，都不注册。上报正文不写 etcd；只有注册状态需要变化时才动 key。
func (r *Registrar) Report(ctx context.Context, rep Report) error {
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return ErrStopped
	}
	ev := ReportEvent{
		NetFault: rep.NetFault, CPUFault: rep.CPUFault, MemFault: rep.MemFault,
		EgressIP: rep.EgressIP, Drained: r.drained, At: time.Now(),
	}
	if r.cfg.OnReport != nil {
		cb := r.cfg.OnReport
		go cb(ev)
	}
	// 还没经过一次成功的 Ready（或本机接管）时，Report 不能把旧出口写回去。
	// 状态文件缺失或损坏时 drained 从一开始就是 true，这里同样不会注册。
	if r.drained || !r.mayRegister {
		r.event("api_report", "ip", rep.EgressIP, "net_fault", rep.NetFault, "cpu_fault", rep.CPUFault, "mem_fault", rep.MemFault, "ignored", true)
		return nil
	}
	r.netOK = !rep.NetFault
	r.resourcesOK = !rep.CPUFault && !rep.MemFault
	if rep.EgressIP != "" {
		r.wantIP = rep.EgressIP
		r.resumeIP = rep.EgressIP
	}
	r.event("api_report", "ip", rep.EgressIP, "net_fault", rep.NetFault, "cpu_fault", rep.CPUFault, "mem_fault", rep.MemFault, "ignored", false)
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
	r.revokeEpoch(ctx)
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
		Key:         r.key,
		NodeID:      r.cfg.NodeID,
		LeaseID:     int64(r.lease),
		Addr:        r.cfg.IntranetAddr,
		EgressIP:    r.regIP,
		Drained:     r.drained,
		ProbeOK:     r.probeOK,
		NetOK:       r.netOK,
		ResourcesOK: r.resourcesOK,
		WantIP:      r.wantIP,
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
	r.machineID = r.cfg.currentMachineID()
	st, res := loadState(r.cfg.StateFile, r.cfg.NodeID)
	switch res {
	case stateMissing:
		// 文件缺失和第一次启动无法靠文件区分。先按 drained 处理：Report 不能注册，必须等 Ready。
		r.instanceID = newInstanceID()
		r.noteDrained()
		r.mayRegister = false
	case stateUntrusted:
		r.instanceID = newInstanceID()
		r.stateUntrusted = true
		r.noteDrained()
		r.mayRegister = false
		r.event("state_untrusted", "path", r.cfg.StateFile)
	case stateOK:
		if !sameMachine(st.MachineID, r.machineID) || st.InstanceID == "" {
			// machine-id 不同、有一边是空的，或 instance_id 是空的：身份不明，不接管、不 Revoke。
			r.instanceID = newInstanceID()
			r.noteDrained()
			r.mayRegister = false
			r.event("foreign_state", "state_machine", st.MachineID, "machine", r.machineID, "state_instance", st.InstanceID)
		} else {
			r.adoptState(ctx, st)
		}
	}
	if err := r.grant(ctx); err != nil {
		return err
	}
	r.saveLocked()
	return nil
}

func (r *Registrar) adoptState(ctx context.Context, st diskState) {
	if st.InstanceID == "" {
		r.instanceID = newInstanceID()
		r.noteDrained()
		r.mayRegister = false
		r.event("identity_unknown", "reason", "empty_instance")
		return
	}
	r.instanceID = st.InstanceID
	stale := uniqueLeases(append([]int64{st.Lease}, st.PrevLeases...))
	lip := r.cfg.LocalIP()
	r.prevEgress = st.PrevEgress
	r.expectNewEgress = st.ExpectNewEgress
	present, ours, live, err := r.keyOwnedHere(ctx, r.cfg.OpTimeout)
	if err != nil || (present && !ours) {
		// 当前 key 属于别的地址或实例，或身份字段是空的。绝不 Revoke。
		r.noteDrained()
		r.mayRegister = false
		r.stale = nil
		if err != nil {
			r.event("stale_revoke_skipped", "err", err.Error())
		} else {
			r.event("stale_revoke_skipped", "lease", live, "state_ip", st.IP, "local_ip", lip)
		}
		return
	}
	if !st.Drained && st.IP != "" && st.IP == lip {
		r.wantIP = st.IP
		r.resumeIP = st.IP
		if r.prevEgress == "" {
			r.prevEgress = st.IP
		}
		r.expectNewEgress = false
		r.stale = stale
		r.drained = false
		r.mayRegister = true
		r.priorHolder = true
		r.event("takeover_candidate", "ip", st.IP, "prev_leases", stale, "instance", r.instanceID)
		return
	}
	if st.Drained || (st.IP != "" && st.IP != lip) {
		r.noteDrained()
	} else {
		r.drained = false
	}
	r.mayRegister = false
	r.priorHolder = true
	r.stale = stale
	r.revokeStale(ctx, r.cfg.OpTimeout)
	if r.drained {
		r.resumeIP = ""
	}
	r.event("stale_not_resumed", "drained", r.drained, "state_ip", st.IP, "local_ip", lip, "expect_new_egress", r.expectNewEgress)
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
			if r.lease == 0 && !r.drained && r.mayRegister && r.wantIP != "" && !time.Now().Before(r.retryNotBefore) {
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
	r.renewLease(false)
	r.renewLease(true)
}

func (r *Registrar) renewLease(epoch bool) {
	r.mu.Lock()
	if r.stopped || r.cli == nil {
		r.mu.Unlock()
		return
	}
	var id clientv3.LeaseID
	var due, dead time.Time
	var renewEvery time.Duration
	if epoch {
		if r.epochLease != 0 && r.epochHoldExceeded() {
			r.releaseEpochHoldLocked()
		}
		id = r.epochLease
		due = r.epochNextRenew
		dead = r.epochDeadline
		renewEvery = r.cfg.EpochRenewInterval
	} else {
		id = r.lease
		due = r.nextRenew
		dead = r.deadline
		renewEvery = r.cfg.RenewInterval
	}
	if id == 0 || time.Now().Before(due) {
		r.mu.Unlock()
		return
	}
	cli := r.cli
	timeout := r.cfg.OpTimeout
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t0 := time.Now()
	resp, err := cli.KeepAliveOnce(ctx, id)
	cancel()

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cli == nil {
		return
	}
	if epoch {
		if r.epochLease != id {
			return
		}
		if err == nil && resp != nil && resp.TTL > 0 {
			r.epochDeadline = time.Now().Add(time.Duration(resp.TTL) * time.Second)
			r.epochNextRenew = t0.Add(renewEvery)
			return
		}
		if leaseMissing(err) || (err == nil && (resp == nil || resp.TTL <= 0)) || time.Now().After(dead) {
			r.event("epoch_lease_lost", "err", fmt.Sprint(err))
			r.staleEpoch = id
			r.epochLease = 0
			if werr := r.refreshEpoch(context.Background()); werr != nil {
				r.event("epoch_refresh_err", "err", werr.Error())
			}
			return
		}
		r.event("epoch_ka_err", "err", err.Error())
		r.epochNextRenew = time.Now().Add(500 * time.Millisecond)
		return
	}
	if r.lease != id {
		return
	}
	if err == nil && resp != nil && resp.TTL > 0 {
		r.deadline = time.Now().Add(time.Duration(resp.TTL) * time.Second)
		r.nextRenew = t0.Add(renewEvery)
		r.event("ka_ok", "ttl", resp.TTL, "rtt_ms", time.Since(t0).Milliseconds())
		return
	}
	if leaseMissing(err) || (err == nil && (resp == nil || resp.TTL <= 0)) || time.Now().After(dead) {
		// 客户端以为租约没了，但切主后服务端可能还活着。先记进 stale，后面才能认领，而不是和自己冲突。
		r.loseLeaseLocked(err)
		return
	}
	r.event("ka_err", "err", err.Error())
	r.nextRenew = time.Now().Add(500 * time.Millisecond)
}

func (r *Registrar) loseLeaseLocked(err error) {
	if r.lease != 0 {
		r.stale = uniqueLeases(append(r.stale, int64(r.lease)))
	}
	r.event("lease_lost", "err", fmt.Sprint(err), "kept_stale", r.stale)
	r.lease = 0
	r.regIP = ""
	r.saveLocked()
	// 只有登记过的持有者才补标记。没拿过 key 的实例在这里写，会把别人的空窗占住。
	if werr := r.refreshEpoch(context.Background()); werr != nil {
		r.event("epoch_refresh_err", "err", werr.Error())
	}
}

func (r *Registrar) resourceFault() bool {
	return r.cfg.ResourceFaultAction == ResourceFaultDeregister && !r.resourcesOK
}

func (r *Registrar) reconcile(ctx context.Context, trigger string, timeout time.Duration) {
	if r.stopped || r.cli == nil {
		return
	}
	lip := r.cfg.LocalIP()
	if r.regIP != "" && lip != r.regIP {
		r.event("ip_guard", "registered_ip", r.regIP, "local_ip", lip)
		r.beginDrainedHold()
		r.mayRegister = false
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
	healthy := r.probeOK && (r.cfg.IgnoreAdminNetCheck || r.netOK) && !r.resourceFault()
	want := !r.drained && r.mayRegister && healthy && r.wantIP != "" && r.wantIP == lip
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
		} else if r.resourceFault() {
			reason = "resource_check_fail"
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
	if !r.mayRegister {
		return fmt.Errorf("%w: registration is closed until Ready", ErrEgressUnconfirmed)
	}
	if err := r.checkEgressChange(ip, ""); err != nil {
		return err
	}
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
		InstanceID:   r.instanceID,
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
			Then(r.opCreateGuarded(string(body))).
			Else(clientv3.OpGet(r.key)).
			Commit()
		if err != nil {
			cancel()
			return err
		}
		if tr.Succeeded {
			wrote, ev, foreignLease, raw, kind := createResult(tr, r.instanceID, r.cfg.IntranetAddr, r.machineID, r.epochCanonical())
			if wrote {
				cancel()
				r.finishPut(ip, how, "create")
				return nil
			}
			if kind == epochRetry {
				cancel()
				last = errLostRace
				continue
			}
			if kind == epochSameOwner {
				r.event("epoch_same_owner", "addr", ev.Addr, "instance", ev.InstanceID)
				ok, perr := r.putIfEpochBytes(c, op, raw, 0)
				cancel()
				if perr != nil {
					return perr
				}
				if !ok {
					last = errLostRace
					continue
				}
				r.recognizedEpoch = raw
				r.finishPut(ip, how, "create")
				return nil
			}
			cancel()
			return r.rejectEpoch(kind, ev, foreignLease)
		}
		kvs := tr.Responses[0].GetResponseRange().Kvs
		if len(kvs) == 0 {
			cancel()
			last = errLostRace
			continue
		}
		kv := kvs[0]
		reason, allow, aerr := r.allowWrite(c, kv.Lease, kv.Value)
		if aerr != nil {
			cancel()
			return aerr
		}
		if !allow {
			cancel()
			r.alertDenied(kv.Lease, kv.Value, reason)
			return ErrNodeConflict
		}
		if reason == "dead_lease" {
			// 过期 lease 上的残留 key 也不能绕过重拨标记。
			tr2, err := r.cli.Txn(c).
				If(clientv3.Compare(clientv3.ModRevision(r.key), "=", kv.ModRevision)).
				Then(r.opCreateGuarded(string(body))).
				Commit()
			if err != nil {
				cancel()
				return err
			}
			if !tr2.Succeeded {
				cancel()
				last = errLostRace
				continue
			}
			wrote, ev, foreignLease, raw, kind := createResult(tr2, r.instanceID, r.cfg.IntranetAddr, r.machineID, r.epochCanonical())
			if wrote {
				cancel()
				r.finishPut(ip, how, reason)
				return nil
			}
			if kind == epochRetry {
				cancel()
				last = errLostRace
				continue
			}
			if kind == epochSameOwner {
				r.event("epoch_same_owner", "addr", ev.Addr, "instance", ev.InstanceID)
				ok, perr := r.putIfEpochBytes(c, op, raw, kv.ModRevision)
				cancel()
				if perr != nil {
					return perr
				}
				if !ok {
					last = errLostRace
					continue
				}
				r.recognizedEpoch = raw
				r.finishPut(ip, how, reason)
				return nil
			}
			cancel()
			return r.rejectEpoch(kind, ev, foreignLease)
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
	r.prevEgress = ip
	r.expectNewEgress = false
	r.conflictAlerted = false
	r.priorHolder = true
	r.drainedSince = time.Time{}
	r.epochHoldReleased = false
	r.event("registered", "how", how, "reason", reason, "ip", ip, "old_ip", old, "lease", int64(r.lease), "instance", r.instanceID)
	r.revokeStale(context.Background(), r.cfg.OpTimeout)
	r.clearEpoch(context.Background())
	r.revokeEpoch(context.Background())
	r.saveLocked()
}

func (r *Registrar) allowWrite(ctx context.Context, lease int64, raw []byte) (string, bool, error) {
	switch {
	case lease == int64(r.lease):
		return "own_lease", true, nil
	case leaseIn(r.stale, lease):
		addr, inst := valueIdentity(raw)
		if addr == "" || inst == "" || r.instanceID == "" || inst != r.instanceID || addr != r.cfg.IntranetAddr {
			r.stale = removeLease(r.stale, lease)
			r.saveLocked()
			return "foreign", false, nil
		}
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
			// 只有曾经拿过这个 key 的进程才补标记。Drain 本身把 drained 置上，不能当成持有证明。
			if r.priorHolder {
				if err := r.writeHolderEpoch(c, true); err != nil && !errors.Is(err, ErrNodeConflict) {
					cancel()
					if errors.Is(err, errLostRace) {
						last = err
						continue
					}
					return err
				}
			}
			cancel()
			if r.regIP != "" {
				r.event("unregistered", "reason", reason, "ip", r.regIP, "missing", true)
			}
			r.regIP = ""
			return nil
		}
		kv := resp.Kvs[0]
		why, allow, aerr := r.allowWrite(c, kv.Lease, kv.Value)
		if aerr != nil {
			cancel()
			return aerr
		}
		if !allow {
			cancel()
			r.alertDenied(kv.Lease, kv.Value, why)
			return ErrNodeConflict
		}
		// 标记和删除放进同一事务。别人先写下的标记不能挡住我们摘掉自己的 key。
		if err := r.deleteKeyAndEpoch(c, kv.ModRevision); err != nil {
			if errors.Is(err, errLostRace) {
				cancel()
				last = err
				continue
			}
			if derr := r.deleteAtRev(c, kv.ModRevision); derr != nil {
				cancel()
				if errors.Is(derr, errLostRace) {
					last = derr
					continue
				}
				return derr
			}
		}
		cancel()
		r.priorHolder = true
		r.event("unregistered", "reason", reason, "ip", r.regIP, "lease", int64(r.lease))
		r.regIP = ""
		return nil
	}
	if last == nil {
		last = errLostRace
	}
	return last
}

func (r *Registrar) deleteAtRev(ctx context.Context, mod int64) error {
	tr, err := r.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(r.key), "=", mod)).
		Then(clientv3.OpDelete(r.key)).
		Commit()
	if err != nil {
		return err
	}
	if !tr.Succeeded {
		return errLostRace
	}
	return nil
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
	present, ours, live, err := r.keyOwnedHere(ctx, timeout)
	if err != nil {
		r.event("stale_revoke_err", "err", err.Error())
		return
	}
	if present && !ours {
		r.stale = nil
		r.event("stale_revoke_skipped", "lease", live)
		return
	}
	ids := append([]int64(nil), r.stale...)
	var left []int64
	for _, id := range ids {
		if id == 0 || id == int64(r.lease) || id == int64(r.epochLease) {
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

func (r *Registrar) alertDenied(other int64, raw []byte, reason string) {
	if reason == "foreign" {
		r.alert(AlertForeignInstance, other, fmt.Sprintf("nodeId 冲突：%s 属于另一个实例 %s（lease %d），拒绝撤租或覆盖", r.key, instanceIDOf(raw), other))
		return
	}
	r.alert(AlertNodeConflict, other, fmt.Sprintf("nodeId 冲突：拒绝覆盖 %s，对方 lease %d 仍有效", r.key, other))
}

func (r *Registrar) alertForeign(other int64, inst string) {
	r.alert(AlertForeignInstance, other, fmt.Sprintf("nodeId 冲突：%s 的重拨标记属于另一个实例 %s（lease %d），拒绝在空窗里接管", r.key, inst, other))
}

func (r *Registrar) alert(kind string, other int64, msg string) {
	if r.conflictAlerted && r.conflictLease == other {
		return
	}
	r.conflictAlerted = true
	r.conflictLease = other
	r.event("conflict", "key", r.key, "kind", kind, "other_lease", other, "msg", msg)
	if r.cfg.OnAlert == nil {
		return
	}
	a := Alert{Kind: kind, NodeID: r.cfg.NodeID, Key: r.key, OtherLease: other, Message: msg}
	cb := r.cfg.OnAlert
	go cb(a)
}

// epochValue 的字节要稳定，事务才能用 Value(epoch)==mine 判断是不是自己的标记。不要往里写时间戳。
// addr 和 machine_id 只用来识别「同一台机器、状态文件丢了」：这种情况不报 foreign_instance，
// 但在标记过期之前仍然拒绝接管。能直接注册的只有 instance_id 对得上，并且 addr 为空（旧标记）或等于本机内网地址。
type epochValue struct {
	InstanceID string `json:"instance_id"`
	NodeID     string `json:"node_id"`
	Addr       string `json:"addr,omitempty"`
	MachineID  string `json:"machine_id,omitempty"`
}

func (ev epochValue) sameHost(addr, machine string) bool {
	return ev.Addr != "" && ev.MachineID != "" && addr != "" && machine != "" && ev.Addr == addr && ev.MachineID == machine
}

func (r *Registrar) epochCanonical() string {
	s, _ := r.epochBody()
	return s
}

func (r *Registrar) epochBody() (string, error) {
	b, err := json.Marshal(epochValue{
		InstanceID: r.instanceID,
		NodeID:     r.cfg.NodeID,
		Addr:       r.cfg.IntranetAddr,
		MachineID:  r.machineID,
	})
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// opCreateGuarded 在一次事务里创建业务 key：epoch 不存在，或内容就是本实例，才 Put。
// etcd 的 If 只能做与，所以「不存在或是我的」用两层事务表达。
func (r *Registrar) opCreateGuarded(body string) clientv3.Op {
	put := clientv3.OpPut(r.key, body, clientv3.WithLease(r.lease))
	epochKey := EpochKey(r.cfg.NodeID)
	mine, err := r.epochBody()
	if err != nil {
		mine = ""
	}
	whenMine := clientv3.OpTxn(
		[]clientv3.Cmp{clientv3.Compare(clientv3.Value(epochKey), "=", mine)},
		[]clientv3.Op{put},
		[]clientv3.Op{clientv3.OpGet(epochKey)},
	)
	return clientv3.OpTxn(
		[]clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(epochKey), "=", 0)},
		[]clientv3.Op{put},
		[]clientv3.Op{whenMine},
	)
}

func (r *Registrar) ensureEpochLease(ctx context.Context) error {
	if r.epochLease != 0 {
		return nil
	}
	c, cancel := context.WithTimeout(ctx, r.cfg.OpTimeout)
	defer cancel()
	resp, err := r.grantEpoch(c, int64(r.cfg.EpochTTL/time.Second))
	if err != nil {
		return err
	}
	r.epochLease = resp.ID
	r.epochDeadline = time.Now().Add(time.Duration(resp.TTL) * time.Second)
	r.epochNextRenew = time.Now().Add(r.cfg.EpochRenewInterval)
	r.event("epoch_granted", "lease", int64(resp.ID), "ttl", resp.TTL)
	return nil
}

func (r *Registrar) noteDrained() {
	r.drained = true
	if r.drainedSince.IsZero() {
		r.drainedSince = time.Now()
	}
}

// beginDrainedHold 从这一次 Drain 或地址守卫重新计算标记还能续多久。
func (r *Registrar) beginDrainedHold() {
	r.drained = true
	r.drainedSince = time.Now()
	r.epochHoldReleased = false
}

func (r *Registrar) epochHoldExceeded() bool {
	if r.epochHoldReleased || r.cfg.MaxDrained <= 0 || r.drainedSince.IsZero() {
		return false
	}
	if !r.drained && !r.expectNewEgress {
		return false
	}
	return !time.Now().Before(r.drainedSince.Add(r.cfg.MaxDrained))
}

func (r *Registrar) releaseEpochHoldLocked() {
	if r.epochHoldReleased {
		return
	}
	r.epochHoldReleased = true
	// 不 Revoke：让服务端上的标记按剩余 TTL 自己过期，最多再留一个 EpochTTL。
	r.epochLease = 0
	r.event("epoch_hold_expired", "max", r.cfg.MaxDrained.String())
	if r.cfg.OnAlert == nil {
		return
	}
	a := Alert{
		Kind:    AlertEpochHoldExpired,
		NodeID:  r.cfg.NodeID,
		Key:     EpochKey(r.cfg.NodeID),
		Message: fmt.Sprintf("drained 已超过 %s，停止续租重拨标记，标记将在一个 EpochTTL 内过期", r.cfg.MaxDrained),
	}
	cb := r.cfg.OnAlert
	go cb(a)
}

func (r *Registrar) grantEpoch(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	if r.epochGrant != nil {
		return r.epochGrant(ctx, ttl)
	}
	return r.cli.Grant(ctx, ttl)
}

func (r *Registrar) rejectEpoch(kind int, ev epochValue, foreignLease int64) error {
	if kind == epochSameHost {
		// 同机同内网地址只压住 foreign_instance。标记还在就不能接管，换了出口也不行。
		r.event("epoch_same_host", "addr", ev.Addr, "instance", ev.InstanceID, "blocked", true)
		return fmt.Errorf("%w: same-host epoch still held by %s", ErrNodeConflict, ev.InstanceID)
	}
	r.alertForeign(foreignLease, ev.InstanceID)
	return ErrNodeConflict
}

func leaseRejected(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, rpctypes.ErrLeaseNotFound) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "requested lease not found") || strings.Contains(msg, "lease not found")
}

// refreshEpoch 在 drained 期间把已有标记续到新的 epoch lease 上。
// 业务 key 若属于别人，或者本进程从来没拿过这个 nodeId，就什么都不写。
func (r *Registrar) refreshEpoch(ctx context.Context) error {
	if r.epochHoldReleased {
		return nil
	}
	if r.epochHoldExceeded() {
		r.releaseEpochHoldLocked()
		return nil
	}
	if !r.priorHolder || (!r.drained && !r.expectNewEgress) {
		return nil
	}
	present, ours, _, err := r.keyOwnedHere(ctx, r.cfg.OpTimeout)
	if err != nil {
		return err
	}
	if present && !ours {
		return nil
	}
	return r.writeHolderEpoch(ctx, !present)
}

// writeHolderEpoch 只给登记过的持有者补标记。requireMainAbsent 时，业务 key 一旦出现就不写。
// 已有标记只有内容就是本实例才刷新，并且会撤销被换掉的旧 epoch lease。
func (r *Registrar) writeHolderEpoch(ctx context.Context, requireMainAbsent bool) error {
	if !r.priorHolder || r.cli == nil || r.instanceID == "" {
		return nil
	}
	prev := r.epochLease
	if err := r.ensureEpochLease(ctx); err != nil {
		return err
	}
	body, err := r.epochBody()
	if err != nil {
		return err
	}
	epochKey := EpochKey(r.cfg.NodeID)
	put := clientv3.OpPut(epochKey, body, clientv3.WithLease(r.epochLease))
	c, cancel := context.WithTimeout(ctx, r.cfg.OpTimeout)
	defer cancel()

	var tr *clientv3.TxnResponse
	if requireMainAbsent {
		tr, err = r.cli.Txn(c).
			If(
				clientv3.Compare(clientv3.CreateRevision(r.key), "=", 0),
				clientv3.Compare(clientv3.CreateRevision(epochKey), "=", 0),
			).
			Then(put).
			Else(clientv3.OpTxn(
				[]clientv3.Cmp{
					clientv3.Compare(clientv3.CreateRevision(r.key), "=", 0),
					clientv3.Compare(clientv3.Value(epochKey), "=", body),
				},
				[]clientv3.Op{clientv3.OpGet(epochKey), put},
				[]clientv3.Op{clientv3.OpGet(r.key), clientv3.OpGet(epochKey)},
			)).
			Commit()
	} else {
		tr, err = r.cli.Txn(c).
			If(clientv3.Compare(clientv3.CreateRevision(epochKey), "=", 0)).
			Then(put).
			Else(clientv3.OpTxn(
				[]clientv3.Cmp{clientv3.Compare(clientv3.Value(epochKey), "=", body)},
				[]clientv3.Op{clientv3.OpGet(epochKey), put},
				[]clientv3.Op{clientv3.OpGet(epochKey)},
			)).
			Commit()
	}
	if err != nil {
		return err
	}
	if tr.Succeeded {
		r.revokeStaleEpoch(ctx)
		return nil
	}
	inner := nestedTxn(tr)
	if inner != nil && inner.Succeeded {
		r.revokeObservedEpoch(ctx, inner)
		r.revokeStaleEpoch(ctx)
		return nil
	}
	if requireMainAbsent && mainKeyPresent(inner) {
		if prev == 0 {
			r.revokeEpoch(ctx)
		}
		return errLostRace
	}
	ev, _, _, empty := epochFromTxn(inner, requireMainAbsent)
	if empty || ev.InstanceID == r.instanceID {
		// 这次 Grant 还没挂上标记。冲突或重试之前先撤掉，避免空 lease 一直被续租。
		if prev == 0 {
			r.revokeEpoch(ctx)
		}
		return errLostRace
	}
	if prev == 0 {
		r.revokeEpoch(ctx)
	}
	return ErrNodeConflict
}

// deleteKeyAndEpoch 在一次事务里确认业务 key 还是我们刚读到的那一版，写上本实例的标记，并删除 key。
// 标记原先属于谁都无所谓：拿着当前 key 的才是持有者。
// 客户端还记着的 epoch lease 如果已经被服务端撤销，整次事务会失败。这时重新 Grant 再试一次，
// 仍然失败才把错误交给调用方，由它退回只删 key。刚 Grant、又没写上标记的 lease 会撤销，避免空租约一直续。
func (r *Registrar) deleteKeyAndEpoch(ctx context.Context, mod int64) error {
	var last error
	for attempt := 0; attempt < 2; attempt++ {
		fresh := r.epochLease == 0
		if err := r.ensureEpochLease(ctx); err != nil {
			return err
		}
		body, err := r.epochBody()
		if err != nil {
			if fresh {
				r.revokeEpoch(ctx)
			}
			return err
		}
		epochKey := EpochKey(r.cfg.NodeID)
		tr, err := r.cli.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(r.key), "=", mod)).
			Then(
				clientv3.OpGet(epochKey),
				clientv3.OpPut(epochKey, body, clientv3.WithLease(r.epochLease)),
				clientv3.OpDelete(r.key),
			).
			Commit()
		if leaseRejected(err) && attempt == 0 {
			r.epochLease = 0
			last = err
			continue
		}
		if err != nil {
			if fresh {
				r.revokeEpoch(ctx)
			}
			return err
		}
		if !tr.Succeeded {
			if fresh {
				r.revokeEpoch(ctx)
			}
			return errLostRace
		}
		if len(tr.Responses) > 0 {
			if rr := tr.Responses[0].GetResponseRange(); rr != nil && len(rr.Kvs) == 1 {
				old := clientv3.LeaseID(rr.Kvs[0].Lease)
				if old != 0 && old != r.epochLease && old != r.lease {
					r.revokeLeaseID(ctx, old)
				}
			}
		}
		r.revokeStaleEpoch(ctx)
		return nil
	}
	if last == nil {
		last = errors.New("epoch lease rejected")
	}
	return last
}

func (r *Registrar) putIfEpochBytes(ctx context.Context, op clientv3.Op, raw string, mod int64) (bool, error) {
	cmps := []clientv3.Cmp{clientv3.Compare(clientv3.Value(EpochKey(r.cfg.NodeID)), "=", raw)}
	if mod > 0 {
		cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(r.key), "=", mod))
	} else {
		cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(r.key), "=", 0))
	}
	tr, err := r.cli.Txn(ctx).If(cmps...).Then(op).Commit()
	if err != nil {
		return false, err
	}
	return tr.Succeeded, nil
}

// clearEpoch 只删除本实例的标记，或同一台机器上已认出的残留标记。
func (r *Registrar) clearEpoch(ctx context.Context) {
	if r.cli == nil || r.instanceID == "" {
		return
	}
	bodies := []string{}
	if body, err := r.epochBody(); err == nil && body != "" {
		bodies = append(bodies, body)
	} else if err != nil {
		r.event("epoch_delete_err", "err", err.Error())
	}
	if r.recognizedEpoch != "" && r.recognizedEpoch != r.epochCanonical() {
		bodies = append(bodies, r.recognizedEpoch)
	}
	for _, body := range bodies {
		c, cancel := context.WithTimeout(ctx, r.cfg.OpTimeout)
		_, err := r.cli.Txn(c).
			If(clientv3.Compare(clientv3.Value(EpochKey(r.cfg.NodeID)), "=", body)).
			Then(clientv3.OpDelete(EpochKey(r.cfg.NodeID))).
			Commit()
		cancel()
		if err != nil {
			r.event("epoch_delete_err", "err", err.Error())
		}
	}
}

func (r *Registrar) revokeEpoch(ctx context.Context) {
	if r.epochLease == 0 || r.cli == nil {
		return
	}
	id := r.epochLease
	r.revokeLeaseID(ctx, id)
	if r.epochLease == id {
		r.epochLease = 0
	}
}

func (r *Registrar) revokeStaleEpoch(ctx context.Context) {
	if r.staleEpoch == 0 {
		return
	}
	id := r.staleEpoch
	r.staleEpoch = 0
	r.revokeLeaseID(ctx, id)
}

func (r *Registrar) revokeObservedEpoch(ctx context.Context, tr *clientv3.TxnResponse) {
	if tr == nil || len(tr.Responses) == 0 {
		return
	}
	rr := tr.Responses[0].GetResponseRange()
	if rr == nil || len(rr.Kvs) != 1 {
		return
	}
	old := clientv3.LeaseID(rr.Kvs[0].Lease)
	if old != 0 && old != r.epochLease && old != r.lease {
		r.revokeLeaseID(ctx, old)
	}
}

func (r *Registrar) revokeLeaseID(ctx context.Context, id clientv3.LeaseID) {
	if id == 0 || r.cli == nil || id == r.lease {
		return
	}
	c, cancel := context.WithTimeout(ctx, r.cfg.OpTimeout)
	defer cancel()
	_, err := r.cli.Revoke(c, id)
	if err != nil && !leaseMissing(err) {
		r.event("epoch_revoke_err", "lease", int64(id), "err", err.Error())
	}
}

// keyOwnedHere 读当前业务 key。地址或 instance_id 有一边是空的，或者和本机不一致，就不是我们的。
func (r *Registrar) keyOwnedHere(ctx context.Context, timeout time.Duration) (present, ours bool, lease int64, err error) {
	if r.cli == nil {
		return false, false, 0, errors.New("etcd client closed")
	}
	c, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	resp, err := r.cli.Get(c, r.key)
	if err != nil {
		return false, false, 0, err
	}
	if len(resp.Kvs) == 0 {
		return false, false, 0, nil
	}
	kv := resp.Kvs[0]
	addr, inst := valueIdentity(kv.Value)
	ours = addr != "" && inst != "" && r.instanceID != "" && addr == r.cfg.IntranetAddr && inst == r.instanceID
	return true, ours, kv.Lease, nil
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

func instanceIDOf(raw []byte) string {
	_, inst := valueIdentity(raw)
	return inst
}

func valueIdentity(raw []byte) (addr, inst string) {
	var v Value
	if json.Unmarshal(raw, &v) != nil {
		return "", ""
	}
	return v.Addr, v.InstanceID
}

func sameMachine(stateID, currentID string) bool {
	return stateID != "" && currentID != "" && stateID == currentID
}

func nestedTxn(tr *clientv3.TxnResponse) *clientv3.TxnResponse {
	if tr == nil || len(tr.Responses) == 0 {
		return nil
	}
	tv := tr.Responses[0].GetResponseTxn()
	if tv == nil {
		return nil
	}
	return (*clientv3.TxnResponse)(tv)
}

const (
	epochWrote = iota
	epochRetry
	epochForeign
	epochSameOwner
	epochSameHost
)

// createResult 解读内层 epoch 事务。wrote 表示业务 key 已经写入。
func createResult(tr *clientv3.TxnResponse, self, addr, machine, canonical string) (wrote bool, ev epochValue, lease int64, raw string, kind int) {
	epochTxn := nestedTxn(tr)
	if epochTxn == nil {
		return false, epochValue{}, 0, "", epochRetry
	}
	if epochTxn.Succeeded {
		return true, epochValue{}, 0, "", epochWrote
	}
	mineTxn := nestedTxn(epochTxn)
	if mineTxn == nil {
		return false, epochValue{}, 0, "", epochRetry
	}
	if mineTxn.Succeeded {
		return true, epochValue{}, 0, "", epochWrote
	}
	ev, raw, lease, empty := epochGet(mineTxn)
	if empty {
		return false, ev, 0, "", epochRetry
	}
	if epochOwned(ev, self, addr) {
		if raw == canonical {
			return false, ev, lease, raw, epochRetry
		}
		return false, ev, lease, raw, epochSameOwner
	}
	if ev.sameHost(addr, machine) {
		return false, ev, lease, raw, epochSameHost
	}
	return false, ev, lease, raw, epochForeign
}

// epochOwned 只认本实例。旧标记没有 addr 时只比 instance_id；新标记还必须 addr 一致。
// 拷走状态文件、内网地址已经不同的机器不能靠同一个 instance_id 接管。
func epochOwned(ev epochValue, self, addr string) bool {
	if ev.InstanceID == "" || self == "" || ev.InstanceID != self {
		return false
	}
	if ev.Addr == "" {
		return true
	}
	return addr != "" && ev.Addr == addr
}

func epochGet(tr *clientv3.TxnResponse) (ev epochValue, raw string, lease int64, empty bool) {
	if tr == nil || len(tr.Responses) == 0 {
		return epochValue{}, "", 0, true
	}
	rr := tr.Responses[0].GetResponseRange()
	if rr == nil || len(rr.Kvs) == 0 {
		return epochValue{}, "", 0, true
	}
	raw = string(rr.Kvs[0].Value)
	if json.Unmarshal(rr.Kvs[0].Value, &ev) != nil {
		return epochValue{}, raw, rr.Kvs[0].Lease, false
	}
	return ev, raw, rr.Kvs[0].Lease, false
}

func epochFromTxn(tr *clientv3.TxnResponse, requireMainAbsent bool) (epochValue, string, int64, bool) {
	if tr == nil {
		return epochValue{}, "", 0, true
	}
	if requireMainAbsent && len(tr.Responses) > 1 {
		return epochGet(&clientv3.TxnResponse{Responses: tr.Responses[1:]})
	}
	return epochGet(tr)
}

func mainKeyPresent(tr *clientv3.TxnResponse) bool {
	if tr == nil || len(tr.Responses) == 0 {
		return false
	}
	rr := tr.Responses[0].GetResponseRange()
	return rr != nil && len(rr.Kvs) > 0
}

func removeLease(ids []int64, id int64) []int64 {
	var out []int64
	for _, v := range ids {
		if v != id {
			out = append(out, v)
		}
	}
	return out
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
