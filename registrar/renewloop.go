package registrar

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"math/big"
	mrand "math/rand"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// leaseFail 是续租循环发现某条 lease 可能保不住。gen 用来丢掉「失败之后又续上了」的过期通知。
type leaseFail struct {
	id  int64
	err error
	gen int64
}

// commitTxn 在锁外提交已经组好的事务。组事务本身不访问网络，要在持锁时做完。
func (r *Registrar) commitTxn(txn clientv3.Txn) (*clientv3.TxnResponse, error) {
	if txn == nil {
		return nil, errors.New("etcd client closed")
	}
	var tr *clientv3.TxnResponse
	var err error
	r.do(func() {
		tr, err = txn.Commit()
	})
	if err == nil && tr == nil {
		err = errors.New("empty txn")
	}
	return tr, err
}

func (r *Registrar) txn(ctx context.Context) clientv3.Txn {
	if r.cli == nil {
		return nil
	}
	return r.cli.Txn(ctx)
}

// 不能嵌套。续租循环不走这里，也不拿 r.mu。
func (r *Registrar) do(fn func()) {
	r.mu.Unlock()
	defer r.mu.Lock()
	fn()
}

func (r *Registrar) bindBusinessLocked(id clientv3.LeaseID, ttlSec int64) {
	now := time.Now()
	r.lease = id
	r.deadline = now.Add(time.Duration(ttlSec) * time.Second)
	r.nextRenew = now.Add(r.cfg.RenewInterval)
	r.pubLease.Store(int64(id))
	r.renewMu.Lock()
	r.bizDead = r.deadline
	r.bizDue = r.nextRenew
	r.renewMu.Unlock()
}

func (r *Registrar) clearBusinessLocked() {
	r.lease = 0
	r.deadline = time.Time{}
	r.nextRenew = time.Time{}
	r.pubLease.Store(0)
	r.renewMu.Lock()
	r.bizDue = time.Time{}
	r.bizDead = time.Time{}
	r.renewMu.Unlock()
}

func (r *Registrar) bindEpochLocked(id clientv3.LeaseID, ttlSec int64) {
	now := time.Now()
	r.epochLease = id
	r.epochDeadline = now.Add(time.Duration(ttlSec) * time.Second)
	r.epochNextRenew = now.Add(r.cfg.EpochRenewInterval)
	r.pubEpoch.Store(int64(id))
	r.renewMu.Lock()
	r.epDead = r.epochDeadline
	r.epDue = r.epochNextRenew
	r.renewMu.Unlock()
}

func (r *Registrar) clearEpochLeaseLocked() {
	r.epochLease = 0
	r.epochDeadline = time.Time{}
	r.epochNextRenew = time.Time{}
	r.pubEpoch.Store(0)
	r.renewMu.Lock()
	r.epDue = time.Time{}
	r.epDead = time.Time{}
	r.renewMu.Unlock()
}

// publishHoldLocked 把 drained 是否还在续租标记的事实交给续租循环。续租循环不拿 r.mu。
func (r *Registrar) publishHoldLocked() {
	if r.epochHoldReleased || r.drainedSince.IsZero() || (!r.drained && !r.expectNewEgress) {
		r.holdActive.Store(false)
		return
	}
	r.holdSince.Store(r.drainedSince.UnixNano())
	r.holdActive.Store(true)
}

func (r *Registrar) renewLoop() {
	defer r.wg.Done()
	tk := time.NewTicker(50 * time.Millisecond)
	defer tk.Stop()
	for {
		select {
		case <-r.root.Done():
			return
		case <-tk.C:
			r.renewIndependent(false)
			if r.root.Err() != nil {
				return
			}
			r.renewIndependent(true)
		}
	}
}

func (r *Registrar) renewIndependent(epoch bool) {
	var id int64
	var every time.Duration
	if epoch {
		id = r.pubEpoch.Load()
		every = r.cfg.EpochRenewInterval
		if id != 0 && r.holdActive.Load() {
			since := r.holdSince.Load()
			if since > 0 && !time.Now().Before(time.Unix(0, since).Add(r.cfg.MaxDrained)) {
				if r.pubEpoch.CompareAndSwap(id, 0) {
					r.queueHold(id)
				}
				return
			}
		}
	} else {
		id = r.pubLease.Load()
		every = r.cfg.RenewInterval
	}
	if id == 0 || every <= 0 {
		return
	}
	now := time.Now()
	r.renewMu.Lock()
	var due, dead time.Time
	if epoch {
		due, dead = r.epDue, r.epDead
	} else {
		due, dead = r.bizDue, r.bizDead
	}
	if !now.Before(due) {
		// 一次 KeepAliveOnce 没回来之前，不要在每个 tick 上再叠一次。
		pushed := now.Add(r.cfg.OpTimeout)
		if epoch {
			r.epDue = pushed
		} else {
			r.bizDue = pushed
		}
	}
	r.renewMu.Unlock()
	if now.Before(due) {
		return
	}
	cli := r.cliPtr.Load()
	if cli == nil {
		return
	}
	parent := r.root
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, r.cfg.OpTimeout)
	t0 := time.Now()
	resp, err := cli.KeepAliveOnce(ctx, clientv3.LeaseID(id))
	cancel()
	if r.pubID(epoch) != id {
		return
	}
	if err == nil && resp != nil && resp.TTL > 0 {
		deadline := time.Now().Add(time.Duration(resp.TTL) * time.Second)
		next := t0.Add(every)
		r.renewMu.Lock()
		if r.pubID(epoch) != id {
			r.renewMu.Unlock()
			return
		}
		if epoch {
			r.epDead = deadline
			r.epDue = next
			r.epGen.Add(1)
		} else {
			r.bizDead = deadline
			r.bizDue = next
			r.bizGen.Add(1)
			r.renewStamps = append(r.renewStamps, time.Now())
			if len(r.renewStamps) > 128 {
				r.renewStamps = append([]time.Time(nil), r.renewStamps[len(r.renewStamps)-128:]...)
			}
			r.lastRenew.Store(time.Now().UnixNano())
		}
		r.renewMu.Unlock()
		r.dropPending(epoch, id)
		if !epoch {
			r.event("ka_ok", "ttl", resp.TTL, "rtt_ms", time.Since(t0).Milliseconds())
		}
		return
	}
	lost := leaseMissing(err) || (err == nil && (resp == nil || resp.TTL <= 0)) || time.Now().After(dead)
	if lost {
		// 不把 pub id 清掉。Drain 可能正占着 opMu，处理函数要等它结束；
		// 这段时间里必须继续 KeepAliveOnce，否则一次超时就会把还活着的 lease 饿死。
		r.renewMu.Lock()
		var gen int64
		if r.pubID(epoch) == id {
			retry := time.Now().Add(200 * time.Millisecond)
			if epoch {
				r.epDue = retry
				gen = r.epGen.Load()
			} else {
				r.bizDue = retry
				gen = r.bizGen.Load()
			}
		} else {
			r.renewMu.Unlock()
			return
		}
		r.renewMu.Unlock()
		note := leaseFail{id: id, err: err, gen: gen}
		if epoch {
			r.queueEpoch(note)
		} else {
			r.queueMain(note)
		}
		return
	}
	var msg string
	if err != nil {
		msg = err.Error()
	}
	retry := time.Now().Add(200 * time.Millisecond)
	var emit bool
	r.renewMu.Lock()
	if r.pubID(epoch) == id {
		emit = true
		if epoch {
			r.epDue = retry
		} else {
			r.bizDue = retry
		}
	}
	r.renewMu.Unlock()
	if !emit {
		return
	}
	if epoch {
		r.event("epoch_ka_err", "err", msg)
		return
	}
	r.event("ka_err", "err", msg)
}

func (r *Registrar) pubID(epoch bool) int64 {
	if epoch {
		return r.pubEpoch.Load()
	}
	return r.pubLease.Load()
}

func (r *Registrar) dropPending(epoch bool, id int64) {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()
	if epoch {
		if r.pendingEpoch != nil && r.pendingEpoch.id == id {
			r.pendingEpoch = nil
		}
		return
	}
	if r.pendingMain != nil && r.pendingMain.id == id {
		r.pendingMain = nil
	}
}

func (r *Registrar) wakeLoss() {
	if r.lossWake == nil {
		return
	}
	select {
	case r.lossWake <- struct{}{}:
	default:
	}
}

func (r *Registrar) queueMain(f leaseFail) {
	r.pendingMu.Lock()
	cp := f
	r.pendingMain = &cp
	r.pendingMu.Unlock()
	r.wakeLoss()
}

func (r *Registrar) queueEpoch(f leaseFail) {
	r.pendingMu.Lock()
	cp := f
	r.pendingEpoch = &cp
	r.pendingMu.Unlock()
	r.wakeLoss()
}

func (r *Registrar) queueHold(id int64) {
	r.pendingMu.Lock()
	r.pendingHold = id
	r.pendingMu.Unlock()
	r.wakeLoss()
}

func (r *Registrar) hasSignals() bool {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()
	return r.pendingMain != nil || r.pendingEpoch != nil || r.pendingHold != 0
}

// drainSignals 在控制循环里处理续租失败。拿不到 opMu 就留到下一轮，不挡住续租循环。
func (r *Registrar) drainSignals() {
	if !r.hasSignals() {
		return
	}
	if !r.opMu.TryLock() {
		return
	}
	defer r.opMu.Unlock()
	r.pendingMu.Lock()
	main := r.pendingMain
	epoch := r.pendingEpoch
	hold := r.pendingHold
	r.pendingMain = nil
	r.pendingEpoch = nil
	r.pendingHold = 0
	r.pendingMu.Unlock()
	if main == nil && epoch == nil && hold == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return
	}
	if main != nil && r.lossStillCurrent(false, main) {
		r.loseLeaseLocked(main.err)
	}
	if hold != 0 && int64(r.epochLease) == hold && !r.epochHoldReleased {
		r.releaseEpochHoldLocked()
	}
	if epoch != nil && r.lossStillCurrent(true, epoch) {
		r.event("epoch_lease_lost", "err", errString(epoch.err))
		r.staleEpoch = clientv3.LeaseID(epoch.id)
		r.clearEpochLeaseLocked()
		if werr := r.refreshEpoch(context.Background()); werr != nil {
			r.event("epoch_refresh_err", "err", werr.Error())
		}
	}
}

// lossStillCurrent 在 r.mu 下调用。续租已经又成功过，或者 lease 已经换了，就不要再判丢失。
func (r *Registrar) lossStillCurrent(epoch bool, note *leaseFail) bool {
	if note == nil {
		return false
	}
	r.renewMu.Lock()
	defer r.renewMu.Unlock()
	if epoch {
		if int64(r.epochLease) != note.id || r.epGen.Load() != note.gen {
			return false
		}
		r.epGen.Add(1)
		return true
	}
	if int64(r.lease) != note.id || r.bizGen.Load() != note.gen {
		return false
	}
	r.bizGen.Add(1)
	return true
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// RenewTimes 返回本进程业务 lease 最近若干次续租成功的时间，测试用来量间隔。
func (r *Registrar) RenewTimes() []time.Time {
	r.renewMu.Lock()
	defer r.renewMu.Unlock()
	return append([]time.Time(nil), r.renewStamps...)
}

// LastRenew 是最近一次业务续租成功的时间。零值表示还没有成功过。
func (r *Registrar) LastRenew() time.Time {
	ns := r.lastRenew.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// JitterBeforeDrain 在计划内批量重拨之前睡眠一段 [0, RedialJitter) 的随机时间。
// RedialJitter 为 0 时立即返回。调度对齐的重拨必须先调用它，不能在同一时刻一起 Drain。
func (r *Registrar) JitterBeforeDrain(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	spread := r.cfg.RedialJitter
	if spread <= 0 {
		return nil
	}
	n, err := cryptorand.Int(cryptorand.Reader, big.NewInt(int64(spread)))
	var d time.Duration
	if err != nil {
		d = spread / 2
	} else {
		d = time.Duration(n.Int64())
	}
	return sleepCtx(ctx, d)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// drainBackoff 是失败后的等待，落在 [base/2, base)，并封顶由调用方控制。
func drainBackoff(base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	span := int64(base / 2)
	if span <= 0 {
		return base
	}
	return base/2 + time.Duration(mrand.Int63n(span))
}

// epochWriteLanded 在事务超时时确认标记是不是已经写上了我们刚 Grant 的 lease。
// checked 为 false 表示读失败，调用方不能据此 Revoke。
func (r *Registrar) epochWriteLanded(ctx context.Context, body string, id clientv3.LeaseID) (landed, checked bool) {
	cli := r.cli
	if cli == nil || id == 0 || body == "" {
		return false, false
	}
	key := EpochKey(r.cfg.NodeID)
	var resp *clientv3.GetResponse
	var err error
	r.do(func() {
		c, cancel := context.WithTimeout(ctx, r.cfg.OpTimeout)
		resp, err = cli.Get(c, key)
		cancel()
	})
	if err != nil || resp == nil {
		return false, false
	}
	if len(resp.Kvs) != 1 {
		return false, true
	}
	kv := resp.Kvs[0]
	return string(kv.Value) == body && clientv3.LeaseID(kv.Lease) == id, true
}
