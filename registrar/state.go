package registrar

import (
	"encoding/json"
	"os"
	"path/filepath"
)

const (
	stateMissing = iota
	stateOK
	stateUntrusted
)

// diskState 落在 StateFile。drained 必须在 Delete 之前写下去，崩溃后才不会把旧出口又注册上。
// instance_id 和 machine_id 用来识别状态文件是不是从别的虚拟机拷过来的。
type diskState struct {
	NodeID          string  `json:"node_id"`
	InstanceID      string  `json:"instance_id,omitempty"`
	MachineID       string  `json:"machine_id,omitempty"`
	Lease           int64   `json:"lease"`
	PrevLeases      []int64 `json:"prev_leases,omitempty"`
	Drained         bool    `json:"drained"`
	IP              string  `json:"ip"`
	PrevEgress      string  `json:"prev_egress,omitempty"`
	ExpectNewEgress bool    `json:"expect_new_egress,omitempty"`
}

func (r *Registrar) saveLocked() {
	if r.cfg.StateFile == "" {
		return
	}
	ip := r.regIP
	if ip == "" {
		ip = r.resumeIP
	}
	if r.drained {
		ip = ""
	}
	st := diskState{
		NodeID:          r.cfg.NodeID,
		InstanceID:      r.instanceID,
		MachineID:       r.machineID,
		Lease:           int64(r.lease),
		PrevLeases:      append([]int64(nil), r.stale...),
		Drained:         r.drained,
		IP:              ip,
		PrevEgress:      r.prevEgress,
		ExpectNewEgress: r.expectNewEgress,
	}
	b, err := json.Marshal(st)
	if err != nil {
		r.event("state_err", "err", err.Error())
		return
	}
	path := r.cfg.StateFile
	r.do(func() {
		if err := writeStateFile(path, b); err != nil {
			r.event("state_err", "err", err.Error())
		}
	})
}

func writeStateFile(path string, b []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	_, werr := f.Write(b)
	if werr == nil {
		werr = f.Sync()
	}
	cerr := f.Close()
	if werr != nil {
		return werr
	}
	if cerr != nil {
		return cerr
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	df, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = df.Sync()
	_ = df.Close()
	return err
}

// loadState 区分三种结果：文件不存在、读到本机状态、文件损坏或 node_id 对不上。
// 后一种不能当成“第一次启动”，否则重拨中的出口校验会被跳过。
func loadState(path, nodeID string) (diskState, int) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return diskState{}, stateMissing
		}
		return diskState{}, stateUntrusted
	}
	var st diskState
	if json.Unmarshal(b, &st) != nil || st.NodeID != nodeID {
		return diskState{}, stateUntrusted
	}
	return st, stateOK
}
