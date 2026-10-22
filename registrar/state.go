package registrar

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// diskState 落在 StateFile。drained 必须在 Delete 之前写下去，崩溃后才不会把旧出口又注册上。
type diskState struct {
	NodeID     string  `json:"node_id"`
	Lease      int64   `json:"lease"`
	PrevLeases []int64 `json:"prev_leases,omitempty"`
	Drained    bool    `json:"drained"`
	IP         string  `json:"ip"`
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
		NodeID:     r.cfg.NodeID,
		Lease:      int64(r.lease),
		PrevLeases: append([]int64(nil), r.stale...),
		Drained:    r.drained,
		IP:         ip,
	}
	b, err := json.Marshal(st)
	if err != nil {
		r.event("state_err", "err", err.Error())
		return
	}
	dir := filepath.Dir(r.cfg.StateFile)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		r.event("state_err", "err", err.Error())
		return
	}
	tmp := r.cfg.StateFile + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		r.event("state_err", "err", err.Error())
		return
	}
	if err := os.Rename(tmp, r.cfg.StateFile); err != nil {
		r.event("state_err", "err", err.Error())
	}
}

func loadState(path, nodeID string) (diskState, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return diskState{}, false
	}
	var st diskState
	if json.Unmarshal(b, &st) != nil || st.NodeID != nodeID {
		return diskState{}, false
	}
	return st, true
}
