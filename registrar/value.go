package registrar

// Value 是 /relay/l2/{nodeId} 的 JSON。addr 给 L1 拨号，egress_ip 只作信息。
type Value struct {
	Addr         string `json:"addr"`
	EgressIP     string `json:"egress_ip"`
	ProxyPort    int    `json:"proxy_port"`
	ControlPort  int    `json:"control_port"`
	NodeID       string `json:"node_id"`
	AZ           string `json:"az"`
	Weight       int    `json:"weight"`
	Capability   string `json:"capability"`
	Version      string `json:"version"`
	RegisteredAt int64  `json:"registered_at"`
}

// Key 返回这台主机在 etcd 里的 key。
func Key(nodeID string) string { return "/relay/l2/" + nodeID }

// Status 是本进程看到的注册状态，不额外读 etcd。
type Status struct {
	Key      string `json:"key"`
	NodeID   string `json:"node_id"`
	LeaseID  int64  `json:"lease_id"`
	Addr     string `json:"addr"`
	EgressIP string `json:"egress_ip"`
	Drained  bool   `json:"drained"`
	ProbeOK  bool   `json:"probe_ok"`
	NetOK    bool   `json:"net_ok"`
	WantIP   string `json:"want_ip"`
}
