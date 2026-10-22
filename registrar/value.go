package registrar

// Value 是 /relay/l2/{nodeId} 的 JSON。addr 给 L1 拨号，egress_ip 只作信息。
type Value struct {
	Addr         string `json:"addr"`
	EgressIP     string `json:"egress_ip"`
	ProxyPort    int    `json:"proxy_port"`
	ControlPort  int    `json:"control_port"`
	NodeID       string `json:"node_id"`
	InstanceID   string `json:"instance_id"`
	AZ           string `json:"az"`
	Weight       int    `json:"weight"`
	Capability   string `json:"capability"`
	Version      string `json:"version"`
	RegisteredAt int64  `json:"registered_at"`
}

// Key 返回这台主机在 etcd 里的 key。L1 只 Watch 这个前缀。
func Key(nodeID string) string { return "/relay/l2/" + nodeID }

// EpochKey 是重拨期间留下的标记，不在 /relay/l2/ 下面，L1 的前缀 Watch 看不到。
// 它挂在自己的 lease 上，不随业务 lease 一起消失。
func EpochKey(nodeID string) string { return "/relay/l2-epoch/" + nodeID }

// Status 是本进程看到的注册状态，不额外读 etcd。
type Status struct {
	Key         string `json:"key"`
	NodeID      string `json:"node_id"`
	LeaseID     int64  `json:"lease_id"`
	Addr        string `json:"addr"`
	EgressIP    string `json:"egress_ip"`
	Drained     bool   `json:"drained"`
	ProbeOK     bool   `json:"probe_ok"`
	NetOK       bool   `json:"net_ok"`
	ResourcesOK bool   `json:"resources_ok"`
	WantIP      string `json:"want_ip"`
}
