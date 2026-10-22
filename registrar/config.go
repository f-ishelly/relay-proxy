package registrar

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	// DefaultTTL 是 lease 的租期。进程消失后 key 最多再存活这么久。
	DefaultTTL = 10 * time.Second
	// DefaultRenewInterval 是续租间隔。
	DefaultRenewInterval = 3 * time.Second
	// DefaultProbeInterval 是 gost 探测间隔。
	DefaultProbeInterval = time.Second
	// DefaultProbeTimeout 是单次探测超时。
	DefaultProbeTimeout = 800 * time.Millisecond
	// DefaultProbeFailThreshold 连续失败这么多次就摘除。
	DefaultProbeFailThreshold = 3
	// DefaultProbeRiseThreshold 连续成功这么多次才恢复注册。
	DefaultProbeRiseThreshold = 2
	// DefaultIPGuardInterval 是 ppp 地址守卫的间隔。
	DefaultIPGuardInterval = 500 * time.Millisecond
	// DefaultDrainAttempt 是 Drain 单次 etcd 调用的超时。
	DefaultDrainAttempt = 700 * time.Millisecond
	// DefaultDrainBudget 是 Drain 的总重试时间。
	DefaultDrainBudget = 6 * time.Second
	// DefaultOpTimeout 是 Grant、Put、Revoke、续租的单次超时。
	DefaultOpTimeout = 2 * time.Second
	// DefaultBackgroundTimeout 是探测和地址守卫触发的 etcd 写入超时。
	DefaultBackgroundTimeout = 700 * time.Millisecond
	// DefaultStartGrantTimeout 是 Start 等待第一次 Grant 成功的上限。
	DefaultStartGrantTimeout = 5 * time.Second
	// DefaultAssumedDial 是 admin 侧拨号耗时的规划值。registrar 不会按它睡眠。
	DefaultAssumedDial = 3 * time.Second
	// DefaultAssumedEgressCheck 是 admin 侧出口校验耗时的规划值。registrar 不会按它睡眠。
	DefaultAssumedEgressCheck = 300 * time.Millisecond
	// DefaultInspectionInterval 是期望 admin 调用 Report 的间隔。registrar 不会自己按它轮询。
	DefaultInspectionInterval = 2 * time.Second

	defaultCapability = "http-connect"
	defaultWeight     = 100
	defaultVersion    = "gost-v3"
)

// AlertNodeConflict 表示另一个还活着的 lease 占着同一个 nodeId，本进程拒绝覆盖。
const AlertNodeConflict = "node_id_conflict"

// Alert 是给 admin 打日志或接告警用的。OnAlert 在独立 goroutine 里调用。
type Alert struct {
	Kind       string
	NodeID     string
	Key        string
	OtherLease int64
	Message    string
}

// ReportEvent 是一次 Report 的本地通知。默认不写 etcd，也不外发。
type ReportEvent struct {
	NetOK    bool
	EgressIP string
	Drained  bool
	At       time.Time
}

// Logger 接收状态机事件。args 是成对的键和值。nil 表示不打日志。
type Logger interface {
	Event(ev string, args ...any)
}

// JSONLogger 把事件打成一行 JSON，字段 t 是 unix 毫秒。
type JSONLogger struct {
	Out interface{ Write([]byte) (int, error) }
}

// Event implements Logger.
func (l JSONLogger) Event(ev string, args ...any) {
	if l.Out == nil {
		return
	}
	m := map[string]any{"t": time.Now().UnixMilli(), "src": "registrar", "ev": ev}
	for i := 0; i+1 < len(args); i += 2 {
		m[fmt.Sprint(args[i])] = args[i+1]
	}
	b, err := json.Marshal(m)
	if err != nil {
		return
	}
	b = append(b, '\n')
	_, _ = l.Out.Write(b)
}

// Config 是一台主机的注册配置。先用 DefaultConfig，再填本机必填项。
type Config struct {
	// Endpoints 是 etcd 全部成员。生产环境少配成员时，切主会让连在死去成员上的 lease 过期。
	Endpoints []string
	// NodeID 是这台主机已有的节点 ID，例如 626220。空则读 NodeIDFile。本包不分配 ID。
	NodeID string
	// NodeIDFile 是 NodeID 为空时读取的文件，内容会去掉首尾空白。
	NodeIDFile string

	// IntranetAddr 写入 value.addr，形如 10.0.8.26:8080。重拨前后保持不变。
	IntranetAddr string
	// ProxyPort 是 gost 监听端口，必须和 IntranetAddr 里的端口一致。
	ProxyPort int
	// ControlPort 写入 value.control_port，供运维识别 admin 端口。
	ControlPort int
	AZ          string
	// Weight 为 0 时使用默认 100。v1 每个 nodeId 只有一个 key，L1 不必再做多实例均衡。
	Weight int
	// Capability 默认 http-connect。
	Capability string
	// Version 默认 gost-v3，可写成实际安装的 gost 版本。
	Version string

	// LocalIP 返回本机 ppp 出口地址。链路断开时返回空字符串。必须并发安全，且不能回调 registrar。
	LocalIP func() string

	// ProbeAddr 是本机探测 gost 的地址，通常是 127.0.0.1:端口。
	ProbeAddr     string
	ProbeUser     string
	ProbePassword string
	// ProbeTarget 是 CONNECT 的目标，默认 127.0.0.1:1，用来撞上出口拒绝并期望 403。
	ProbeTarget string
	// ProbeExpectStatus 默认 403。
	ProbeExpectStatus  int
	ProbeInterval      time.Duration
	ProbeTimeout       time.Duration
	ProbeFailThreshold int
	ProbeRiseThreshold int
	IPGuardInterval    time.Duration

	TTL                  time.Duration
	RenewInterval        time.Duration
	OpTimeout            time.Duration
	BackgroundTimeout    time.Duration
	DrainAttempt         time.Duration
	DrainBudget          time.Duration
	StartGrantTimeout    time.Duration
	DialTimeout          time.Duration
	DialKeepAliveTime    time.Duration
	DialKeepAliveTimeout time.Duration

	// StateFile 保存 lease、drained 和已注册的出口 IP，重拨中崩溃靠它避免提前注册。
	StateFile string

	// IgnoreAdminNetCheck 为 true 时，Report 的 netOK 不摘除也不恢复。默认 false：信任 admin 巡检。
	IgnoreAdminNetCheck bool
	// OnReport 把上报转给 admin 现有通道。默认 nil，不上报副本，也不把上报正文写入 etcd。
	OnReport func(ReportEvent)
	// OnAlert 在拒绝覆盖冲突 key 时调用。
	OnAlert func(Alert)
	Logger  Logger

	// 下面三项是待确认问题的规划默认值，状态机不按它们睡眠。
	// admin 自己持有拨号和巡检的时钟，可以从这里读默认值，也可以覆盖。
	AssumedDial        time.Duration
	AssumedEgressCheck time.Duration
	InspectionInterval time.Duration
}

// DefaultConfig 返回方案里的默认节奏。调用方仍须填写 Endpoints、NodeID、地址、探测和 LocalIP。
func DefaultConfig() Config {
	return Config{
		Weight:               defaultWeight,
		Capability:           defaultCapability,
		Version:              defaultVersion,
		ProbeTarget:          "127.0.0.1:1",
		ProbeExpectStatus:    403,
		ProbeInterval:        DefaultProbeInterval,
		ProbeTimeout:         DefaultProbeTimeout,
		ProbeFailThreshold:   DefaultProbeFailThreshold,
		ProbeRiseThreshold:   DefaultProbeRiseThreshold,
		IPGuardInterval:      DefaultIPGuardInterval,
		TTL:                  DefaultTTL,
		RenewInterval:        DefaultRenewInterval,
		OpTimeout:            DefaultOpTimeout,
		BackgroundTimeout:    DefaultBackgroundTimeout,
		DrainAttempt:         DefaultDrainAttempt,
		DrainBudget:          DefaultDrainBudget,
		StartGrantTimeout:    DefaultStartGrantTimeout,
		DialTimeout:          2 * time.Second,
		DialKeepAliveTime:    2 * time.Second,
		DialKeepAliveTimeout: 2 * time.Second,
		AssumedDial:          DefaultAssumedDial,
		AssumedEgressCheck:   DefaultAssumedEgressCheck,
		InspectionInterval:   DefaultInspectionInterval,
	}
}

func applyDefaults(c *Config) {
	d := DefaultConfig()
	if c.Weight == 0 {
		c.Weight = d.Weight
	}
	if c.Capability == "" {
		c.Capability = d.Capability
	}
	if c.Version == "" {
		c.Version = d.Version
	}
	if c.ProbeTarget == "" {
		c.ProbeTarget = d.ProbeTarget
	}
	if c.ProbeExpectStatus == 0 {
		c.ProbeExpectStatus = d.ProbeExpectStatus
	}
	if c.ProbeInterval == 0 {
		c.ProbeInterval = d.ProbeInterval
	}
	if c.ProbeTimeout == 0 {
		c.ProbeTimeout = d.ProbeTimeout
	}
	if c.ProbeFailThreshold == 0 {
		c.ProbeFailThreshold = d.ProbeFailThreshold
	}
	if c.ProbeRiseThreshold == 0 {
		c.ProbeRiseThreshold = d.ProbeRiseThreshold
	}
	if c.IPGuardInterval == 0 {
		c.IPGuardInterval = d.IPGuardInterval
	}
	if c.TTL == 0 {
		c.TTL = d.TTL
	}
	if c.RenewInterval == 0 {
		c.RenewInterval = d.RenewInterval
	}
	if c.OpTimeout == 0 {
		c.OpTimeout = d.OpTimeout
	}
	if c.BackgroundTimeout == 0 {
		c.BackgroundTimeout = d.BackgroundTimeout
	}
	if c.DrainAttempt == 0 {
		c.DrainAttempt = d.DrainAttempt
	}
	if c.DrainBudget == 0 {
		c.DrainBudget = d.DrainBudget
	}
	if c.StartGrantTimeout == 0 {
		c.StartGrantTimeout = d.StartGrantTimeout
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = d.DialTimeout
	}
	if c.DialKeepAliveTime == 0 {
		c.DialKeepAliveTime = d.DialKeepAliveTime
	}
	if c.DialKeepAliveTimeout == 0 {
		c.DialKeepAliveTimeout = d.DialKeepAliveTimeout
	}
	if c.AssumedDial == 0 {
		c.AssumedDial = d.AssumedDial
	}
	if c.AssumedEgressCheck == 0 {
		c.AssumedEgressCheck = d.AssumedEgressCheck
	}
	if c.InspectionInterval == 0 {
		c.InspectionInterval = d.InspectionInterval
	}
}

var nodeIDRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

func validate(c *Config) error {
	if len(c.Endpoints) == 0 {
		return fmt.Errorf("%w: endpoints required", ErrInvalidConfig)
	}
	for _, ep := range c.Endpoints {
		if strings.TrimSpace(ep) == "" {
			return fmt.Errorf("%w: empty endpoint", ErrInvalidConfig)
		}
	}
	if strings.TrimSpace(c.NodeID) == "" {
		return fmt.Errorf("%w: node id required", ErrInvalidConfig)
	}
	if !nodeIDRe.MatchString(c.NodeID) {
		return fmt.Errorf("%w: node id %q", ErrInvalidConfig, c.NodeID)
	}
	host, port, err := net.SplitHostPort(c.IntranetAddr)
	if err != nil || host == "" {
		return fmt.Errorf("%w: intranet addr %q", ErrInvalidConfig, c.IntranetAddr)
	}
	if c.ProxyPort <= 0 {
		return fmt.Errorf("%w: proxy port", ErrInvalidConfig)
	}
	if port != strconv.Itoa(c.ProxyPort) {
		return fmt.Errorf("%w: intranet addr port %s != proxy_port %d", ErrInvalidConfig, port, c.ProxyPort)
	}
	if c.LocalIP == nil {
		return fmt.Errorf("%w: LocalIP required", ErrInvalidConfig)
	}
	if c.ProbeAddr == "" {
		return fmt.Errorf("%w: probe addr required", ErrInvalidConfig)
	}
	if c.StateFile == "" {
		return fmt.Errorf("%w: state file required", ErrInvalidConfig)
	}
	if c.TTL < time.Second || c.TTL%time.Second != 0 {
		return fmt.Errorf("%w: ttl must be whole seconds", ErrInvalidConfig)
	}
	if c.RenewInterval <= 0 || c.RenewInterval >= c.TTL {
		return fmt.Errorf("%w: renew interval must be >0 and < ttl", ErrInvalidConfig)
	}
	if c.DrainAttempt <= 0 || c.DrainBudget < c.DrainAttempt {
		return fmt.Errorf("%w: drain budget", ErrInvalidConfig)
	}
	return nil
}

func loadNodeID(c *Config) error {
	if strings.TrimSpace(c.NodeID) != "" {
		c.NodeID = strings.TrimSpace(c.NodeID)
		return nil
	}
	if c.NodeIDFile == "" {
		return nil
	}
	b, err := os.ReadFile(c.NodeIDFile)
	if err != nil {
		return fmt.Errorf("%w: read node id file: %v", ErrInvalidConfig, err)
	}
	c.NodeID = strings.TrimSpace(string(b))
	return nil
}
