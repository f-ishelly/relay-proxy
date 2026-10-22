package registrar

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
)

const (
	// DefaultTTL 是 lease 的租期。进程消失后 key 最多再存活这么久。
	DefaultTTL = 10 * time.Second
	// DefaultRenewInterval 是续租间隔。
	DefaultRenewInterval = 3 * time.Second
	// DefaultEpochTTL 是重拨标记自己的租期，比业务 lease 长。进程还在就续租；进程退出后标记最多再留这么久。
	DefaultEpochTTL = 30 * time.Second
	// DefaultEpochRenewInterval 是重拨标记的续租间隔。
	DefaultEpochRenewInterval = 10 * time.Second
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
	// DefaultDrainAttempt 是 Drain 单次尝试的超时。持有者删除是一次事务。
	// 续租量到更大的 RTT 时，这一次会拉到 1.5×RTT，但不超过剩余的 DrainBudget。
	// 失败后按指数退避加抖动再试，避免 700ms 超时打成重试风暴。
	DefaultDrainAttempt = 2 * time.Second
	// DefaultRedialJitter 是计划内批量重拨在 Drain 之前必须拉开的随机上界。
	DefaultRedialJitter = 60 * time.Second
	// DefaultDrainBudget 是 Drain 的总重试时间。
	DefaultDrainBudget = 6 * time.Second
	// DefaultOpTimeout 是 Grant、Put、Revoke、续租的单次超时。
	DefaultOpTimeout = 2 * time.Second
	// DefaultBackgroundTimeout 是探测和地址守卫触发的 etcd 写入超时。
	DefaultBackgroundTimeout = 700 * time.Millisecond
	// DefaultStartGrantTimeout 是 Start 等待第一次 Grant 成功的上限。
	DefaultStartGrantTimeout = 5 * time.Second
	// DefaultAssumedDial 与 DefaultAssumedEgressCheck 只给 admin 的时钟用，registrar 不按它们睡眠。
	// 一次拨号加一次出口校验合计约 5s。没有分开的实测，规划上按 4s 拨号、1s 校验。
	DefaultAssumedDial = 4 * time.Second
	// DefaultAssumedEgressCheck 见 DefaultAssumedDial。
	DefaultAssumedEgressCheck = time.Second
	// DefaultAssumedRedialFailureRate 是一次拨号加出口校验的失败率上界（低于 1%）。
	// admin 用它估计要不要马上再拨一次。registrar 不按它拒绝注册。
	DefaultAssumedRedialFailureRate = 0.01
	// DefaultInspectionInterval 是 admin 巡检间隔：外网是否可达，以及 CPU、内存是否正常。
	// registrar 不自己轮询，等 admin 调用 Report。
	DefaultInspectionInterval = time.Minute

	defaultCapability = "http-connect"
	defaultWeight     = 100
	defaultVersion    = "gost-v3"

	// NodeIDEnv 是运维注入 nodeId 的环境变量。Config.NodeID 为空时读取它。
	// 唯一性由运维层保证；事务里的冲突检测只是兜底。
	NodeIDEnv = "RELAY_NODE_ID"
)

// 资源异常如何影响注册。v1 的 L1 按 node_id 精确选路，不看 weight，
// 所以默认摘掉 key，而不是只把 weight 写小。
const (
	// ResourceFaultDeregister 在 Report 报告 CPU 或内存异常时 Delete key，恢复后再 Put。
	ResourceFaultDeregister = "deregister"
	// ResourceFaultIgnore 只把资源结论交给 OnReport，不改 etcd。
	ResourceFaultIgnore = "ignore"
)

// AlertNodeConflict 表示另一个还活着的 lease 占着同一个 nodeId，本进程拒绝覆盖。
const AlertNodeConflict = "node_id_conflict"

// AlertForeignInstance 表示 key 或重拨标记属于另一个实例（克隆或另一台同 nodeId 的虚拟机）。
const AlertForeignInstance = "foreign_instance"

// AlertEpochHoldExpired 表示持有者 drained 太久，已经停止续租重拨标记。标记会在一个 EpochTTL 内自己过期。
const AlertEpochHoldExpired = "epoch_hold_expired"

// Alert 是给 admin 打日志或接告警用的。OnAlert 在独立 goroutine 里调用。
type Alert struct {
	Kind       string
	NodeID     string
	Key        string
	OtherLease int64
	Message    string
}

// Report 是 admin 每分钟巡检的结论。零值表示健康：没有故障标志就不会摘 key。
// 不要用“正常=true”的布尔，否则只填了外网、忘了资源字段时会被当成资源故障。
type Report struct {
	NetFault bool
	CPUFault bool
	MemFault bool
	EgressIP string
}

// ReportEvent 是一次 Report 的本地通知。默认不写 etcd，也不外发。
type ReportEvent struct {
	NetFault bool
	CPUFault bool
	MemFault bool
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
	// NodeID 是这台虚拟机的节点 ID，例如 626220。
	// 为空时读环境变量 RELAY_NODE_ID，再不行才读 NodeIDFile。本包不分配 ID，唯一性由运维保证。
	NodeID string
	// NodeIDFile 是环境变量也没有时的兜底文件，内容会去掉首尾空白。生产路径不依赖它。
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

	TTL           time.Duration
	RenewInterval time.Duration
	// EpochTTL 是 /relay/l2-epoch/{nodeId} 的租期。它不挂在业务 lease 上，业务 lease 丢失时标记还在。
	// 默认是业务 TTL 的 3 倍，且不少于 DefaultEpochTTL。进程退出后，别人要等这条租约到期才能在空窗里创建 key。
	EpochTTL           time.Duration
	EpochRenewInterval time.Duration
	// MaxDrained 是持有者停在 drained、一直不调用 Ready 时，最多继续续租重拨标记多久。
	// 超过之后停止续租，打 AlertEpochHoldExpired，标记在剩余的 EpochTTL 内过期，别的实例才能创建。
	// 0 表示用 10×EpochTTL（EpochTTL 为 30s 时是 5 分钟）。
	MaxDrained           time.Duration
	OpTimeout            time.Duration
	BackgroundTimeout    time.Duration
	DrainAttempt         time.Duration
	DrainBudget          time.Duration
	StartGrantTimeout    time.Duration
	DialTimeout          time.Duration
	DialKeepAliveTime    time.Duration
	DialKeepAliveTimeout time.Duration
	// DialOptions 附加到 etcd 客户端，测试用来注入延迟。生产留空。
	DialOptions []grpc.DialOption
	// RedialJitter 是计划内重拨调用 JitterBeforeDrain 时的随机上界。默认 60s。
	// 一批节点的 Drain 必须先拉开这段随机时间，不能在同一时刻一起删 key。
	RedialJitter time.Duration

	// StateFile 保存 lease、实例身份、drained 和已注册的出口 IP。
	StateFile string
	// MachineID 覆盖 /etc/machine-id。生产留空。克隆检测靠它和状态文件里记下的值是否相同。
	MachineID string

	// IgnoreAdminNetCheck 为 true 时，Report 的 NetOK 不摘除也不恢复。默认 false：信任 admin 的外网巡检。
	IgnoreAdminNetCheck bool
	// ResourceFaultAction 控制 CPU、内存异常。默认 deregister：摘掉 key，恢复后再写回。
	// ignore 时不改 etcd。不提供“只降 weight”：v1 按 node_id 精确选路，weight 不影响流量。
	ResourceFaultAction string
	// OnReport 把上报转给 admin 现有通道。默认 nil，不上报副本，也不把上报正文写入 etcd。
	OnReport func(ReportEvent)
	// OnAlert 在拒绝覆盖冲突 key 时调用。冲突检测是 nodeId 的兜底，不能代替运维保证唯一。
	OnAlert func(Alert)
	Logger  Logger

	// 下面几项是 admin 侧的规划值，状态机不按它们睡眠。
	// 一次拨号加一次出口校验合计约 5s，失败率低于 1%。lease 在这段时间里继续续租，TTL 仍是 10s。
	AssumedDial              time.Duration
	AssumedEgressCheck       time.Duration
	AssumedRedialFailureRate float64
	InspectionInterval       time.Duration
}

// DefaultConfig 返回方案里的默认节奏。调用方仍须填写 Endpoints、NodeID、地址、探测和 LocalIP。
func DefaultConfig() Config {
	return Config{
		Weight:                   defaultWeight,
		Capability:               defaultCapability,
		Version:                  defaultVersion,
		ProbeTarget:              "127.0.0.1:1",
		ProbeExpectStatus:        403,
		ProbeInterval:            DefaultProbeInterval,
		ProbeTimeout:             DefaultProbeTimeout,
		ProbeFailThreshold:       DefaultProbeFailThreshold,
		ProbeRiseThreshold:       DefaultProbeRiseThreshold,
		IPGuardInterval:          DefaultIPGuardInterval,
		TTL:                      DefaultTTL,
		RenewInterval:            DefaultRenewInterval,
		EpochTTL:                 DefaultEpochTTL,
		EpochRenewInterval:       DefaultEpochRenewInterval,
		OpTimeout:                DefaultOpTimeout,
		BackgroundTimeout:        DefaultBackgroundTimeout,
		DrainAttempt:             DefaultDrainAttempt,
		DrainBudget:              DefaultDrainBudget,
		StartGrantTimeout:        DefaultStartGrantTimeout,
		DialTimeout:              2 * time.Second,
		DialKeepAliveTime:        2 * time.Second,
		DialKeepAliveTimeout:     2 * time.Second,
		RedialJitter:             DefaultRedialJitter,
		AssumedDial:              DefaultAssumedDial,
		AssumedEgressCheck:       DefaultAssumedEgressCheck,
		AssumedRedialFailureRate: DefaultAssumedRedialFailureRate,
		InspectionInterval:       DefaultInspectionInterval,
		ResourceFaultAction:      ResourceFaultDeregister,
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
	if c.EpochTTL == 0 {
		c.EpochTTL = c.TTL * 3
		if c.EpochTTL < d.EpochTTL {
			c.EpochTTL = d.EpochTTL
		}
	}
	if c.EpochRenewInterval == 0 {
		c.EpochRenewInterval = d.EpochRenewInterval
		if c.EpochRenewInterval >= c.EpochTTL {
			c.EpochRenewInterval = c.EpochTTL / 3
		}
	}
	if c.MaxDrained == 0 {
		c.MaxDrained = 10 * c.EpochTTL
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
	if c.AssumedRedialFailureRate == 0 {
		c.AssumedRedialFailureRate = d.AssumedRedialFailureRate
	}
	if c.InspectionInterval == 0 {
		c.InspectionInterval = d.InspectionInterval
	}
	if c.ResourceFaultAction == "" {
		c.ResourceFaultAction = d.ResourceFaultAction
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
	if c.EpochTTL < time.Second || c.EpochTTL%time.Second != 0 || c.EpochTTL < c.TTL {
		return fmt.Errorf("%w: epoch ttl must be whole seconds and >= ttl", ErrInvalidConfig)
	}
	if c.EpochRenewInterval <= 0 || c.EpochRenewInterval >= c.EpochTTL {
		return fmt.Errorf("%w: epoch renew interval must be >0 and < epoch ttl", ErrInvalidConfig)
	}
	if c.MaxDrained <= 0 {
		return fmt.Errorf("%w: max drained duration", ErrInvalidConfig)
	}
	if c.DrainAttempt <= 0 || c.DrainBudget < c.DrainAttempt {
		return fmt.Errorf("%w: drain budget", ErrInvalidConfig)
	}
	switch c.ResourceFaultAction {
	case ResourceFaultDeregister, ResourceFaultIgnore:
	default:
		return fmt.Errorf("%w: resource fault action %q", ErrInvalidConfig, c.ResourceFaultAction)
	}
	return nil
}

func loadNodeID(c *Config) error {
	if c.NodeID != "" {
		c.NodeID = strings.TrimSpace(c.NodeID)
		if c.NodeID == "" {
			return fmt.Errorf("%w: node id is blank", ErrInvalidConfig)
		}
		return nil
	}
	if raw, ok := os.LookupEnv(NodeIDEnv); ok {
		v := strings.TrimSpace(raw)
		if v == "" {
			return fmt.Errorf("%w: %s is set but blank", ErrInvalidConfig, NodeIDEnv)
		}
		c.NodeID = v
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

func (c Config) currentMachineID() string {
	if strings.TrimSpace(c.MachineID) != "" {
		return strings.TrimSpace(c.MachineID)
	}
	b, err := os.ReadFile("/etc/machine-id")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func newInstanceID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return hex.EncodeToString([]byte(time.Now().UTC().Format(time.RFC3339Nano)))
	}
	return hex.EncodeToString(b[:])
}
