//go:build docfixture

package l1metrics

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	// NodeUnknown 是路由表从未收录的 node_id 在标签里的固定值。
	NodeUnknown = "unknown"

	ReasonNoUpstream = "no_upstream"
	ReasonDial       = "dial"
	ReasonL2Write    = "l2_write"
	ReasonL2Deadline = "l2_deadline"
	ReasonL2Response = "l2_response"
	ReasonL2Status   = "l2_status"
	ReasonL2Auth     = "l2_auth"

	// ReasonClientGone 只用于 proxy_client_abort_total，以及建连时延的 result。
	// 不要传给 Fail502。它不进入上游失败率的分子。
	ReasonClientGone = "client_gone"

	// ReasonStaleTunnel 只用于明文 HTTP：复用的隧道在源站字节之前断开，且请求不能重放。
	// 不传给 Fail502，也不进 proxy_http_tunnel_setup_ms。
	ReasonStaleTunnel = "stale_tunnel"

	// 正文中断的 reason。正常 EOF 和 101 的干净拆除不记。
	// client_cancel 是客户端断开或收不下。origin_reset 是源站把响应截断或拆连接。
	// idle_timeout 是空闲读超时。request_body 是最终状态行写出之后，请求字节写进隧道失败。
	ReasonAbortClientCancel = "client_cancel"
	ReasonAbortOriginReset  = "origin_reset"
	ReasonAbortIdleTimeout  = "idle_timeout"
	ReasonAbortRequestBody  = "request_body"

	// ResultOK 是 L2 已返回 200。失败结果复用上面的 reason 常量，不含 no_upstream。
	ResultOK = "ok"

	ReasonRejectAuth          = "auth"
	ReasonRejectNoNode        = "no_node"
	ReasonRejectBadTarget     = "bad_target"
	ReasonRejectBadCredential = "bad_credential"

	// DirUp 是客户端到 L2 的隧道字节。DirDown 是 L2 到客户端。
	DirUp   = "up"
	DirDown = "down"

	// ProtocolConnect 与 ProtocolHTTP 只写进计量事件，不是 Prometheus 标签。
	ProtocolConnect = "connect"
	ProtocolHTTP    = "http"

	StateReady    = "ready"
	StateNotReady = "not_ready"

	// MeterQueue 是计量事件的缓冲。满了就丢，转发路径不阻塞。
	MeterQueue = 1024
)

var (
	proxyConnActive = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "proxy_conn_active",
		Help: "只统计 CONNECT。已经向客户端写出 200 Connection Established、handleConnect 尚未返回的隧道数。明文 HTTP 不写 L1 自己的 200，不进这个 gauge。客户端断开后最多滞后 30s（半关闭空闲）或 5 分钟（隧道空闲）。",
	}, []string{"node_id"})

	proxyConnectLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "proxy_connect_latency_ms",
		Help:    "从开始拨号 L2 到本次尝试结束的毫秒数。result 只取 ok、dial、l2_write、l2_deadline、l2_response、l2_status、l2_auth、client_gone。",
		Buckets: []float64{5, 10, 25, 50, 100, 200, 500, 1000, 3000, 10000},
	}, []string{"node_id", "result"})

	proxyTunnelDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "proxy_tunnel_duration_seconds",
		Help:    "已向客户端写出 200 的隧道，从写出成功到 handleConnect 返回的秒数。",
		Buckets: []float64{1, 5, 15, 30, 60, 300, 900, 3600},
	}, []string{"node_id"})

	proxyTunnelBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "proxy_tunnel_bytes_total",
		Help: "隧道载荷字节。up 是客户端到 L2，down 是 L2 到客户端。不含 CONNECT 请求和 200 响应。",
	}, []string{"node_id", "direction"})

	proxyUpstreamFail = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "proxy_upstream_fail_total",
		Help: "没有建成隧道的上游结果。l2_auth 对应交给客户端的 L2 407，其余 reason 对应 502。不含 client_gone。",
	}, []string{"node_id", "reason"})

	proxyClientReject = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "proxy_client_reject_total",
		Help: "拨号 L2 之前，L1 本地返回 400 或 407 的次数。",
	}, []string{"reason"})

	proxyClientAbort = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "proxy_client_abort_total",
		Help: "等待 L2 状态行时客户端读泵先结束，且状态行未能解析。reason 只有 client_gone。",
	}, []string{"node_id", "reason"})

	proxyL2Nodes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "proxy_l2_nodes",
		Help: "本地路由表中的 node_id 个数。ready 至少有一个健康地址，not_ready 没有。",
	}, []string{"state"})

	proxyL2HealthyAddrs = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "proxy_l2_healthy_addrs",
		Help: "本地路由表里的健康 L2 地址条数。地址本身不是标签。",
	})

	proxyMeterDrop = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "proxy_meter_drop_total",
		Help: "计量事件被丢弃的条数。队列满，或 protocol 不是 connect / http。",
	})

	proxyHTTPTunnelSetup = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "proxy_http_tunnel_setup_ms",
		Help:    "新开的明文 HTTP 隧道，从拨号前到 L2 的 CONNECT 出结果的毫秒数。result 是 ok、dial、l2_write、l2_deadline、l2_response、l2_status、l2_auth。不含 client_gone，不含 stale_tunnel。复用不观察。P95 页看 result=ok。",
		Buckets: []float64{5, 10, 25, 50, 100, 200, 500, 1000, 3000, 10000},
	}, []string{"node_id", "result"})

	proxyHTTPRequestLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "proxy_http_request_latency_ms",
		Help:    "一条明文 HTTP 请求从拨号前（复用隧道则从写出请求起）到结果确定的毫秒数。result 比建连多 stale_tunnel。ok 停在源站最终状态行写完。只上看板，不设页。",
		Buckets: []float64{5, 10, 25, 50, 100, 200, 500, 1000, 3000, 10000},
	}, []string{"node_id", "result"})

	proxyHTTPTunnelActive = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "proxy_http_tunnel_active",
		Help: "明文 HTTP 握住的 L2 隧道数，含 keep-alive 空等，最长 5 分钟。L2 的 CONNECT 返回 200 时加一，隧道关闭时减一。不是 SLI。明文 HTTP 不向客户端写 L1 自己的 200。",
	}, []string{"node_id"})

	proxyHTTPTunnelDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "proxy_http_tunnel_duration_seconds",
		Help:    "明文 HTTP 的 L2 隧道从 L2 返回 200 到关闭的秒数，含两条请求之间的空等。",
		Buckets: []float64{1, 5, 15, 30, 60, 300, 900, 3600},
	}, []string{"node_id"})

	proxyHTTPTunnelBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "proxy_http_tunnel_bytes_total",
		Help: "明文 HTTP 载荷，与 proxy_tunnel_bytes_total 对齐。up 是 L2 返回 200 之后写进隧道的字节，不含发往 L2 的 CONNECT，含 101 之后的上行。down 含 1xx、最终响应和 101 之后的下行。",
	}, []string{"node_id", "direction"})

	proxyHTTPUpstreamFail = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "proxy_http_upstream_fail_total",
		Help: "明文请求没有完成。reason 比 proxy_upstream_fail_total 多 stale_tunnel，不含 client_gone。已经写出源站字节时不再补 502，计数仍然加。",
	}, []string{"node_id", "reason"})

	proxyHTTPClientReject = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "proxy_http_client_reject_total",
		Help: "明文请求在拨 L2 之前，L1 本地返回 400 或 407 的次数。没有 node_id。",
	}, []string{"reason"})

	proxyHTTPClientAbort = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "proxy_http_client_abort_total",
		Help: "明文请求在最终状态行完整写出之前客户端离开。reason 只有 client_gone。不进失败率。",
	}, []string{"node_id", "reason"})

	proxyHTTPReusedRetry = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "proxy_http_reused_conn_retry_total",
		Help: "复用隧道被对端拆掉后重开的次数。只含 GET/HEAD/OPTIONS/TRACE，以及打开 retryPutDelete 时的 PUT/DELETE。不含读超时，不含写到一半的请求头。不进失败率。",
	}, []string{"node_id"})

	proxyHTTPBodyAbort = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "proxy_http_body_abort_total",
		Help: "最终状态行已经写出之后的正文或 101 隧道失败。reason 是 client_cancel、origin_reset、idle_timeout、request_body。正常 EOF 和干净的 101 拆除不记。结果仍是 ok。不进失败率。",
	}, []string{"node_id", "reason"})

	meterEvents = make(chan MeterEvent, MeterQueue)
)

func init() {
	prometheus.MustRegister(
		proxyConnActive,
		proxyConnectLatency,
		proxyTunnelDuration,
		proxyTunnelBytes,
		proxyUpstreamFail,
		proxyClientReject,
		proxyClientAbort,
		proxyL2Nodes,
		proxyL2HealthyAddrs,
		proxyMeterDrop,
		proxyHTTPTunnelSetup,
		proxyHTTPRequestLatency,
		proxyHTTPTunnelActive,
		proxyHTTPTunnelDuration,
		proxyHTTPTunnelBytes,
		proxyHTTPUpstreamFail,
		proxyHTTPClientReject,
		proxyHTTPClientAbort,
		proxyHTTPReusedRetry,
		proxyHTTPBodyAbort,
	)
}

// Mount 在管理 mux 上暴露 /metrics。该 mux 使用独立监听地址，不挂在代理端口上。
func Mount(mux *http.ServeMux) {
	mux.Handle("/metrics", promhttp.Handler())
}

// NodeLabel 只接受路由表里已有的 node_id。未收录或空字符串折叠为 unknown。
func NodeLabel(inTable bool, nodeID string) string {
	if !inTable || nodeID == "" {
		return NodeUnknown
	}
	return nodeID
}

// Fail502 增加 proxy_upstream_fail_total。
// ReasonNoUpstream、ReasonDial、ReasonL2Write、ReasonL2Deadline、ReasonL2Response、ReasonL2Status
// 由调用方接着对客户端写 502。ReasonL2Auth 时调用方把 L2 的 407 原样交还，不改写成 502。
// ReasonClientGone 不是这里的 reason。
func Fail502(nodeID, reason string) {
	proxyUpstreamFail.WithLabelValues(nodeID, reason).Inc()
}

// RejectClient 记录一次拨号 L2 之前的本地 400 或 407。
// reason 取 ReasonRejectAuth、ReasonRejectNoNode、ReasonRejectBadTarget、ReasonRejectBadCredential。
func RejectClient(reason string) {
	proxyClientReject.WithLabelValues(reason).Inc()
}

// ClientAbort 记录一次 client_gone。其它 reason 直接丢掉，避免新标签。
func ClientAbort(nodeID, reason string) {
	if reason != ReasonClientGone {
		return
	}
	proxyClientAbort.WithLabelValues(nodeID, reason).Inc()
}

func knownConnectResult(result string) bool {
	switch result {
	case ResultOK, ReasonDial, ReasonL2Write, ReasonL2Deadline, ReasonL2Response, ReasonL2Status, ReasonL2Auth, ReasonClientGone:
		return true
	default:
		return false
	}
}

// ObserveConnect 记录从 started 到本次尝试结束的毫秒数。
// 拨号之前的本地拒绝和 no_upstream 不调用。result 不在闭合集合里时不观察。
func ObserveConnect(nodeID, result string, started time.Time) {
	if !knownConnectResult(result) {
		return
	}
	elapsed := float64(time.Since(started)) / float64(time.Millisecond)
	proxyConnectLatency.WithLabelValues(nodeID, result).Observe(elapsed)
}

// ObserveTunnel 记录一条已建立隧道的存活秒数。started 是向客户端写出 200 成功的时刻。
func ObserveTunnel(nodeID string, started time.Time) {
	if started.IsZero() {
		return
	}
	proxyTunnelDuration.WithLabelValues(nodeID).Observe(time.Since(started).Seconds())
}

// AddTunnelBytes 累加一个方向的隧道载荷。n <= 0 或 direction 不是 up/down 时不记。
func AddTunnelBytes(nodeID, direction string, n int64) {
	if n <= 0 || (direction != DirUp && direction != DirDown) {
		return
	}
	proxyTunnelBytes.WithLabelValues(nodeID, direction).Add(float64(n))
}

// EnterTunnel 在向客户端写出 200 成功之后调用。返回值在 handleConnect 返回时把 gauge 减一。
// 客户端断开后，减一仍可能再等半关闭空闲（默认 30s）或隧道空闲（默认 5 分钟）。
// 明文 HTTP 不调用它：那条路径不写 L1 自己的 200。
func EnterTunnel(nodeID string) (leave func()) {
	gauge := proxyConnActive.WithLabelValues(nodeID)
	gauge.Inc()
	return gauge.Dec
}

func knownHTTPFail(reason string) bool {
	switch reason {
	case ReasonNoUpstream, ReasonDial, ReasonL2Write, ReasonL2Deadline, ReasonL2Response, ReasonL2Status, ReasonL2Auth, ReasonStaleTunnel:
		return true
	default:
		return false
	}
}

func knownHTTPResult(result string) bool {
	switch result {
	case ResultOK, ReasonDial, ReasonL2Write, ReasonL2Deadline, ReasonL2Response, ReasonL2Status, ReasonL2Auth, ReasonClientGone, ReasonStaleTunnel:
		return true
	default:
		return false
	}
}

func knownHTTPReject(reason string) bool {
	switch reason {
	case ReasonRejectAuth, ReasonRejectNoNode, ReasonRejectBadTarget, ReasonRejectBadCredential:
		return true
	default:
		return false
	}
}

// FailHTTP 增加 proxy_http_upstream_fail_total。
// reason 是七个上游失败值再加 stale_tunnel，不含 client_gone。其它字符串直接丢掉。
// 已经向客户端写出过源站字节时，调用方不再补 502，这个计数仍然加。
func FailHTTP(nodeID, reason string) {
	if !knownHTTPFail(reason) {
		return
	}
	proxyHTTPUpstreamFail.WithLabelValues(nodeID, reason).Inc()
}

// RejectHTTP 记录明文请求在拨 L2 之前的本地 400 或 407。
// reason 只取四个本地拒绝。没有 node_id 标签。
func RejectHTTP(reason string) {
	if !knownHTTPReject(reason) {
		return
	}
	proxyHTTPClientReject.WithLabelValues(reason).Inc()
}

// AbortHTTP 记录明文请求的 client_gone。其它 reason 直接丢掉。
func AbortHTTP(nodeID, reason string) {
	if reason != ReasonClientGone {
		return
	}
	proxyHTTPClientAbort.WithLabelValues(nodeID, reason).Inc()
}

// ObserveHTTP 记录一条明文请求从 started 到结果确定的毫秒数。
// result 是建连那八个再加 stale_tunnel。写错或 started 为零都不观察。一条请求只调用一次。
func ObserveHTTP(nodeID, result string, started time.Time) {
	if started.IsZero() || !knownHTTPResult(result) {
		return
	}
	elapsed := float64(time.Since(started)) / float64(time.Millisecond)
	proxyHTTPRequestLatency.WithLabelValues(nodeID, result).Observe(elapsed)
}

func knownHTTPSetupResult(result string) bool {
	switch result {
	case ResultOK, ReasonDial, ReasonL2Write, ReasonL2Deadline, ReasonL2Response, ReasonL2Status, ReasonL2Auth:
		return true
	default:
		return false
	}
}

// ObserveHTTPSetup 记录新隧道从拨号前到 L2 的 CONNECT 出结果的毫秒数。
// result 是 ok、dial、l2_write、l2_deadline、l2_response、l2_status、l2_auth。
// client_gone 和 stale_tunnel 不观察。复用隧道不调用。一条隧道只调用一次。
func ObserveHTTPSetup(nodeID, result string, started time.Time) {
	if started.IsZero() || !knownHTTPSetupResult(result) {
		return
	}
	elapsed := float64(time.Since(started)) / float64(time.Millisecond)
	proxyHTTPTunnelSetup.WithLabelValues(nodeID, result).Observe(elapsed)
}

// ObserveHTTPTunnel 记录一条明文 HTTP 的 L2 隧道从 L2 返回 200 到关闭的秒数。
// 中间空等下一条请求的时间算在里面。started 为零不观察。
func ObserveHTTPTunnel(nodeID string, started time.Time) {
	if started.IsZero() {
		return
	}
	proxyHTTPTunnelDuration.WithLabelValues(nodeID).Observe(time.Since(started).Seconds())
}

// AddHTTPBytes 累加明文请求的载荷。up 不含发往 L2 的 CONNECT。
// n <= 0 或 direction 不是 up/down 时不记。
func AddHTTPBytes(nodeID, direction string, n int64) {
	if n <= 0 || (direction != DirUp && direction != DirDown) {
		return
	}
	proxyHTTPTunnelBytes.WithLabelValues(nodeID, direction).Add(float64(n))
}

// AddHTTPReusedRetry 记录一次可重放请求因复用隧道被对端拆掉而重开。不进失败率。
func AddHTTPReusedRetry(nodeID string) {
	proxyHTTPReusedRetry.WithLabelValues(nodeID).Inc()
}

func knownBodyAbort(reason string) bool {
	switch reason {
	case ReasonAbortClientCancel, ReasonAbortOriginReset, ReasonAbortIdleTimeout, ReasonAbortRequestBody:
		return true
	default:
		return false
	}
}

// AddHTTPBodyAbort 记录最终状态行写出之后的正文或 101 隧道失败。不改 ok，不进失败率。
// reason 不在闭合集合里时直接丢掉，不创建序列。正常关闭不要调用。
func AddHTTPBodyAbort(nodeID, reason string) {
	if !knownBodyAbort(reason) {
		return
	}
	proxyHTTPBodyAbort.WithLabelValues(nodeID, reason).Inc()
}

// EnterHTTPTunnel 在 L2 的 CONNECT 返回 200 时调用。明文 HTTP 不向客户端写 L1 自己的 200。
// 返回值在这条 L2 隧道关闭时把 gauge 减一。keep-alive 复用期间不反复加减。
func EnterHTTPTunnel(nodeID string) (leave func()) {
	gauge := proxyHTTPTunnelActive.WithLabelValues(nodeID)
	gauge.Inc()
	return gauge.Dec
}

// SetL2Inventory 由路由表刷新时调用，不在单次 CONNECT 上调用。
// readyNodes 是至少有一个健康地址的 node_id 数，notReadyNodes 是表里没有健康地址的 node_id 数。
// healthyAddrs 是健康地址条数，不是去重后的公网 IP。
func SetL2Inventory(readyNodes, notReadyNodes, healthyAddrs int) {
	proxyL2Nodes.WithLabelValues(StateReady).Set(float64(readyNodes))
	proxyL2Nodes.WithLabelValues(StateNotReady).Set(float64(notReadyNodes))
	proxyL2HealthyAddrs.Set(float64(healthyAddrs))
}

// MeterEvent 是一条尝试的计量事件。认证失败不产生事件。
// Protocol 是 connect 或 http，只出现在事件里。用户名、目标主机、客户端地址同样不进 Prometheus 标签。
// NodeID 是用户名里请求的那一段，可以是路由表没有的值；Prometheus 的 node_id 仍然失败关闭。
type MeterEvent struct {
	V          int     `json:"v"`
	Protocol   string  `json:"protocol"`
	Result     string  `json:"result"`
	User       string  `json:"user,omitempty"`
	NodeID     string  `json:"node_id,omitempty"`
	TargetHost string  `json:"target_host,omitempty"`
	TargetPort string  `json:"target_port,omitempty"`
	ClientIP   string  `json:"client_ip,omitempty"`
	SetupMs    float64 `json:"setup_ms,omitempty"`
	RequestMs  float64 `json:"request_ms,omitempty"`
	TunnelSec  float64 `json:"tunnel_sec,omitempty"`
	BytesUp    int64   `json:"bytes_up,omitempty"`
	BytesDown  int64   `json:"bytes_down,omitempty"`
}

// TryEmit 非阻塞写入。队列满则丢弃并增加 proxy_meter_drop_total。
// v 固定写成 2。protocol 不是 connect 或 http 时丢掉，同样增加 proxy_meter_drop_total。
// Protocol 的 JSON 标签没有 omitempty，空字符串也会出现在行里；那种行在写入前就被丢掉。
// 没有 protocol 的 v=1 事件是旧日志，读的时候按 connect 处理；新事件不再写出那种行。
func TryEmit(ev MeterEvent) {
	if ev.Protocol != ProtocolConnect && ev.Protocol != ProtocolHTTP {
		proxyMeterDrop.Inc()
		return
	}
	ev.V = 2
	select {
	case meterEvents <- ev:
	default:
		proxyMeterDrop.Inc()
	}
}

// MeterEvents 返回唯一的事件队列。一个进程只能有一个消费者，不要 close。
func MeterEvents() <-chan MeterEvent {
	return meterEvents
}

// ServeMeter 把事件写成 JSON 行。生产环境把它接到独立日志；测试直接读 MeterEvents。
func ServeMeter(w io.Writer) {
	enc := json.NewEncoder(w)
	for ev := range meterEvents {
		_ = enc.Encode(ev)
	}
}
