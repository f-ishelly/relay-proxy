package proxy

import (
	"bufio"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/f-ishelly/relay-proxy/internal/docfixture/l1metrics"
)

// ErrUnknownNode 表示本地路由表从未收录该 node_id。
var ErrUnknownNode = errors.New("unknown node")

// ErrNoHealthyAddr 表示该 node_id 已在路由表里，但当前没有健康地址。
// 只有这个错误保留原来的 node_id 标签，reason 仍是 no_upstream。
var ErrNoHealthyAddr = errors.New("no healthy address")

// Resolver 按 node_id 给出一个健康的 L2 地址。
// 成功必须返回非空地址。返回 ("", nil) 按解析失败处理：502，node_id="unknown"，reason 为 empty_addr。
// 从未收录返回 ErrUnknownNode，reason 为 unknown_node。
// 已收录但没有健康地址返回 ErrNoHealthyAddr，保留原来的 node_id，reason 为 no_upstream。
// 其它错误（etcd 超时、watch 中断）是 resolver_error，node_id 为 unknown。
// 三种哨兵都不把用户名里的 ID 写进 Prometheus 标签。
// 经 errors.Join 同时带上 ErrNoHealthyAddr 的错误会留下原来的 ID，
// 例如 errors.Join(ErrUnknownNode, ErrNoHealthyAddr)。Resolver 不得把这两个哨兵 Join 在一起。
type Resolver interface {
	Resolve(nodeID string) (addr string, err error)
}

// classifyResolve 把 Resolve 的结果分成标签和 reason。
// 成功且地址非空时 reason 为空。否则返回要写入失败计数的 node_id 和 reason。
func classifyResolve(nodeID, addr string, err error) (label, reason string, ret error) {
	if err == nil && addr != "" {
		return nodeID, "", nil
	}
	if errors.Is(err, ErrNoHealthyAddr) {
		return l1metrics.NodeLabel(true, nodeID), l1metrics.ReasonNoUpstream, err
	}
	if errors.Is(err, ErrUnknownNode) {
		return l1metrics.NodeUnknown, l1metrics.ReasonUnknownNode, err
	}
	if err == nil {
		return l1metrics.NodeUnknown, l1metrics.ReasonEmptyAddr, errors.New("empty address")
	}
	return l1metrics.NodeUnknown, l1metrics.ReasonResolverError, err
}

// sharedSecret 是全系统那一份共享口令，与客户端登录 L1 以及 L2 的 auth.password 相同。
// 生产环境从配置注入；示例写成常量。
var sharedSecret = "s3cret"

// l2ResponseTimeout 是等待 L2 CONNECT 状态行的上限，部署时可改这个变量。
// gost 在拨通源站之后才回 200。2s 会把 2.5s 才拨通的源站写成 502。
// 默认 10s，覆盖一次较慢的源站拨号。读完应答后必须清掉，隧道拷贝不再受它限制。
var l2ResponseTimeout = 10 * time.Second

// halfCloseIdleTimeout 是半关闭之后，反方向连续没有读到字节的上限，部署时可改。
// 每次成功 Read 之前都把截止时间重新拨到现在 + 该值，所以是空闲超时，不是从 CloseWrite 起的一次性截止。
// 客户端先 FIN 与上游先 FIN 用的是同一套。默认 30s。
var halfCloseIdleTimeout = 30 * time.Second

// tunnelIdleTimeout 是整条隧道的空闲兜底，部署时可改。任一方向读到字节都重新计时。
// gost 的 idleTimeout 默认是 0（不限制），L1 不能靠它把两边都安静的隧道收掉。默认 5 分钟。
var tunnelIdleTimeout = 5 * time.Minute

// headerDeadline 是读请求头的绝对上限。等第一个字节用 tunnelIdle，不占用这段截止。
// 读到第一个字节之后设一次，之后的读不顺延。默认 10s。用来切断 slowloris。
// 一个字节都没有仍不记拒绝；已经读到一部分才到点记 header_timeout。
var headerDeadline = 10 * time.Second

// 配置里的时长上限。超过就拒绝这份配置，避免把空闲或请求头等待设成几天。
const (
	maxTunnelIdle     = time.Hour
	maxHalfCloseIdle  = time.Hour
	maxL2Response     = 2 * time.Minute
	maxHeaderDeadline = time.Minute
)

// dialUpstream 拨向选定 L2 的 HTTP 监听地址。默认 TCP，超时 3s。
var dialUpstream = func(addr string) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, 3*time.Second)
}

// errIdleExpired 是整条隧道的空闲计时到点、并且已经关掉两端之后的读错误。
// fire 在 Close 之前先置位。读到 net.ErrClosed 时改成这个错误，调用方记 idle_timeout，
// 不再把这次关闭记成 origin_reset 或 client_cancel。
var errIdleExpired = errors.New("tunnel idle timeout")

// errHalfCloseIdle 是半关闭之后反方向的空闲读超时。
// 一侧已经干净结束，这段等待是拆除，不是正文被截断，不进 proxy_http_body_abort_total。
var errHalfCloseIdle = errors.New("idle after half-close")

// proxyLimits 是一条连接使用的超时、重试开关和拨号函数。
// 包级变量只作为进程默认值。LimitsFromConfig 从 JSON 配置文件和命令行得到一份，
// 调用方把它传给 serveProxyConnLimited 或 handleConnect。请求路径不改包级变量。
// 配置文件示例：
// {"retry_put_delete":false,"tunnel_idle":"5m","half_close_idle":"30s","l2_response_timeout":"10s","header_deadline":"10s"}
// 命令行：-proxy-config、-retry-put-delete、-tunnel-idle、-half-close-idle、-l2-response-timeout、-header-deadline。
// half_close_idle 不得大于 tunnel_idle。未知 JSON 字段拒绝。
type proxyLimits struct {
	tunnelIdle     time.Duration
	halfCloseIdle  time.Duration
	l2Response     time.Duration
	headerDeadline time.Duration
	retryPutDelete bool
	dial           func(string) (net.Conn, error)
}

func defaultLimits() proxyLimits {
	return proxyLimits{
		tunnelIdle:     tunnelIdleTimeout,
		halfCloseIdle:  halfCloseIdleTimeout,
		l2Response:     l2ResponseTimeout,
		headerDeadline: headerDeadline,
		retryPutDelete: retryPutDelete,
		dial:           dialUpstream,
	}
}

// fillLimits 把没填的时长和拨号补成进程默认。retryPutDelete 以传入的值为准。
func fillLimits(l proxyLimits) proxyLimits {
	d := defaultLimits()
	if l.tunnelIdle <= 0 {
		l.tunnelIdle = d.tunnelIdle
	}
	if l.halfCloseIdle <= 0 {
		l.halfCloseIdle = d.halfCloseIdle
	}
	if l.l2Response <= 0 {
		l.l2Response = d.l2Response
	}
	if l.headerDeadline <= 0 {
		l.headerDeadline = d.headerDeadline
	}
	if l.dial == nil {
		l.dial = d.dial
	}
	return l
}

// LimitsFromConfig 先读 JSON 文件，再用命令行覆盖，返回一份 proxyLimits。
// 不写包级变量。path 非空、且 args 里没有 -proxy-config 时，用 path。
// -retry-put-delete 接受 true 或 false；命令行没写这项时，保留文件或默认值。
func LimitsFromConfig(path string, args []string) (proxyLimits, error) {
	lim := defaultLimits()
	fs := flag.NewFlagSet("proxy", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	cfgPath := fs.String("proxy-config", "", "JSON 配置文件")
	retryStr := fs.String("retry-put-delete", "", "true 则允许重试 PUT 和 DELETE")
	tunnelStr := fs.String("tunnel-idle", "", "整条隧道空闲兜底，例如 5m")
	halfStr := fs.String("half-close-idle", "", "半关闭后的空闲上限，例如 30s")
	l2Str := fs.String("l2-response-timeout", "", "等待 L2 CONNECT 状态行的上限，例如 10s")
	headStr := fs.String("header-deadline", "", "读请求头的绝对上限，例如 10s")
	if err := fs.Parse(args); err != nil {
		return proxyLimits{}, err
	}
	file := path
	if *cfgPath != "" {
		file = *cfgPath
	}
	if file != "" {
		b, err := os.ReadFile(file)
		if err != nil {
			return proxyLimits{}, err
		}
		dec := json.NewDecoder(strings.NewReader(string(b)))
		dec.DisallowUnknownFields()
		var cfg proxyFile
		if err := dec.Decode(&cfg); err != nil {
			return proxyLimits{}, err
		}
		if dec.More() {
			return proxyLimits{}, fmt.Errorf("trailing data after JSON object")
		}
		if err := cfg.apply(&lim); err != nil {
			return proxyLimits{}, err
		}
	}
	if *retryStr != "" {
		v, err := strconv.ParseBool(*retryStr)
		if err != nil {
			return proxyLimits{}, fmt.Errorf("retry-put-delete: %w", err)
		}
		lim.retryPutDelete = v
	}
	if err := applyDuration(&lim.tunnelIdle, *tunnelStr, "tunnel-idle", maxTunnelIdle); err != nil {
		return proxyLimits{}, err
	}
	if err := applyDuration(&lim.halfCloseIdle, *halfStr, "half-close-idle", maxHalfCloseIdle); err != nil {
		return proxyLimits{}, err
	}
	if err := applyDuration(&lim.l2Response, *l2Str, "l2-response-timeout", maxL2Response); err != nil {
		return proxyLimits{}, err
	}
	if err := applyDuration(&lim.headerDeadline, *headStr, "header-deadline", maxHeaderDeadline); err != nil {
		return proxyLimits{}, err
	}
	if lim.halfCloseIdle > lim.tunnelIdle {
		return proxyLimits{}, fmt.Errorf("half-close-idle %s must be <= tunnel-idle %s", lim.halfCloseIdle, lim.tunnelIdle)
	}
	return lim, nil
}

type proxyFile struct {
	RetryPutDelete *bool  `json:"retry_put_delete"`
	TunnelIdle     string `json:"tunnel_idle"`
	HalfCloseIdle  string `json:"half_close_idle"`
	L2Response     string `json:"l2_response_timeout"`
	HeaderDeadline string `json:"header_deadline"`
}

func (c proxyFile) apply(l *proxyLimits) error {
	if c.RetryPutDelete != nil {
		l.retryPutDelete = *c.RetryPutDelete
	}
	if err := applyDuration(&l.tunnelIdle, c.TunnelIdle, "tunnel_idle", maxTunnelIdle); err != nil {
		return err
	}
	if err := applyDuration(&l.halfCloseIdle, c.HalfCloseIdle, "half_close_idle", maxHalfCloseIdle); err != nil {
		return err
	}
	if err := applyDuration(&l.l2Response, c.L2Response, "l2_response_timeout", maxL2Response); err != nil {
		return err
	}
	return applyDuration(&l.headerDeadline, c.HeaderDeadline, "header_deadline", maxHeaderDeadline)
}

func applyDuration(dst *time.Duration, raw, name string, max time.Duration) error {
	if raw == "" {
		return nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if d <= 0 {
		return fmt.Errorf("%s must be > 0", name)
	}
	if d > max {
		return fmt.Errorf("%s must be <= %s", name, max)
	}
	*dst = d
	return nil
}

// username 格式示例: appuser@node-az01-003。
// node_id 取最后一个 '@' 之后的部分，用户名本身可以含 '@'（例如 user@corp@node1 → node1）。
func parseNodeID(username string) (user string, nodeID string, err error) {
	i := strings.LastIndex(username, "@")
	if i <= 0 || i == len(username)-1 {
		return "", "", fmt.Errorf("invalid username format")
	}
	user, nodeID = username[:i], username[i+1:]
	if strings.ContainsAny(nodeID, " \r\n/") {
		return "", "", fmt.Errorf("invalid username format")
	}
	return user, nodeID, nil
}

// sharedPasswordOK 先哈希再比较，避免口令长度不同时 ConstantTimeCompare 直接返回。
func sharedPasswordOK(password string) bool {
	got := sha256.Sum256([]byte(password))
	want := sha256.Sum256([]byte(sharedSecret))
	return subtle.ConstantTimeCompare(got[:], want[:]) == 1
}

// target 为客户端 CONNECT 的 authority，形如 host:port；IPv6 字面量保留方括号。
// password 与客户端登录 L1 的密码是同一份共享口令，不轮换。
func handleConnect(clientConn net.Conn, username, password, target string, resolver Resolver, lim proxyLimits) error {
	lim = fillLimits(lim)
	defer clientConn.Close()

	// 认证通过之后才发计量事件。握手耗时在结果确定时写入，不含后面的隧道时间。
	meter := l1metrics.MeterEvent{Protocol: l1metrics.ProtocolConnect}
	if ra := clientConn.RemoteAddr(); ra != nil {
		meter.ClientIP = ra.String()
	}
	var (
		meterOn        bool
		nodeID         string
		handshakeStart time.Time
		tunnelStart    time.Time
		bytesUp        atomic.Int64
		bytesDown      atomic.Int64
	)
	finish := func(result string) {
		meter.Result = result
		if handshakeStart.IsZero() {
			return
		}
		meter.SetupMs = float64(time.Since(handshakeStart)) / float64(time.Millisecond)
		l1metrics.ObserveConnect(nodeID, result, handshakeStart)
	}
	defer func() {
		if !meterOn {
			return
		}
		if !tunnelStart.IsZero() {
			meter.TunnelSec = time.Since(tunnelStart).Seconds()
			l1metrics.ObserveTunnel(nodeID, tunnelStart)
		}
		meter.BytesUp = bytesUp.Load()
		meter.BytesDown = bytesDown.Load()
		l1metrics.TryEmit(meter)
	}()

	// 隧道字节还没开始。400 / 407 / 502 都在这里写回客户端（见 §5.1、§5.2）。
	reply := func(status int, reason, header string, err error) error {
		msg := fmt.Sprintf("HTTP/1.1 %d %s\r\n", status, reason)
		if header != "" {
			msg += header + "\r\n"
		}
		msg += "\r\n"
		_, _ = io.WriteString(clientConn, msg)
		return err
	}
	badGateway := func(err error) error {
		return reply(http.StatusBadGateway, "Bad Gateway", "", err)
	}

	if strings.ContainsAny(username, "\r\n") || strings.ContainsAny(password, "\r\n") {
		l1metrics.RejectClient(l1metrics.ReasonRejectBadCredential)
		return reply(http.StatusBadRequest, "Bad Request", "", fmt.Errorf("invalid credential"))
	}
	if password == "" || !sharedPasswordOK(password) {
		l1metrics.RejectClient(l1metrics.ReasonRejectAuth)
		return reply(http.StatusProxyAuthRequired, "Proxy Authentication Required",
			`Proxy-Authenticate: Basic realm="relay"`, fmt.Errorf("invalid credential"))
	}
	meterOn = true

	var user string
	var err error
	user, nodeID, err = parseNodeID(username)
	if err != nil {
		finish(l1metrics.ReasonRejectNoNode)
		l1metrics.RejectClient(l1metrics.ReasonRejectNoNode)
		return reply(http.StatusBadRequest, "Bad Request", "", err)
	}
	meter.User = user
	// 事件保留请求的 node_id。Prometheus 只在 ErrNoHealthyAddr 时保留它。
	meter.NodeID = nodeID

	host, port, err := net.SplitHostPort(target)
	if err != nil || host == "" || strings.ContainsAny(host, " \r\n/") {
		finish(l1metrics.ReasonRejectBadTarget)
		l1metrics.RejectClient(l1metrics.ReasonRejectBadTarget)
		return reply(http.StatusBadRequest, "Bad Request", "", fmt.Errorf("invalid connect target"))
	}
	if _, err := strconv.Atoi(port); err != nil {
		finish(l1metrics.ReasonRejectBadTarget)
		l1metrics.RejectClient(l1metrics.ReasonRejectBadTarget)
		return reply(http.StatusBadRequest, "Bad Request", "", fmt.Errorf("invalid connect target"))
	}
	meter.TargetHost = host
	meter.TargetPort = port

	upstreamAddr, err := resolver.Resolve(nodeID)
	if label, reason, rerr := classifyResolve(nodeID, upstreamAddr, err); reason != "" {
		l1metrics.Fail502(label, reason)
		finish(reason)
		return badGateway(fmt.Errorf("resolve node %s failed: %w", nodeID, rerr))
	}

	handshakeStart = time.Now()
	upstreamConn, err := lim.dial(upstreamAddr)
	if err != nil {
		l1metrics.Fail502(nodeID, l1metrics.ReasonDial)
		finish(l1metrics.ReasonDial)
		return badGateway(fmt.Errorf("connect l2 failed: %w", err))
	}
	defer upstreamConn.Close()

	token := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
	req := "CONNECT " + target + " HTTP/1.1\r\n" +
		"Host: " + target + "\r\n" +
		"Proxy-Authorization: Basic " + token + "\r\n\r\n"
	if _, err := upstreamConn.Write([]byte(req)); err != nil {
		l1metrics.Fail502(nodeID, l1metrics.ReasonL2Write)
		finish(l1metrics.ReasonL2Write)
		return badGateway(err)
	}

	// 此后只有 clientPump 经 clientR 读 clientConn，隧道从 clientPipe 读，避免两个读端抢数据。
	// 等待 L2 时客户端读结束（EOF 或读错误），则关掉 upstream，让状态行读取立刻返回。
	// io.Copy 遇到干净 EOF 时返回的 err 是 nil，clientGone 仍然关闭。
	// 半关闭之后 idleConn 每次 Read 都重新拨空闲截止时间；读到字节则拨回整条隧道的兜底计时。
	idle := &tunnelIdle{client: clientConn, upstream: upstreamConn, timeout: lim.tunnelIdle}
	defer idle.stop()
	clientR := &idleConn{Conn: clientConn, onBytes: idle.touch, expired: &idle.expired, halfIdle: lim.halfCloseIdle}
	upstreamR := &idleConn{Conn: upstreamConn, onBytes: idle.touch, expired: &idle.expired, halfIdle: lim.halfCloseIdle}
	clientPipe, clientPumpW := io.Pipe()
	defer clientPipe.Close()
	clientGone := make(chan struct{})
	go func() {
		_, copyErr := io.Copy(clientPumpW, clientR)
		_ = clientPumpW.CloseWithError(copyErr)
		close(clientGone)
	}()

	type l2Status struct {
		code              int
		proxyAuthenticate string
		br                *bufio.Reader
		err               error
		deadline          bool
	}
	statusc := make(chan l2Status, 1)
	go func() {
		var st l2Status
		if st.err = upstreamConn.SetReadDeadline(time.Now().Add(lim.l2Response)); st.err != nil {
			st.deadline = true
			statusc <- st
			return
		}
		st.br = bufio.NewReader(upstreamR)
		st.code, st.proxyAuthenticate, st.err = readConnectStatus(st.br)
		if clearErr := upstreamConn.SetReadDeadline(time.Time{}); clearErr != nil && st.err == nil {
			st.err = clearErr
			st.deadline = true
		}
		statusc <- st
	}()
	var st l2Status
	var clientLeft bool
	select {
	case st = <-statusc:
	case <-clientGone:
		clientLeft = true
		upstreamConn.Close()
		st = <-statusc
	}
	// deadline 优先：SetReadDeadline 本身失败。其次状态行已解析则按状态码。
	// 只有先走到 clientGone，且状态行没解析出来，才是 client_gone。
	if st.err != nil {
		if st.deadline {
			l1metrics.Fail502(nodeID, l1metrics.ReasonL2Deadline)
			finish(l1metrics.ReasonL2Deadline)
			return badGateway(st.err)
		}
		if clientLeft {
			l1metrics.ClientAbort(nodeID, l1metrics.ReasonClientGone)
			finish(l1metrics.ReasonClientGone)
			return badGateway(st.err)
		}
		l1metrics.Fail502(nodeID, l1metrics.ReasonL2Response)
		finish(l1metrics.ReasonL2Response)
		return badGateway(st.err)
	}
	code, proxyAuthenticate, br := st.code, st.proxyAuthenticate, st.br
	if code == http.StatusProxyAuthRequired {
		challenge := proxyAuthenticate
		if challenge == "" || strings.ContainsAny(challenge, "\r\n") {
			challenge = `Basic realm="relay"`
		}
		l1metrics.Fail502(nodeID, l1metrics.ReasonL2Auth)
		finish(l1metrics.ReasonL2Auth)
		return reply(http.StatusProxyAuthRequired, "Proxy Authentication Required",
			"Proxy-Authenticate: "+challenge, fmt.Errorf("l2 connect status 407"))
	}
	if code != http.StatusOK {
		l1metrics.Fail502(nodeID, l1metrics.ReasonL2Status)
		finish(l1metrics.ReasonL2Status)
		return badGateway(fmt.Errorf("l2 connect status %d", code))
	}

	finish(l1metrics.ResultOK)
	if _, err := io.WriteString(clientConn, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		return err
	}
	defer l1metrics.EnterTunnel(nodeID)()
	tunnelStart = time.Now()

	// br 可能已经缓冲了与 200 粘在一起的后续字节，转发必须从 br 读，不能另起一次 net.Conn 读。
	// 从这里开始不再解析 HTTP，也不建立 TLS。io.Copy 会保持字节内容；是否换成 splice 不影响 JA3/JA4。
	// 读或写出错：先记下第一个真实错误，再完全关闭两端。另一方向因此读到 net.ErrClosed，
	// 那个错误是我们自己关出来的，不再覆盖已经记下的原因。
	// 干净 EOF：只 CloseWrite。反方向若已经堵在 Read 里，先拨一次空闲截止时间把它唤醒；
	// 之后每次 Read 再拨（见 idleConn）。连续 halfCloseIdle 没有字节才算超时。
	// n 是 io.Copy 已经写到对端的字节，含写到一半才出错的那一部分。不含 CONNECT 头和 200 行。
	idle.start()
	var (
		once    sync.Once
		kept    error
		closing atomic.Bool
	)
	done := make(chan struct{}, 2)
	record := func(err error) {
		if err == nil {
			return
		}
		if closing.Load() && errors.Is(err, net.ErrClosed) {
			return
		}
		once.Do(func() { kept = err })
		closing.Store(true)
		clientConn.Close()
		upstreamConn.Close()
	}
	relay := func(dst net.Conn, src io.Reader, reverse *idleConn, direction string, sink *atomic.Int64) {
		defer func() { done <- struct{}{} }()
		n, copyErr := io.Copy(dst, src)
		if n > 0 {
			sink.Add(n)
			l1metrics.AddTunnelBytes(nodeID, direction, n)
		}
		if copyErr != nil {
			record(copyErr)
			return
		}
		// closeWrite 失败不当成隧道中断，也不关另一端。对端经常已经先关了，
		// 这时再把两端都 Close，另一方向会读到 net.ErrClosed，把正常半关闭记成中断。
		// 明文 HTTP 的 pipeUpgrade 同样忽略这个错误。之后的空闲仍由 halfCloseIdle 收掉。
		// 半关闭一旦开始就停掉整条隧道的计时：tunnelIdle 先到点不再记 idle_timeout。
		_ = closeWrite(dst)
		reverse.armHalf(lim.halfCloseIdle)
		idle.stop()
	}
	// 客户端先 FIN：反方向是 upstreamR。上游先 FIN：反方向是 clientR。同一套空闲计时。
	go func() { relay(upstreamConn, clientPipe, upstreamR, l1metrics.DirUp, &bytesUp) }()
	go func() { relay(clientConn, br, clientR, l1metrics.DirDown, &bytesDown) }()
	<-done
	<-done
	// 任一侧已经半关闭时，空闲到点是拆除，不是隧道中断。
	if errors.Is(kept, errIdleExpired) && !clientR.halfClosed.Load() && !upstreamR.halfClosed.Load() {
		l1metrics.AddTunnelAbort(nodeID, l1metrics.ReasonAbortIdleTimeout)
	}
	return kept
}

// idleConn 在半关闭之后把读变成空闲超时：每次 Read 都先把截止时间拨到现在 + halfIdle。
// 读到字节时触碰整条隧道的兜底计时。半关闭之前不改截止时间，握手阶段的读超时仍然有效。
// expired 在 tunnelIdle.fire 关掉连接之前置位。这时的读错误是空闲到点，不是对端 RST。
type idleConn struct {
	net.Conn
	mu         sync.Mutex
	halfClosed atomic.Bool
	onBytes    func()
	expired    *atomic.Bool
	halfIdle   time.Duration
}

func (c *idleConn) armHalf(d time.Duration) {
	c.mu.Lock()
	c.halfClosed.Store(true)
	if d <= 0 {
		d = halfCloseIdleTimeout
	}
	_ = c.Conn.SetReadDeadline(time.Now().Add(d))
	c.mu.Unlock()
}

func (c *idleConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	if c.halfClosed.Load() {
		d := c.halfIdle
		if d <= 0 {
			d = halfCloseIdleTimeout
		}
		_ = c.Conn.SetReadDeadline(time.Now().Add(d))
	}
	c.mu.Unlock()
	n, err := c.Conn.Read(p)
	if n > 0 && c.onBytes != nil {
		c.onBytes()
	}
	if err == nil {
		return n, nil
	}
	// 半关闭优先于整条隧道的空闲：计时器先关掉连接时也不记 idle_timeout。
	if c.halfClosed.Load() && (isTimeout(err) || (c.expired != nil && c.expired.Load())) {
		return n, errHalfCloseIdle
	}
	if c.expired != nil && c.expired.Load() {
		return n, errIdleExpired
	}
	return n, err
}

// tunnelIdle 在任一方向读到字节时把关闭时刻推后 timeout。
// 到点仍没有新字节，先记下 expired，再同时关掉客户端和上游。
type tunnelIdle struct {
	client, upstream net.Conn
	mu               sync.Mutex
	timer            *time.Timer
	stopped          bool
	expired          atomic.Bool
	timeout          time.Duration
}

func (t *tunnelIdle) start() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return
	}
	d := t.timeout
	if d <= 0 {
		d = tunnelIdleTimeout
	}
	t.timer = time.AfterFunc(d, t.fire)
}

func (t *tunnelIdle) touch() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.timer == nil || t.stopped {
		return
	}
	d := t.timeout
	if d <= 0 {
		d = tunnelIdleTimeout
	}
	t.timer.Reset(d)
}

func (t *tunnelIdle) fire() {
	t.mu.Lock()
	if t.stopped {
		t.mu.Unlock()
		return
	}
	t.stopped = true
	t.expired.Store(true)
	t.mu.Unlock()
	t.client.Close()
	t.upstream.Close()
}

func (t *tunnelIdle) stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stopped = true
	if t.timer != nil {
		t.timer.Stop()
	}
}

// closeWrite 关闭写端，使对端读到 EOF，同时保留本端读端。不是 TCP 连接时退化为完全关闭。
func closeWrite(conn net.Conn) error {
	if cw, ok := conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return conn.Close()
}

func readConnectStatus(r *bufio.Reader) (int, string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return 0, "", err
	}
	parts := strings.SplitN(strings.TrimSpace(line), " ", 3)
	if len(parts) < 2 {
		return 0, "", fmt.Errorf("bad status line")
	}
	code, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, "", err
	}
	var proxyAuthenticate string
	for i := 0; i < 64; i++ {
		line, err = r.ReadString('\n')
		if err != nil {
			return 0, "", err
		}
		if line == "\r\n" || line == "\n" {
			return code, proxyAuthenticate, nil
		}
		key, val, ok := strings.Cut(strings.TrimRight(line, "\r\n"), ":")
		if ok && proxyAuthenticate == "" && strings.EqualFold(strings.TrimSpace(key), "Proxy-Authenticate") {
			proxyAuthenticate = strings.TrimSpace(val)
		}
	}
	return 0, "", fmt.Errorf("l2 response headers too long")
}
