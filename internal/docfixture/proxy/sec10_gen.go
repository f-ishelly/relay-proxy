//go:build docfixture

package proxy

import (
	"bufio"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
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
// 只有这个错误保留原来的 node_id 标签。
var ErrNoHealthyAddr = errors.New("no healthy address")

// Resolver 按 node_id 给出一个健康的 L2 地址。
// 成功必须返回非空地址。返回 ("", nil) 按解析失败处理：502，node_id="unknown"，reason 为 no_upstream。
// 从未收录返回 ErrUnknownNode；已收录但没有健康地址返回 ErrNoHealthyAddr。
// 调用方只在 errors.Is(err, ErrNoHealthyAddr) 时把原来的 node_id 写入标签。
// 其他错误，包括 ErrUnknownNode 和任何未预期的错误，都记 node_id="unknown"。
// 这两种失败的 reason 都是 no_upstream。
// 经 errors.Join 同时带上 ErrNoHealthyAddr 的错误也会留下原来的 ID，
// 例如 errors.Join(ErrUnknownNode, ErrNoHealthyAddr)。Resolver 不得把这两个哨兵 Join 在一起。
type Resolver interface {
	Resolve(nodeID string) (addr string, err error)
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

// dialUpstream 拨向选定 L2 的 HTTP 监听地址。默认 TCP，超时 3s。
var dialUpstream = func(addr string) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, 3*time.Second)
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
func handleConnect(clientConn net.Conn, username, password, target string, resolver Resolver) error {
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
		meter.HandshakeMs = float64(time.Since(handshakeStart)) / float64(time.Millisecond)
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
	// 事件保留请求的 node_id。Prometheus 只在 ErrNoHealthyAddr 时保留它，其它解析失败记 unknown。
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
	if err != nil || upstreamAddr == "" {
		inTable := errors.Is(err, ErrNoHealthyAddr)
		l1metrics.Fail502(l1metrics.NodeLabel(inTable, nodeID), l1metrics.ReasonNoUpstream)
		finish(l1metrics.ReasonNoUpstream)
		if err == nil {
			err = errors.New("empty address")
		}
		return badGateway(fmt.Errorf("resolve node %s failed: %w", nodeID, err))
	}

	handshakeStart = time.Now()
	upstreamConn, err := dialUpstream(upstreamAddr)
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
	idle := &tunnelIdle{client: clientConn, upstream: upstreamConn}
	defer idle.stop()
	clientR := &idleConn{Conn: clientConn, onBytes: idle.touch}
	upstreamR := &idleConn{Conn: upstreamConn, onBytes: idle.touch}
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
		if st.err = upstreamConn.SetReadDeadline(time.Now().Add(l2ResponseTimeout)); st.err != nil {
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
	// 读或写出错：完全关闭两端，另一方向的 io.Copy 会立刻返回。
	// 干净 EOF：只 CloseWrite。反方向若已经堵在 Read 里，先拨一次空闲截止时间把它唤醒；
	// 之后每次 Read 再拨（见 idleConn）。连续 halfCloseIdleTimeout 没有字节才算超时。
	// n 是 io.Copy 已经写到对端的字节，含写到一半才出错的那一部分。不含 CONNECT 头和 200 行。
	idle.start()
	errc := make(chan error, 2)
	relay := func(dst net.Conn, src io.Reader, reverse *idleConn, direction string, sink *atomic.Int64) {
		n, copyErr := io.Copy(dst, src)
		if n > 0 {
			sink.Add(n)
			l1metrics.AddTunnelBytes(nodeID, direction, n)
		}
		if copyErr != nil {
			clientConn.Close()
			upstreamConn.Close()
			errc <- copyErr
			return
		}
		if werr := closeWrite(dst); werr != nil {
			clientConn.Close()
			upstreamConn.Close()
			errc <- werr
			return
		}
		reverse.halfClosed.Store(true)
		_ = reverse.SetReadDeadline(time.Now().Add(halfCloseIdleTimeout))
		errc <- nil
	}
	// 客户端先 FIN：反方向是 upstreamR。上游先 FIN：反方向是 clientR。同一套空闲计时。
	go func() { relay(upstreamConn, clientPipe, upstreamR, l1metrics.DirUp, &bytesUp) }()
	go func() { relay(clientConn, br, clientR, l1metrics.DirDown, &bytesDown) }()
	err1 := <-errc
	err2 := <-errc
	if err1 != nil {
		return err1
	}
	return err2
}

// idleConn 在半关闭之后把读变成空闲超时：每次 Read 都先把截止时间拨到现在 + halfCloseIdleTimeout。
// 读到字节时触碰整条隧道的兜底计时。半关闭之前不改截止时间，握手阶段的读超时仍然有效。
type idleConn struct {
	net.Conn
	halfClosed atomic.Bool
	onBytes    func()
}

func (c *idleConn) Read(p []byte) (int, error) {
	if c.halfClosed.Load() {
		_ = c.Conn.SetReadDeadline(time.Now().Add(halfCloseIdleTimeout))
	}
	n, err := c.Conn.Read(p)
	if n > 0 && c.onBytes != nil {
		c.onBytes()
	}
	return n, err
}

// tunnelIdle 在任一方向读到字节时把关闭时刻推后 tunnelIdleTimeout。
// 到点仍没有新字节，就同时关掉客户端和上游。
type tunnelIdle struct {
	client, upstream net.Conn
	mu               sync.Mutex
	timer            *time.Timer
	stopped          bool
}

func (t *tunnelIdle) start() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return
	}
	t.timer = time.AfterFunc(tunnelIdleTimeout, t.fire)
}

func (t *tunnelIdle) touch() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.timer == nil || t.stopped {
		return
	}
	t.timer.Reset(tunnelIdleTimeout)
}

func (t *tunnelIdle) fire() {
	t.mu.Lock()
	if t.stopped {
		t.mu.Unlock()
		return
	}
	t.stopped = true
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
