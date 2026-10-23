//go:build docfixture

package proxy

import (
	"bufio"
	"bytes"
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
	"syscall"
	"time"

	"github.com/f-ishelly/relay-proxy/internal/docfixture/l1metrics"
)

// maxHTTPHead 是单条请求或响应头部的上限。超过则本地 400，reason 为 bad_target。
// 正文不设这个上限，按 Content-Length 或 chunked 原样转发。
const maxHTTPHead = 64 << 10

// serveProxyConn 是客户端连接的入口。第一条请求的方法是 CONNECT 时交给 handleConnect；
// 否则按明文 HTTP 处理，并在这条连接上串行读取后续请求。
// 不支持把后续请求并发转发。bufio 里已经读到的下一个请求，要等当前响应写完再处理。
func serveProxyConn(conn net.Conn, resolver Resolver) error {
	return serveProxyConnLimited(conn, resolver, defaultLimits())
}

// serveProxyConnLimited 与 serveProxyConn 相同，超时和重试开关来自这一次的 lim。
// 测试和 LimitsFromConfig 的结果走这里，不改包级变量。
func serveProxyConnLimited(conn net.Conn, resolver Resolver, lim proxyLimits) error {
	lim = fillLimits(lim)
	defer conn.Close()
	session := newHTTPSession(conn, lim)
	defer session.stop()
	mc := &modeConn{Conn: conn, session: session}
	br := bufio.NewReaderSize(mc, maxHTTPHead)
	head, err := readRequestHead(mc, br)
	if err != nil {
		// 一个字节都没有：不计数。零字节的空闲超时也在这里，不记拒绝。
		// 已经读到一部分才超时：slowloris，记 header_timeout。
		// 已经读到一部分但不是超时：头不完整，记 bad_target。
		// 请求行已经是 CONNECT 时，这两项走 CONNECT 的拒绝计数。
		if len(head) == 0 {
			return err
		}
		notePartialHead(head, err)
		writeHTTPError(conn, http.StatusBadRequest, "Bad Request", "")
		return err
	}
	if requestMethod(head) == "CONNECT" {
		// CONNECT 隧道由 handleConnect 自己计时空闲。这里停掉明文 HTTP 的计时，
		// 并停止 modeConn 在每次 Read 时覆盖截止时间。
		session.stop()
		mc.disabled.Store(true)
		_ = conn.SetReadDeadline(time.Time{})
		return finishConnect(mc, br, head, resolver)
	}
	return serveAbsolute(conn, session, mc, br, head, resolver)
}

// finishConnect 把头里已经消费的 CONNECT 控制面交给 handleConnect。
// bufio 若多读了隧道字节，用 prefixConn 放回去，避免 ClientHello 被吃掉。
func finishConnect(mc *modeConn, br *bufio.Reader, head []byte, resolver Resolver) error {
	start, headers, err := splitHead(head)
	if err != nil {
		l1metrics.RejectClient(l1metrics.ReasonRejectBadTarget)
		writeHTTPError(mc, http.StatusBadRequest, "Bad Request", "")
		return err
	}
	parts := strings.Split(start, " ")
	if len(parts) != 3 || parts[0] != "CONNECT" || parts[1] == "" {
		l1metrics.RejectClient(l1metrics.ReasonRejectBadTarget)
		writeHTTPError(mc, http.StatusBadRequest, "Bad Request", "")
		return fmt.Errorf("bad connect request")
	}
	user, pass := basicFrom(headers)
	prefix := takeBuffered(br)
	pc := &prefixConn{Conn: mc, r: io.MultiReader(bytes.NewReader(prefix), mc)}
	return handleConnect(pc, user, pass, parts[1], resolver, mc.session.lim)
}

// httpTry 是一条明文 HTTP 请求的计数，不是一条 L2 隧道。
// keep-alive 上的下一条请求是新的 httpTry。认证失败不发计量事件。
type httpTry struct {
	meter          l1metrics.MeterEvent
	meterOn        bool
	nodeID         string
	started        time.Time
	up, down       atomic.Int64
	observed       bool
	emitted        bool
	retried        bool
	retryPutDelete bool
}

func newHTTPTry(client net.Conn, lim proxyLimits) *httpTry {
	t := &httpTry{retryPutDelete: lim.retryPutDelete}
	t.meter.Protocol = l1metrics.ProtocolHTTP
	if ra := client.RemoteAddr(); ra != nil {
		t.meter.ClientIP = ra.String()
	}
	return t
}

func (t *httpTry) addUp(n int) {
	if t == nil || n <= 0 {
		return
	}
	t.up.Add(int64(n))
	l1metrics.AddHTTPBytes(t.nodeID, l1metrics.DirUp, int64(n))
}

func (t *httpTry) addDown(n int) {
	if t == nil || n <= 0 {
		return
	}
	t.down.Add(int64(n))
	l1metrics.AddHTTPBytes(t.nodeID, l1metrics.DirDown, int64(n))
}

// observe 在结果确定时调用一次。started 为零说明还没拨号、也还没开始写复用隧道，只记结果，不进直方图。
// request_ms 从 started 起算。重试不重置 started，所以白写的那一次算在同一次 request_ms 里。
func (t *httpTry) observe(result string) {
	if t == nil || t.observed {
		return
	}
	t.observed = true
	t.meter.Result = result
	if t.started.IsZero() {
		return
	}
	t.meter.RequestMs = float64(time.Since(t.started)) / float64(time.Millisecond)
	l1metrics.ObserveHTTP(t.nodeID, result, t.started)
}

func (t *httpTry) failUpstream(promNode, reason string) {
	if t == nil || t.observed {
		return
	}
	l1metrics.FailHTTP(promNode, reason)
	t.observe(reason)
}

// failOpen 记录新隧道这次 CONNECT 的失败。请求记一次，setup 用这一次拨号前的时刻再记一次。
// 重试时不改请求自己的 started。no_upstream 发生在拨号前，不走这里。
// 事件里的 setup_ms 只覆盖这一次隧道，和 ObserveHTTPSetup 同一段钟。
func (t *httpTry) failOpen(promNode, reason string, setupStart time.Time) {
	if t == nil {
		return
	}
	t.noteSetup(setupStart)
	t.failUpstream(promNode, reason)
	if !setupStart.IsZero() {
		l1metrics.ObserveHTTPSetup(t.nodeID, reason, setupStart)
	}
}

// noteSetup 把这一次新隧道的建立耗时写进事件。重试时覆盖，只留下实际送出这条请求的隧道。
// 死掉的复用隧道不在这里：它的 setup 在它自己建成时已经 ObserveHTTPSetup。
// setup_ms 不含源站时间，也不含被丢掉的那次尝试。request_ms 才含白写的时间。
func (t *httpTry) noteSetup(setupStart time.Time) {
	if t == nil || setupStart.IsZero() {
		return
	}
	t.meter.SetupMs = float64(time.Since(setupStart)) / float64(time.Millisecond)
}

func (t *httpTry) clientGone() {
	if t == nil || t.observed {
		return
	}
	l1metrics.AbortHTTP(t.nodeID, l1metrics.ReasonClientGone)
	t.observe(l1metrics.ReasonClientGone)
}

func (t *httpTry) emit() {
	if t == nil || t.emitted || !t.meterOn || t.meter.Result == "" {
		return
	}
	t.emitted = true
	t.meter.BytesUp = t.up.Load()
	t.meter.BytesDown = t.down.Load()
	l1metrics.TryEmit(t.meter)
}

// serveAbsolute 处理 absolute-form 明文 HTTP。同一条客户端连接上：
// 同一 node_id 且同一 host:port、双方都还想 keep-alive 时，复用已经建立的 L2 隧道；
// 主机或 node 变了，就关掉隧道再向新的 L2 发 CONNECT。
// 每条请求一个 httpTry。复用隧道不再拨号，延迟从写出请求起算。
// 复用的隧道若在源站字节之前被对端拆掉，且方法默认可重放、头没有写到一半，就重开一次；否则在断管时记 stale_tunnel。读超时不重开。
func serveAbsolute(client net.Conn, session *httpSession, mc *modeConn, br *bufio.Reader, head []byte, resolver Resolver) error {
	var held *upstream
	defer func() {
		if held != nil {
			held.close(session)
		}
	}()
	var try *httpTry
	defer func() { try.emit() }()
	for {
		try.emit()
		try = nil
		if head == nil {
			var err error
			head, err = readRequestHead(mc, br)
			if err != nil {
				if len(head) == 0 {
					return err
				}
				notePartialHead(head, err)
				writeHTTPError(client, http.StatusBadRequest, "Bad Request", "")
				return err
			}
		}
		if requestMethod(head) == "CONNECT" {
			l1metrics.RejectHTTP(l1metrics.ReasonRejectBadTarget)
			writeHTTPError(client, http.StatusBadRequest, "Bad Request", "")
			return fmt.Errorf("connect on http connection")
		}
		req, rej := parseAbsolute(head)
		head = nil
		if rej != nil {
			l1metrics.RejectHTTP(rej.metric)
			writeHTTPError(client, rej.status, rej.reason, rej.header)
			return rej.err
		}
		try = newHTTPTry(client, session.lim)
		if strings.ContainsAny(req.username, "\r\n") || strings.ContainsAny(req.password, "\r\n") {
			l1metrics.RejectHTTP(l1metrics.ReasonRejectBadCredential)
			writeHTTPError(client, http.StatusBadRequest, "Bad Request", "")
			return fmt.Errorf("invalid credential")
		}
		if !sharedPasswordOK(req.password) {
			l1metrics.RejectHTTP(l1metrics.ReasonRejectAuth)
			writeHTTPError(client, http.StatusProxyAuthRequired, "Proxy Authentication Required",
				`Proxy-Authenticate: Basic realm="relay"`)
			return fmt.Errorf("invalid credential")
		}
		try.meterOn = true
		user, nodeID, err := parseNodeID(req.username)
		try.meter.User = user
		try.meter.NodeID = nodeID
		try.nodeID = nodeID
		if err != nil {
			l1metrics.RejectHTTP(l1metrics.ReasonRejectNoNode)
			try.observe(l1metrics.ReasonRejectNoNode)
			writeHTTPError(client, http.StatusBadRequest, "Bad Request", "")
			return err
		}
		if req.targetErr != nil {
			l1metrics.RejectHTTP(l1metrics.ReasonRejectBadTarget)
			try.observe(l1metrics.ReasonRejectBadTarget)
			writeHTTPError(client, http.StatusBadRequest, "Bad Request", "")
			return req.targetErr
		}
		if host, port, serr := net.SplitHostPort(req.originTarget); serr == nil {
			try.meter.TargetHost = host
			try.meter.TargetPort = port
		}
		if held != nil && (held.nodeID != nodeID || held.target != req.originTarget) {
			held.close(session)
			held = nil
		}
		reused := held != nil
		if held == nil {
			held, err = openUpstream(session, req.username, req.password, nodeID, req.originTarget, resolver, client, try)
			if err != nil {
				held = nil
				return err
			}
		}
		outcome, err := held.relay(client, mc, br, req, try, reused)
		if outcome.stale {
			// 第一次不记失败，也不 ObserveHTTP。关掉旧隧道（时长和 gauge 各结清一次）后重开。
			// 请求的 started 保持不变，握手时间含这次白走的尝试。
			held.close(session)
			held = nil
			l1metrics.AddHTTPReusedRetry(nodeID)
			try.retried = true
			held, err = openUpstream(session, req.username, req.password, nodeID, req.originTarget, resolver, client, try)
			if err != nil {
				held = nil
				return err
			}
			outcome, err = held.relay(client, mc, br, req, try, false)
		}
		if err != nil {
			held.close(session)
			held = nil
			return err
		}
		if !outcome.reuse {
			held.close(session)
			held = nil
		}
		if outcome.endClient {
			return nil
		}
	}
}

// absReq 是已经决定要转发的明文请求。wireHead 是写给源站的头部字节。
type absReq struct {
	method         string
	username       string
	password       string
	originTarget   string
	wireHead       []byte
	chunked        bool
	contentLength  int64
	expectContinue bool
	upgrade        bool
	closeClient    bool
	targetErr      error
}

type httpReject struct {
	status int
	reason string
	header string
	metric string
	err    error
}

type relayOutcome struct {
	reuse     bool
	endClient bool
	stale     bool
}

// parseAbsolute 只接受 http 的 absolute-form。https 的明文请求、origin-form 都记为坏目标。
// 口令是否正确不在这里判断，调用方按与 CONNECT 相同的顺序处理。
func parseAbsolute(head []byte) (*absReq, *httpReject) {
	start, headers, err := splitHead(head)
	if err != nil {
		return nil, &httpReject{
			status: http.StatusBadRequest, reason: "Bad Request",
			metric: l1metrics.ReasonRejectBadTarget, err: err,
		}
	}
	parts := strings.Split(start, " ")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || (parts[2] != "HTTP/1.0" && parts[2] != "HTTP/1.1") {
		return nil, &httpReject{
			status: http.StatusBadRequest, reason: "Bad Request",
			metric: l1metrics.ReasonRejectBadTarget, err: fmt.Errorf("bad request line"),
		}
	}
	method, rawTarget, version := parts[0], parts[1], parts[2]
	user, pass := basicFrom(headers)

	req := &absReq{
		method: method, username: user, password: pass, contentLength: -1,
	}
	originForm, hostport, terr := parseHTTPAbsolute(rawTarget)
	if terr != nil {
		req.targetErr = terr
		return req, nil
	}
	req.originTarget = hostport

	connToks, proxyToks, chunked, clValues, expect, hasUpgrade, syntaxErr := scanHeaders(headers)
	if syntaxErr != nil {
		return nil, &httpReject{
			status: http.StatusBadRequest, reason: "Bad Request",
			metric: l1metrics.ReasonRejectBadTarget, err: syntaxErr,
		}
	}
	if len(clValues) > 0 {
		n, perr := strconv.ParseInt(clValues[0], 10, 64)
		if perr != nil || n < 0 {
			req.targetErr = fmt.Errorf("bad content-length")
			return req, nil
		}
		for _, v := range clValues[1:] {
			if v != clValues[0] {
				req.targetErr = fmt.Errorf("conflicting content-length")
				return req, nil
			}
		}
		req.contentLength = n
	}
	req.chunked = chunked
	req.expectContinue = expect && (chunked || req.contentLength > 0)
	req.upgrade = hasUpgrade && hasToken(connToks, "upgrade")
	req.closeClient = wantsClose(version, connToks, proxyToks)
	req.wireHead = rewriteHead(method, originForm, version, headers, chunked, req.upgrade, connToks)
	return req, nil
}

func scanHeaders(headers [][]byte) (connToks, proxyToks []string, chunked bool, clValues []string, expect, hasUpgrade bool, err error) {
	for _, ln := range headers {
		name, _, ok := headerName(ln)
		if !ok {
			return nil, nil, false, nil, false, false, fmt.Errorf("bad header")
		}
		val := headerValue(ln)
		switch strings.ToLower(string(name)) {
		case "connection":
			connToks = append(connToks, splitTokens(val)...)
		case "proxy-connection":
			proxyToks = append(proxyToks, splitTokens(val)...)
		case "transfer-encoding":
			if hasToken(splitTokens(val), "chunked") {
				chunked = true
			}
		case "content-length":
			clValues = append(clValues, strings.TrimSpace(val))
		case "expect":
			if hasToken(splitTokens(val), "100-continue") {
				expect = true
			}
		case "upgrade":
			if strings.TrimSpace(val) != "" {
				hasUpgrade = true
			}
		}
	}
	return connToks, proxyToks, chunked, clValues, expect, hasUpgrade, nil
}

// rewriteHead 把请求行改成 origin-form，删掉逐跳头，其余头行按原字节顺序和大小写保留。
// 不添加 Via、X-Forwarded-For、Forwarded、User-Agent。
// Connection 是唯一可能改值的头：只删掉那些指向已被删除头的 token。token 集合没变时整行不动。
func rewriteHead(method, originForm, version string, headers [][]byte, chunked, upgrade bool, connToks []string) []byte {
	drop := map[string]bool{
		"proxy-authorization": true,
		"proxy-connection":    true,
		"keep-alive":          true,
		"te":                  true,
		"trailer":             true,
		"proxy-authenticate":  true,
	}
	if chunked {
		// 同时带 Content-Length 和 chunked 时删掉 Content-Length，避免请求走私。
		drop["content-length"] = true
	}
	for _, t := range connToks {
		lt := strings.ToLower(t)
		switch lt {
		case "close", "keep-alive", "upgrade", "transfer-encoding":
			continue
		default:
			drop[lt] = true
		}
	}
	delete(drop, "transfer-encoding")
	if upgrade {
		delete(drop, "upgrade")
	}

	var b bytes.Buffer
	fmt.Fprintf(&b, "%s %s %s\r\n", method, originForm, version)
	for _, ln := range headers {
		name, _, ok := headerName(ln)
		if !ok {
			continue
		}
		lname := strings.ToLower(string(name))
		if drop[lname] {
			continue
		}
		if lname == "connection" {
			kept := filterConnTokens(headerValue(ln), drop)
			if len(kept) == 0 {
				continue
			}
			if sameTokens(headerValue(ln), kept) {
				b.Write(ln)
			} else {
				b.Write(name)
				b.WriteString(": ")
				b.WriteString(strings.Join(kept, ", "))
				b.WriteString("\r\n")
			}
			continue
		}
		b.Write(ln)
	}
	b.WriteString("\r\n")
	return b.Bytes()
}

func filterConnTokens(val string, drop map[string]bool) []string {
	var kept []string
	for _, t := range splitTokens(val) {
		lt := strings.ToLower(t)
		switch lt {
		case "close", "keep-alive", "upgrade", "transfer-encoding":
			kept = append(kept, t)
			continue
		}
		if drop[lt] {
			continue
		}
		kept = append(kept, t)
	}
	return kept
}

func sameTokens(val string, kept []string) bool {
	orig := splitTokens(val)
	if len(orig) != len(kept) {
		return false
	}
	for i := range orig {
		if orig[i] != kept[i] {
			return false
		}
	}
	return true
}

func wantsClose(version string, connToks, proxyToks []string) bool {
	if hasToken(connToks, "close") || hasToken(proxyToks, "close") {
		return true
	}
	if version == "HTTP/1.0" {
		return !(hasToken(connToks, "keep-alive") || hasToken(proxyToks, "keep-alive"))
	}
	return false
}

// parseHTTPAbsolute 手工切 absolute-form，不经过 net/url，避免百分号解码和主机名大小写变化。
// 缺端口时用 80。userinfo 和 fragment 不进入 CONNECT 目标，也不进入 origin-form。
func parseHTTPAbsolute(raw string) (originForm, hostport string, err error) {
	i := strings.Index(raw, "://")
	if i <= 0 || !strings.EqualFold(raw[:i], "http") {
		return "", "", fmt.Errorf("not http absolute-form")
	}
	rest := raw[i+3:]
	if hash := strings.IndexByte(rest, '#'); hash >= 0 {
		rest = rest[:hash]
	}
	var authority, pathq string
	if s := strings.IndexAny(rest, "/?"); s >= 0 {
		authority = rest[:s]
		pathq = rest[s:]
		if pathq[0] == '?' {
			pathq = "/" + pathq
		}
	} else {
		authority = rest
		pathq = "/"
	}
	if authority == "" {
		return "", "", fmt.Errorf("missing host")
	}
	if at := strings.LastIndex(authority, "@"); at >= 0 {
		authority = authority[at+1:]
	}
	hostport, err = splitHostPortDefault(authority, "80")
	if err != nil {
		return "", "", err
	}
	return pathq, hostport, nil
}

func splitHostPortDefault(authority, defPort string) (string, error) {
	var host, port string
	if strings.HasPrefix(authority, "[") {
		end := strings.IndexByte(authority, ']')
		if end <= 1 {
			return "", fmt.Errorf("bad ipv6 host")
		}
		host = authority[:end+1]
		rest := authority[end+1:]
		switch {
		case rest == "":
			port = defPort
		case strings.HasPrefix(rest, ":"):
			port = rest[1:]
		default:
			return "", fmt.Errorf("bad ipv6 host")
		}
	} else {
		h, p, e := net.SplitHostPort(authority)
		if e != nil {
			if strings.Contains(authority, ":") {
				return "", fmt.Errorf("bad host")
			}
			host = authority
			port = defPort
		} else {
			host = h
			port = p
		}
	}
	if host == "" || host == "[]" || strings.ContainsAny(host, " \r\n/") {
		return "", fmt.Errorf("bad host")
	}
	pn, err := strconv.Atoi(port)
	if err != nil || pn < 1 || pn > 65535 {
		return "", fmt.Errorf("bad port")
	}
	if strings.HasPrefix(host, "[") {
		return host + ":" + port, nil
	}
	return net.JoinHostPort(host, port), nil
}

// upstream 是一条已经拿到 L2 CONNECT 200 的隧道，后面只搬运 HTTP 字节。
// opened 是 L2 返回 200 的时刻。明文 HTTP 不向客户端写 200，gauge 从这里加一。
type upstream struct {
	nodeID string
	target string
	conn   net.Conn
	br     *bufio.Reader
	mode   *modeConn
	leave  func()
	opened time.Time
}

func (u *upstream) close(session *httpSession) {
	if u == nil {
		return
	}
	if u.leave != nil {
		u.leave()
		u.leave = nil
	}
	if !u.opened.IsZero() {
		l1metrics.ObserveHTTPTunnel(u.nodeID, u.opened)
		u.opened = time.Time{}
	}
	u.conn.Close()
	if session != nil {
		session.clearUpstream(u.conn)
	}
}

// openUpstream 向选定 L2 发与 CONNECT 数据面相同的第二次 CONNECT。
// 失败时的状态码与 §5.6 相同，计数走明文 HTTP 的指标，不调用 Fail502、ObserveConnect 或 EnterTunnel。
// 请求的延迟从第一次拨号前开始；重试不覆盖 started。setup 用这一次自己的 setupStart，
// 在 CONNECT 出结果时记一次：失败走 failOpen，成功在返回隧道之前记 ok。L2 返回 200 时请求还没结束，不在这里记请求的 ok。
func openUpstream(session *httpSession, username, password, nodeID, target string, resolver Resolver, client net.Conn, try *httpTry) (*upstream, error) {
	addr, err := resolver.Resolve(nodeID)
	if label, reason, rerr := classifyResolve(nodeID, addr, err); reason != "" {
		try.failUpstream(label, reason)
		writeHTTPError(client, http.StatusBadGateway, "Bad Gateway", "")
		return nil, fmt.Errorf("resolve node %s failed: %w", nodeID, rerr)
	}
	if try.started.IsZero() {
		try.started = time.Now()
	}
	setupStart := time.Now()
	raw, err := session.lim.dial(addr)
	if err != nil {
		try.failOpen(nodeID, l1metrics.ReasonDial, setupStart)
		writeHTTPError(client, http.StatusBadGateway, "Bad Gateway", "")
		return nil, err
	}
	session.setUpstream(raw)
	ok := false
	defer func() {
		if !ok {
			raw.Close()
			session.clearUpstream(raw)
		}
	}()

	token := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
	creq := "CONNECT " + target + " HTTP/1.1\r\n" +
		"Host: " + target + "\r\n" +
		"Proxy-Authorization: Basic " + token + "\r\n\r\n"
	nw, err := raw.Write([]byte(creq))
	if err != nil || nw != len(creq) {
		if err == nil {
			err = io.ErrShortWrite
		}
		try.failOpen(nodeID, l1metrics.ReasonL2Write, setupStart)
		writeHTTPError(client, http.StatusBadGateway, "Bad Gateway", "")
		return nil, err
	}
	if err := raw.SetReadDeadline(time.Now().Add(session.lim.l2Response)); err != nil {
		try.failOpen(nodeID, l1metrics.ReasonL2Deadline, setupStart)
		writeHTTPError(client, http.StatusBadGateway, "Bad Gateway", "")
		return nil, err
	}
	br := bufio.NewReader(raw)
	code, pa, err := readConnectStatus(br)
	if clearErr := raw.SetReadDeadline(time.Time{}); clearErr != nil && err == nil {
		try.failOpen(nodeID, l1metrics.ReasonL2Deadline, setupStart)
		writeHTTPError(client, http.StatusBadGateway, "Bad Gateway", "")
		return nil, clearErr
	}
	if err != nil {
		try.failOpen(nodeID, l1metrics.ReasonL2Response, setupStart)
		writeHTTPError(client, http.StatusBadGateway, "Bad Gateway", "")
		return nil, err
	}
	if code == http.StatusProxyAuthRequired {
		challenge := pa
		if challenge == "" || strings.ContainsAny(challenge, "\r\n") {
			challenge = `Basic realm="relay"`
		}
		try.failOpen(nodeID, l1metrics.ReasonL2Auth, setupStart)
		writeHTTPError(client, http.StatusProxyAuthRequired, "Proxy Authentication Required",
			"Proxy-Authenticate: "+challenge)
		return nil, fmt.Errorf("l2 connect status 407")
	}
	if code != http.StatusOK {
		try.failOpen(nodeID, l1metrics.ReasonL2Status, setupStart)
		writeHTTPError(client, http.StatusBadGateway, "Bad Gateway", "")
		return nil, fmt.Errorf("l2 connect status %d", code)
	}

	var prefix []byte
	if n := br.Buffered(); n > 0 {
		prefix = make([]byte, n)
		if _, err := io.ReadFull(br, prefix); err != nil {
			try.failOpen(nodeID, l1metrics.ReasonL2Response, setupStart)
			writeHTTPError(client, http.StatusBadGateway, "Bad Gateway", "")
			return nil, err
		}
	}
	try.noteSetup(setupStart)
	l1metrics.ObserveHTTPSetup(nodeID, l1metrics.ResultOK, setupStart)
	mc := &modeConn{Conn: raw, session: session}
	ubr := bufio.NewReaderSize(io.MultiReader(bytes.NewReader(prefix), mc), maxHTTPHead)
	ok = true
	opened := time.Now()
	return &upstream{
		nodeID: nodeID,
		target: target,
		conn:   raw,
		br:     ubr,
		mode:   mc,
		leave:  l1metrics.EnterHTTPTunnel(nodeID),
		opened: opened,
	}, nil
}

// retryPutDelete 是进程默认值，默认关闭。一条连接用的是 proxyLimits.retryPutDelete，
// 来自 LimitsFromConfig（JSON 的 retry_put_delete，或命令行 -retry-put-delete）。
// RFC 9110 把 PUT 和 DELETE 标成幂等，但第一次请求可能已经到达源站，很多源站并不是真的幂等。
// 打开之后也只在零个源站字节、且请求头没有写出一部分时重试。
var retryPutDelete = false

func retryableMethod(method string, putDelete bool) bool {
	switch method {
	case "GET", "HEAD", "OPTIONS", "TRACE":
		return true
	case "PUT", "DELETE":
		return putDelete
	default:
		return false
	}
}

func brokenPipeOrEOF(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.ErrClosedPipe) {
		return true
	}
	return errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNABORTED)
}

// classifyStale 只在复用隧道、这条请求还没有结果、并且调用方确认零个源站字节时使用。
// 自动重试只接受对端把连接拆掉：EOF、UnexpectedEOF，以及读或写时的 ECONNRESET、EPIPE、ECONNABORTED、ErrClosedPipe。
// ECONNRESET 在读响应和写请求时都算断管。读超时不是死隧道，返回 false, false，由调用方记原来的原因。
// partialWrite 表示请求头已经写出一部分，那些字节可能已经到了源站，不重试。
// 整段头写完之后的读失败不是 partialWrite，Node 空闲关掉连接时仍要重试。
// 正文已经从客户端读出同样不重试。本版不缓存正文。
// 方法默认只有 GET、HEAD、OPTIONS、TRACE。PUT 和 DELETE 要这条连接的 retryPutDelete 为真。
// 错误确实是断管、但不能重试时记 stale_tunnel。其它错误返回 false, false。
func (t *httpTry) classifyStale(reused, bodyTouched, partialWrite bool, method string, err error) (retry, stale bool) {
	if t == nil || err == nil || !reused || t.observed || t.retried {
		return false, false
	}
	if isTimeout(err) || !brokenPipeOrEOF(err) {
		return false, false
	}
	if partialWrite || bodyTouched || !retryableMethod(method, t.retryPutDelete) {
		return false, true
	}
	return true, false
}

// relay 把改写后的请求写入隧道，再把源站响应原样写回客户端。
// Expect: 100-continue 时先只写头，等源站的 100 之后才写正文；源站直接给最终响应则不写正文。
// 已经向客户端写出过源站字节之后，后续失败只关连接，不再补 502。
// ok 记在最终状态行完整写给客户端的时刻。最终状态行是 code >= 200，或 101。
// 100 和其他 1xx 不是 ok。101 小于 200，但它是这条请求的最终状态。
// 源站自己的 4xx/5xx 只要完整写出，对这条代理来说仍是 ok。
// reused 为真表示这条请求写在已经用过的隧道上。源站一个字节都还没到、且请求还能重放时，
// 返回 stale，由调用方重开；不能重放则记 stale_tunnel。一条请求只在最终那次尝试上出结果。
func (u *upstream) relay(client net.Conn, clientMode *modeConn, clientBR *bufio.Reader, req *absReq, try *httpTry, reused bool) (relayOutcome, error) {
	if try.started.IsZero() {
		try.started = time.Now()
	}
	bodyTouched := false
	n, err := u.conn.Write(req.wireHead)
	try.addUp(n)
	if err != nil || n != len(req.wireHead) {
		if err == nil {
			err = io.ErrShortWrite
		}
		if retry, stale := try.classifyStale(reused, false, n > 0, req.method, err); retry {
			return relayOutcome{stale: true}, nil
		} else if stale {
			try.failUpstream(u.nodeID, l1metrics.ReasonStaleTunnel)
			writeHTTPError(client, http.StatusBadGateway, "Bad Gateway", "")
			return relayOutcome{}, err
		}
		try.failUpstream(u.nodeID, l1metrics.ReasonL2Write)
		writeHTTPError(client, http.StatusBadGateway, "Bad Gateway", "")
		return relayOutcome{}, err
	}
	upW := countWriter{w: u.conn, try: try, up: true}
	if !req.expectContinue {
		readFailed, werr := writeBody(upW, clientBR, req)
		if werr != nil {
			return u.finishBodyWrite(client, req, try, reused, false, readFailed, werr)
		}
		bodyTouched = req.chunked || req.contentLength > 0
	}
	sent := false
	wroteBody := !req.expectContinue
	downW := countWriter{w: client, try: try, up: false}
	for {
		head, err := readHTTPHead(u.br)
		if err != nil {
			if !try.observed && len(head) == 0 && !sent {
				if retry, stale := try.classifyStale(reused, bodyTouched, false, req.method, err); retry {
					return relayOutcome{stale: true}, nil
				} else if stale {
					try.failUpstream(u.nodeID, l1metrics.ReasonStaleTunnel)
					writeHTTPError(client, http.StatusBadGateway, "Bad Gateway", "")
					return relayOutcome{}, err
				}
			}
			if !try.observed {
				try.failUpstream(u.nodeID, l1metrics.ReasonL2Response)
				if !sent {
					writeHTTPError(client, http.StatusBadGateway, "Bad Gateway", "")
				}
			}
			return relayOutcome{endClient: sent}, err
		}
		meta, err := inspectResponse(head, req)
		if err != nil {
			if !try.observed {
				try.failUpstream(u.nodeID, l1metrics.ReasonL2Response)
				if !sent {
					writeHTTPError(client, http.StatusBadGateway, "Bad Gateway", "")
				}
			}
			return relayOutcome{endClient: sent}, err
		}
		nw, werr := downW.Write(head)
		if werr != nil || nw != len(head) {
			if !try.observed {
				try.clientGone()
			}
			if werr == nil {
				werr = io.ErrShortWrite
			}
			return relayOutcome{endClient: true}, werr
		}
		sent = true
		if meta.code >= 200 || meta.code == http.StatusSwitchingProtocols {
			try.observe(l1metrics.ResultOK)
		}
		if meta.code == http.StatusSwitchingProtocols {
			if req.upgrade {
				reason, err := pipeUpgrade(client, clientBR, u.conn, u.br, clientMode, u.mode, try)
				if err != nil && reason != "" {
					l1metrics.AddHTTPBodyAbort(u.nodeID, reason)
				}
				return relayOutcome{endClient: true}, err
			}
			return relayOutcome{endClient: true}, nil
		}
		if meta.code < 200 {
			if meta.code == http.StatusContinue && req.expectContinue && !wroteBody {
				readFailed, werr := writeBody(upW, clientBR, req)
				if werr != nil {
					return u.finishBodyWrite(client, req, try, reused, true, readFailed, werr)
				}
				wroteBody = true
				bodyTouched = req.chunked || req.contentLength > 0
			}
			continue
		}
		readFailed, err := relayResponseBody(downW, u.br, meta)
		if err != nil {
			if reason := bodyAbortReason(readFailed, err); reason != "" {
				l1metrics.AddHTTPBodyAbort(u.nodeID, reason)
			}
			return relayOutcome{endClient: true}, err
		}
		// 最终响应已经发出、但 Expect 的正文还没消费：不能在这条客户端连接上接着读下一个请求。
		abandonedBody := req.expectContinue && !wroteBody
		reuse := meta.keep && !req.closeClient && !meta.untilEOF && !abandonedBody
		end := req.closeClient || meta.untilEOF || abandonedBody
		return relayOutcome{reuse: reuse, endClient: end}, nil
	}
}

// finishBodyWrite 处理请求正文没写完。读侧失败是 client_gone。
// 写侧失败发生在请求头已经完整写出之后，头可能已经到达源站，所以 partialWrite 为真，不重试。
// 已经向客户端写过源站字节时记 l2_write，不再补 502。读超时也记 l2_write，不记 stale_tunnel。
// 还没写过源站字节、且错误是断管时，复用隧道记 stale_tunnel。
func (u *upstream) finishBodyWrite(client net.Conn, req *absReq, try *httpTry, reused, afterOrigin, readFailed bool, err error) (relayOutcome, error) {
	if readFailed {
		try.clientGone()
		if !afterOrigin {
			writeHTTPError(client, http.StatusBadGateway, "Bad Gateway", "")
		}
		return relayOutcome{endClient: afterOrigin}, err
	}
	touched := req.chunked || req.contentLength > 0
	if !afterOrigin {
		if retry, stale := try.classifyStale(reused, touched, true, req.method, err); retry {
			return relayOutcome{stale: true}, nil
		} else if stale {
			try.failUpstream(u.nodeID, l1metrics.ReasonStaleTunnel)
			writeHTTPError(client, http.StatusBadGateway, "Bad Gateway", "")
			return relayOutcome{}, err
		}
	}
	try.failUpstream(u.nodeID, l1metrics.ReasonL2Write)
	if !afterOrigin {
		writeHTTPError(client, http.StatusBadGateway, "Bad Gateway", "")
	}
	return relayOutcome{endClient: afterOrigin}, err
}

// writeBody 把请求正文写入隧道，不记指标。readFailed 为真表示客户端这一侧没有把正文给完。
func writeBody(dst io.Writer, src *bufio.Reader, req *absReq) (bool, error) {
	return writeRequestBody(dst, src, req)
}

type respMeta struct {
	code          int
	keep          bool
	chunked       bool
	contentLength int64
	noBody        bool
	untilEOF      bool
}

func inspectResponse(head []byte, req *absReq) (respMeta, error) {
	start, headers, err := splitHead(head)
	if err != nil {
		return respMeta{}, err
	}
	fields := strings.Split(start, " ")
	if len(fields) < 2 {
		return respMeta{}, fmt.Errorf("bad status line")
	}
	code, err := strconv.Atoi(fields[1])
	if err != nil {
		return respMeta{}, err
	}
	version := fields[0]
	var connToks []string
	chunked := false
	cl := int64(-1)
	sawCL := false
	for _, ln := range headers {
		name, _, ok := headerName(ln)
		if !ok {
			return respMeta{}, fmt.Errorf("bad response header")
		}
		val := headerValue(ln)
		switch strings.ToLower(string(name)) {
		case "connection":
			connToks = append(connToks, splitTokens(val)...)
		case "transfer-encoding":
			if hasToken(splitTokens(val), "chunked") {
				chunked = true
			}
		case "content-length":
			n, perr := strconv.ParseInt(strings.TrimSpace(val), 10, 64)
			if perr != nil || n < 0 {
				return respMeta{}, fmt.Errorf("bad content-length")
			}
			if sawCL && n != cl {
				return respMeta{}, fmt.Errorf("conflicting content-length")
			}
			sawCL = true
			cl = n
		}
	}
	meta := respMeta{code: code, contentLength: cl, chunked: chunked}
	if chunked {
		meta.contentLength = -1
	}
	meta.noBody = code < 200 || code == http.StatusNoContent || code == http.StatusNotModified || strings.EqualFold(req.method, "HEAD") || code == http.StatusSwitchingProtocols
	if !meta.noBody && !chunked && !sawCL {
		meta.untilEOF = true
	}
	if version == "HTTP/1.0" {
		meta.keep = hasToken(connToks, "keep-alive") && !meta.untilEOF
	} else {
		meta.keep = !hasToken(connToks, "close") && !meta.untilEOF
	}
	if code == http.StatusSwitchingProtocols {
		meta.keep = false
	}
	return meta, nil
}

func writeRequestBody(dst io.Writer, src *bufio.Reader, req *absReq) (bool, error) {
	if req.chunked {
		return copyChunked(dst, src)
	}
	if req.contentLength > 0 {
		return copyNRead(dst, src, req.contentLength)
	}
	return false, nil
}

type countWriter struct {
	w   io.Writer
	try *httpTry
	up  bool
}

func (c countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	if c.up {
		c.try.addUp(n)
	} else {
		c.try.addDown(n)
	}
	return n, err
}

type errWriter struct {
	w      io.Writer
	failed bool
}

func (e *errWriter) Write(p []byte) (int, error) {
	n, err := e.w.Write(p)
	if err != nil || (len(p) > 0 && n < len(p)) {
		e.failed = true
	}
	return n, err
}

type errReader struct {
	r      io.Reader
	failed bool
}

func (e *errReader) Read(p []byte) (int, error) {
	n, err := e.r.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		e.failed = true
	}
	return n, err
}

// copyNRead 报告失败发生在读侧还是写侧。提前 EOF 算读侧：请求正文没给完是客户端，响应正文没给完是源站。
func copyNRead(dst io.Writer, src io.Reader, n int64) (bool, error) {
	mr := &errReader{r: src}
	_, err := io.CopyN(dst, mr, n)
	if err == nil {
		return false, nil
	}
	if mr.failed || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true, err
	}
	return false, err
}

// relayResponseBody 把最终状态行之后的响应正文写给客户端。
// 返回的 bool 表示失败在读源站这一侧。Content-Length 没给够是读侧，记 origin_reset。
// 直到连接关闭的正文在 EOF 上成功返回，不记 body abort。
func relayResponseBody(dst io.Writer, src *bufio.Reader, meta respMeta) (bool, error) {
	if meta.noBody {
		return false, nil
	}
	if meta.chunked {
		return copyChunked(dst, src)
	}
	if meta.contentLength >= 0 {
		return copyNRead(dst, src, meta.contentLength)
	}
	return copyUntilClose(dst, src)
}

func copyUntilClose(dst io.Writer, src io.Reader) (bool, error) {
	mr := &errReader{r: src}
	_, err := io.Copy(dst, mr)
	if err == nil || errors.Is(err, io.EOF) {
		return false, nil
	}
	if mr.failed || isTimeout(err) {
		return true, err
	}
	return false, err
}

func bodyAbortReason(readFailed bool, err error) string {
	if errors.Is(err, errHalfCloseIdle) {
		return ""
	}
	if errors.Is(err, errIdleExpired) || isTimeout(err) {
		return l1metrics.ReasonAbortIdleTimeout
	}
	if readFailed {
		return l1metrics.ReasonAbortOriginReset
	}
	return l1metrics.ReasonAbortClientCancel
}

// copyChunked 按 chunk 帧拷贝，不解码、不重编码。size 行、扩展、正文和 trailer 都原样写出。
func copyChunked(dst io.Writer, src *bufio.Reader) (bool, error) {
	for {
		line, err := src.ReadBytes('\n')
		if err != nil {
			return true, err
		}
		if _, err := dst.Write(line); err != nil {
			return false, err
		}
		size, err := parseChunkSize(line)
		if err != nil {
			return true, err
		}
		if size == 0 {
			for {
				trailer, err := src.ReadBytes('\n')
				if err != nil {
					return true, err
				}
				if _, err := dst.Write(trailer); err != nil {
					return false, err
				}
				if isBlankLine(trailer) {
					return false, nil
				}
			}
		}
		readFailed, err := copyNRead(dst, src, size)
		if err != nil {
			return readFailed, err
		}
		crlf := make([]byte, 2)
		if _, err := io.ReadFull(src, crlf); err != nil {
			return true, err
		}
		if _, err := dst.Write(crlf); err != nil {
			return false, err
		}
	}
}

func parseChunkSize(line []byte) (int64, error) {
	line = bytes.TrimRight(line, "\r\n")
	if i := bytes.IndexAny(line, "; \t"); i >= 0 {
		line = line[:i]
	}
	if len(line) == 0 {
		return 0, fmt.Errorf("empty chunk size")
	}
	return strconv.ParseInt(string(line), 16, 64)
}

// pipeUpgrade 在 101 之后进入与 CONNECT 相同的双向字节转发。
// 半关闭后的空闲上限是这条连接的 halfCloseIdle，整条隧道的空闲兜底仍是 tunnelIdle。
// io.Copy 在 EOF 上返回 nil。随后的 closeWrite 失败不当成隧道中断，也不去关另一端，
// 否则另一方向会读到 use of closed network connection，把正常的 101 结束记成 abort。
// 某一方向先失败时，用 sync.Once 记下第一个真实错误，再关两端。
// 另一方向因这次 Close 得到的 net.ErrClosed 不再覆盖原因。
// 半关闭后的空闲超时返回空 reason，调用方不计数。正常拆除的 error 为 nil。
func pipeUpgrade(client net.Conn, clientR io.Reader, up net.Conn, upR io.Reader, clientMode, upMode *modeConn, try *httpTry) (string, error) {
	type copyResult struct {
		reason string
		err    error
	}
	var (
		once    sync.Once
		kept    copyResult
		closing atomic.Bool
	)
	done := make(chan struct{}, 2)
	halfIdle := halfCloseIdleTimeout
	if clientMode != nil && clientMode.session != nil && clientMode.session.lim.halfCloseIdle > 0 {
		halfIdle = clientMode.session.lim.halfCloseIdle
	}
	record := func(reason string, err error) {
		if err == nil {
			return
		}
		if closing.Load() && errors.Is(err, net.ErrClosed) {
			return
		}
		// 半关闭空闲的 reason 是空的。仍然先记下，避免另一方向的关闭错误后来居上。
		// 调用方看到空 reason 不进 body abort。
		once.Do(func() { kept = copyResult{reason: reason, err: err} })
		closing.Store(true)
		client.Close()
		up.Close()
	}
	relay := func(dst net.Conn, src io.Reader, reverse *modeConn, upload bool) {
		defer func() { done <- struct{}{} }()
		ew := &errWriter{w: countWriter{w: dst, try: try, up: upload}}
		er := &errReader{r: src}
		_, copyErr := io.Copy(ew, er)
		if copyErr != nil {
			record(upgradeAbortReason(upload, ew.failed, copyErr), copyErr)
			return
		}
		_ = closeWrite(dst)
		if reverse != nil {
			reverse.noteHalf(halfIdle)
		}
	}
	go func() { relay(up, clientR, upMode, true) }()
	go func() { relay(client, upR, clientMode, false) }()
	<-done
	<-done
	return kept.reason, kept.err
}

func upgradeAbortReason(upload, writeFailed bool, err error) string {
	if errors.Is(err, errHalfCloseIdle) {
		return ""
	}
	if errors.Is(err, errIdleExpired) || isTimeout(err) {
		return l1metrics.ReasonAbortIdleTimeout
	}
	if upload && writeFailed {
		return l1metrics.ReasonAbortRequestBody
	}
	if writeFailed || !upload {
		// 写给客户端失败是 client_cancel。从客户端读失败也是 client_cancel。
		// 从源站读失败（下行、且不是写失败）是 origin_reset。
		if !upload && !writeFailed {
			return l1metrics.ReasonAbortOriginReset
		}
		return l1metrics.ReasonAbortClientCancel
	}
	return l1metrics.ReasonAbortClientCancel
}

func writeHTTPError(w io.Writer, status int, reason, header string) {
	var b strings.Builder
	fmt.Fprintf(&b, "HTTP/1.1 %d %s\r\n", status, reason)
	if header != "" {
		b.WriteString(header)
		if !strings.HasSuffix(header, "\r\n") {
			b.WriteString("\r\n")
		}
	}
	b.WriteString("Content-Length: 0\r\nConnection: close\r\n\r\n")
	_, _ = io.WriteString(w, b.String())
}

// readRequestHead 用这条连接的 headerDeadline 做绝对截止。
// 截止时间在开始读头时设一次，读到字节不顺延，slowloris 不能靠慢慢送字节把连接留住。
// 读完或失败后清掉，后面的正文和空闲仍用 tunnelIdle。
func readRequestHead(mc *modeConn, br *bufio.Reader) ([]byte, error) {
	d := headerDeadline
	if mc != nil && mc.session != nil && mc.session.lim.headerDeadline > 0 {
		d = mc.session.lim.headerDeadline
	}
	if mc != nil {
		mc.beginHeader(time.Now().Add(d))
		defer mc.endHeader()
	}
	return readHTTPHead(br)
}

func readHTTPHead(r *bufio.Reader) ([]byte, error) {
	var buf bytes.Buffer
	for {
		line, err := r.ReadSlice('\n')
		if len(line) > 0 {
			if buf.Len()+len(line) > maxHTTPHead {
				return buf.Bytes(), fmt.Errorf("head too large")
			}
			buf.Write(line)
		}
		if err == bufio.ErrBufferFull {
			return buf.Bytes(), fmt.Errorf("head too large")
		}
		if err != nil {
			return buf.Bytes(), err
		}
		if isBlankLine(line) {
			if buf.Len() == len(line) {
				return buf.Bytes(), fmt.Errorf("empty message")
			}
			return buf.Bytes(), nil
		}
	}
}

func splitHead(head []byte) (string, [][]byte, error) {
	parts := bytes.SplitAfter(head, []byte("\n"))
	var lines [][]byte
	for _, ln := range parts {
		if len(ln) == 0 {
			continue
		}
		lines = append(lines, ln)
	}
	if len(lines) == 0 {
		return "", nil, fmt.Errorf("empty message")
	}
	start := string(bytes.TrimRight(lines[0], "\r\n"))
	var headers [][]byte
	for _, ln := range lines[1:] {
		if isBlankLine(ln) {
			continue
		}
		headers = append(headers, ln)
	}
	return start, headers, nil
}

func headerName(line []byte) (name, rest []byte, ok bool) {
	if len(line) == 0 || line[0] == ' ' || line[0] == '\t' {
		return nil, nil, false
	}
	i := bytes.IndexByte(line, ':')
	if i <= 0 {
		return nil, nil, false
	}
	return line[:i], line[i+1:], true
}

func headerValue(line []byte) string {
	_, rest, ok := headerName(line)
	if !ok {
		return ""
	}
	return strings.TrimSpace(string(bytes.TrimRight(rest, "\r\n")))
}

func isBlankLine(line []byte) bool {
	return bytes.Equal(line, []byte("\n")) || bytes.Equal(line, []byte("\r\n"))
}

func requestMethod(head []byte) string {
	line, _, _ := bytes.Cut(head, []byte("\n"))
	line = bytes.TrimRight(line, "\r")
	method, _, _ := bytes.Cut(line, []byte(" "))
	return string(method)
}

func basicFrom(headers [][]byte) (user, pass string) {
	for _, ln := range headers {
		name, _, ok := headerName(ln)
		if !ok || !strings.EqualFold(string(name), "Proxy-Authorization") {
			continue
		}
		u, p, good := parseBasic(headerValue(ln))
		if good {
			return u, p
		}
	}
	return "", ""
}

func parseBasic(v string) (user, pass string, ok bool) {
	if len(v) < 6 || !strings.EqualFold(v[:6], "basic ") {
		return "", "", false
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(v[6:]))
	if err != nil {
		return "", "", false
	}
	s := string(raw)
	i := strings.IndexByte(s, ':')
	if i < 0 {
		return "", "", false
	}
	return s[:i], s[i+1:], true
}

func splitTokens(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func hasToken(toks []string, want string) bool {
	for _, t := range toks {
		if strings.EqualFold(t, want) {
			return true
		}
	}
	return false
}

func takeBuffered(br *bufio.Reader) []byte {
	n := br.Buffered()
	if n == 0 {
		return nil
	}
	b := make([]byte, n)
	_, _ = io.ReadFull(br, b)
	return b
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// modeConn 在每次 Read 前刷新空闲截止时间。半关闭后改用这条连接的 halfCloseIdle。
// disabled 时不再改截止时间，留给 handleConnect 自己的 idleConn。
// headerUntil 非零时是读请求头的绝对截止，不因读到字节而刷新，也不去拨会话空闲计时。
// 会话空闲计时到点会先置位再 Close。这时的读错误记成 errIdleExpired，而不是 net.ErrClosed。
type modeConn struct {
	net.Conn
	session     *httpSession
	mu          sync.Mutex
	half        atomic.Bool
	disabled    atomic.Bool
	headerUntil atomic.Int64
}

func (m *modeConn) beginHeader(deadline time.Time) {
	m.headerUntil.Store(deadline.UnixNano())
	_ = m.SetReadDeadline(deadline)
}

func (m *modeConn) endHeader() {
	m.headerUntil.Store(0)
	if !m.disabled.Load() {
		_ = m.SetReadDeadline(time.Time{})
	}
}

// noteHalf 和 Read 共用 mu，避免 Read 在看到半关闭之前把截止时间又拨回整条隧道的空闲。
func (m *modeConn) noteHalf(d time.Duration) {
	m.mu.Lock()
	m.half.Store(true)
	if d <= 0 {
		d = halfCloseIdleTimeout
	}
	_ = m.Conn.SetReadDeadline(time.Now().Add(d))
	m.mu.Unlock()
}

func (m *modeConn) Read(p []byte) (int, error) {
	lim := defaultLimits()
	if m.session != nil {
		lim = m.session.lim
	}
	m.mu.Lock()
	inHeader := m.headerUntil.Load() != 0
	if !m.disabled.Load() && !inHeader {
		d := lim.tunnelIdle
		if m.half.Load() {
			d = lim.halfCloseIdle
		}
		_ = m.Conn.SetReadDeadline(time.Now().Add(d))
	}
	m.mu.Unlock()
	n, err := m.Conn.Read(p)
	if n > 0 && m.session != nil && m.headerUntil.Load() == 0 {
		m.session.touch()
	}
	if err == nil || m.session == nil {
		return n, err
	}
	if m.session.idleExpired.Load() {
		return n, errIdleExpired
	}
	if m.half.Load() && isTimeout(err) {
		return n, errHalfCloseIdle
	}
	return n, err
}

func (m *modeConn) CloseWrite() error {
	if cw, ok := m.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return m.Conn.Close()
}

// prefixConn 先读完已经从 bufio 取出的字节，再读底层连接。
type prefixConn struct {
	net.Conn
	r io.Reader
}

func (p *prefixConn) Read(b []byte) (int, error) { return p.r.Read(b) }

func (p *prefixConn) CloseWrite() error {
	if cw, ok := p.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return p.Conn.Close()
}

// httpSession 是明文 HTTP 连接上的空闲兜底，时长用 lim.tunnelIdle。
// 任一方向读到字节就重新计时。到点先记下 idleExpired，再关掉客户端和当前上游。
type httpSession struct {
	client      net.Conn
	lim         proxyLimits
	mu          sync.Mutex
	upstream    net.Conn
	timer       *time.Timer
	stopped     bool
	idleExpired atomic.Bool
}

func newHTTPSession(client net.Conn, lim proxyLimits) *httpSession {
	lim = fillLimits(lim)
	s := &httpSession{client: client, lim: lim}
	s.timer = time.AfterFunc(s.lim.tunnelIdle, s.fire)
	return s
}

func (s *httpSession) touch() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped || s.timer == nil {
		return
	}
	s.timer.Reset(s.lim.tunnelIdle)
}

func (s *httpSession) setUpstream(c net.Conn) {
	s.mu.Lock()
	s.upstream = c
	stopped := s.stopped
	s.mu.Unlock()
	if stopped && c != nil {
		c.Close()
	}
}

func (s *httpSession) clearUpstream(c net.Conn) {
	s.mu.Lock()
	if s.upstream == c {
		s.upstream = nil
	}
	s.mu.Unlock()
}

func (s *httpSession) fire() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	s.idleExpired.Store(true)
	up := s.upstream
	s.mu.Unlock()
	s.client.Close()
	if up != nil {
		up.Close()
	}
}

// notePartialHead 给已经读到一部分的请求头记拒绝。
// 超时是 header_timeout，其它不完整是 bad_target。零字节不调用。
// 请求行已经是 CONNECT 时走 CONNECT 的计数，避免把 CONNECT 算进明文 HTTP。
func notePartialHead(head []byte, err error) {
	if len(head) == 0 || err == nil {
		return
	}
	reason := l1metrics.ReasonRejectBadTarget
	// 读截止时间，或空闲计时先关掉连接，都是 header_timeout。零字节不走到这里。
	if isTimeout(err) || errors.Is(err, errIdleExpired) {
		reason = l1metrics.ReasonRejectHeaderTimeout
	}
	if requestMethod(head) == "CONNECT" {
		l1metrics.RejectClient(reason)
		return
	}
	l1metrics.RejectHTTP(reason)
}

func (s *httpSession) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopped = true
	if s.timer != nil {
		s.timer.Stop()
	}
}
