//go:build docfixture

package l1metrics

import (
	"sync"
	"time"
)

const (
	NodeUnknown = "unknown"

	ReasonNoUpstream  = "no_upstream"
	ReasonDial        = "dial"
	ReasonL2Write     = "l2_write"
	ReasonL2Deadline  = "l2_deadline"
	ReasonL2Response  = "l2_response"
	ReasonL2Status    = "l2_status"
	ReasonL2Auth      = "l2_auth"
	ReasonStaleTunnel = "stale_tunnel"

	ReasonClientGone = "client_gone"
	ResultOK         = "ok"

	ReasonRejectAuth          = "auth"
	ReasonRejectNoNode        = "no_node"
	ReasonRejectBadTarget     = "bad_target"
	ReasonRejectBadCredential = "bad_credential"

	DirUp   = "up"
	DirDown = "down"

	ProtocolConnect = "connect"
	ProtocolHTTP    = "http"
)

// MeterEvent matches the §7.1 event. The fixture does not export it to Prometheus.
// TryEmit forces V=2 and drops a protocol other than connect or http.
type MeterEvent struct {
	V           int
	Protocol    string
	Result      string
	User        string
	NodeID      string
	TargetHost  string
	TargetPort  string
	ClientIP    string
	HandshakeMs float64
	TunnelSec   float64
	BytesUp     int64
	BytesDown   int64
}

// Calls records the reference handler's metric hooks so tests can assert them.
// The design doc's l1metrics does not expose this; it exists only in the fixture.
// CONNECT and plain HTTP are separate so a plain-HTTP request can be checked
// for not touching the CONNECT series.
type Calls struct {
	Rejects  []string
	Fails    [][2]string
	Observes []string
	Entered  int
	Left     int

	HTTPRejects    []string
	HTTPFails      [][2]string
	HTTPObserves   []string
	HTTPSetup      []string
	HTTPAborts     [][2]string
	HTTPEntered    int
	HTTPLeft       int
	HTTPTunnels    int
	HTTPUp         int64
	HTTPDown       int64
	HTTPRetries    int
	HTTPBodyAborts int
	Drops          int
	Events         []MeterEvent
}

var (
	mu    sync.Mutex
	calls Calls
)

func Snapshot() Calls {
	mu.Lock()
	defer mu.Unlock()
	out := calls
	out.Rejects = append([]string(nil), calls.Rejects...)
	out.Fails = append([][2]string(nil), calls.Fails...)
	out.Observes = append([]string(nil), calls.Observes...)
	out.HTTPRejects = append([]string(nil), calls.HTTPRejects...)
	out.HTTPFails = append([][2]string(nil), calls.HTTPFails...)
	out.HTTPObserves = append([]string(nil), calls.HTTPObserves...)
	out.HTTPSetup = append([]string(nil), calls.HTTPSetup...)
	out.HTTPAborts = append([][2]string(nil), calls.HTTPAborts...)
	out.Events = append([]MeterEvent(nil), calls.Events...)
	return out
}

func Reset() {
	mu.Lock()
	calls = Calls{}
	mu.Unlock()
}

func NodeLabel(inTable bool, nodeID string) string {
	if !inTable || nodeID == "" {
		return NodeUnknown
	}
	return nodeID
}

func knownResult(result string) bool {
	switch result {
	case ResultOK, ReasonDial, ReasonL2Write, ReasonL2Deadline, ReasonL2Response, ReasonL2Status, ReasonL2Auth, ReasonClientGone:
		return true
	default:
		return false
	}
}

func knownFail(reason string) bool {
	switch reason {
	case ReasonNoUpstream, ReasonDial, ReasonL2Write, ReasonL2Deadline, ReasonL2Response, ReasonL2Status, ReasonL2Auth:
		return true
	default:
		return false
	}
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

func knownReject(reason string) bool {
	switch reason {
	case ReasonRejectAuth, ReasonRejectNoNode, ReasonRejectBadTarget, ReasonRejectBadCredential:
		return true
	default:
		return false
	}
}

func Fail502(nodeID, reason string) {
	mu.Lock()
	calls.Fails = append(calls.Fails, [2]string{nodeID, reason})
	mu.Unlock()
}

func RejectClient(reason string) {
	mu.Lock()
	calls.Rejects = append(calls.Rejects, reason)
	mu.Unlock()
}

func ClientAbort(nodeID, reason string) {}

func ObserveConnect(nodeID, result string, started time.Time) {
	if started.IsZero() || !knownResult(result) {
		return
	}
	mu.Lock()
	calls.Observes = append(calls.Observes, result)
	mu.Unlock()
}

func ObserveTunnel(nodeID string, started time.Time) {}

func AddTunnelBytes(nodeID, direction string, n int64) {}

func EnterTunnel(nodeID string) func() {
	mu.Lock()
	calls.Entered++
	mu.Unlock()
	return func() {
		mu.Lock()
		calls.Left++
		mu.Unlock()
	}
}

func FailHTTP(nodeID, reason string) {
	if !knownHTTPFail(reason) {
		return
	}
	mu.Lock()
	calls.HTTPFails = append(calls.HTTPFails, [2]string{nodeID, reason})
	mu.Unlock()
}

func RejectHTTP(reason string) {
	if !knownReject(reason) {
		return
	}
	mu.Lock()
	calls.HTTPRejects = append(calls.HTTPRejects, reason)
	mu.Unlock()
}

func AbortHTTP(nodeID, reason string) {
	if reason != ReasonClientGone {
		return
	}
	mu.Lock()
	calls.HTTPAborts = append(calls.HTTPAborts, [2]string{nodeID, reason})
	mu.Unlock()
}

func ObserveHTTP(nodeID, result string, started time.Time) {
	if started.IsZero() || !knownHTTPResult(result) {
		return
	}
	mu.Lock()
	calls.HTTPObserves = append(calls.HTTPObserves, result)
	mu.Unlock()
}

func ObserveHTTPSetup(nodeID, result string, started time.Time) {
	if started.IsZero() || !knownResult(result) {
		return
	}
	mu.Lock()
	calls.HTTPSetup = append(calls.HTTPSetup, result)
	mu.Unlock()
}

func ObserveHTTPTunnel(nodeID string, started time.Time) {
	if started.IsZero() {
		return
	}
	mu.Lock()
	calls.HTTPTunnels++
	mu.Unlock()
}

func AddHTTPBytes(nodeID, direction string, n int64) {
	if n <= 0 || (direction != DirUp && direction != DirDown) {
		return
	}
	mu.Lock()
	if direction == DirUp {
		calls.HTTPUp += n
	} else {
		calls.HTTPDown += n
	}
	mu.Unlock()
}

func AddHTTPReusedRetry(nodeID string) {
	mu.Lock()
	calls.HTTPRetries++
	mu.Unlock()
}

func AddHTTPBodyAbort(nodeID string) {
	mu.Lock()
	calls.HTTPBodyAborts++
	mu.Unlock()
}

func EnterHTTPTunnel(nodeID string) func() {
	mu.Lock()
	calls.HTTPEntered++
	mu.Unlock()
	return func() {
		mu.Lock()
		calls.HTTPLeft++
		mu.Unlock()
	}
}

func TryEmit(ev MeterEvent) {
	if ev.Protocol != ProtocolConnect && ev.Protocol != ProtocolHTTP {
		mu.Lock()
		calls.Drops++
		mu.Unlock()
		return
	}
	ev.V = 2
	mu.Lock()
	calls.Events = append(calls.Events, ev)
	mu.Unlock()
}
