//go:build docfixture

package proxy

import (
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/f-ishelly/relay-proxy/internal/docfixture/l1metrics"
)

// metricSnap is the delta since resetMetrics. Counters cannot be reset on the
// real registry, and go test -count repeats in one process, so every assertion
// reads the change since the baseline scrape.
type metricSnap struct {
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
	BodyByReason   map[string]int
	Drops          int
	Events         []l1metrics.MeterEvent
}

type labKey struct {
	name string
	labs string
}

var (
	evMu       sync.Mutex
	evLog      []l1metrics.MeterEvent
	evBase     int
	baseSeries map[labKey]float64
)

func consumeMeters() {
	for ev := range l1metrics.MeterEvents() {
		evMu.Lock()
		evLog = append(evLog, ev)
		evMu.Unlock()
	}
}

func waitMeterIdle() {
	ch := l1metrics.MeterEvents()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(ch) == 0 {
			time.Sleep(2 * time.Millisecond)
			if len(ch) == 0 {
				return
			}
			continue
		}
		time.Sleep(time.Millisecond)
	}
}

func resetMetrics() {
	waitMeterIdle()
	evMu.Lock()
	evBase = len(evLog)
	evMu.Unlock()
	baseSeries = scrapeSeries()
}

func snapMetrics() metricSnap {
	waitMeterIdle()
	cur := scrapeSeries()
	d := deltaSeries(baseSeries, cur)
	var snap metricSnap
	snap.Rejects = expandOne(sumLabel(d, "proxy_client_reject_total", "reason"), rejectOrder)
	snap.Fails = expandTwo(sumTwo(d, "proxy_upstream_fail_total", "node_id", "reason"))
	snap.Observes = expandOne(sumLabel(d, "proxy_connect_latency_ms_count", "result"), connectResultOrder)
	connDur := sumMatch(d, "proxy_tunnel_duration_seconds_count", nil)
	connActive := sumMatch(d, "proxy_conn_active", nil)
	snap.Left = asInt(connDur)
	snap.Entered = asInt(connDur + connActive)

	snap.HTTPRejects = expandOne(sumLabel(d, "proxy_http_client_reject_total", "reason"), rejectOrder)
	snap.HTTPFails = expandTwo(sumTwo(d, "proxy_http_upstream_fail_total", "node_id", "reason"))
	snap.HTTPObserves = expandOne(sumLabel(d, "proxy_http_request_latency_ms_count", "result"), httpResultOrder)
	snap.HTTPSetup = expandOne(sumLabel(d, "proxy_http_tunnel_setup_ms_count", "result"), setupResultOrder)
	snap.HTTPAborts = expandTwo(sumTwo(d, "proxy_http_client_abort_total", "node_id", "reason"))
	httpDur := sumMatch(d, "proxy_http_tunnel_duration_seconds_count", nil)
	httpActive := sumMatch(d, "proxy_http_tunnel_active", nil)
	snap.HTTPLeft = asInt(httpDur)
	snap.HTTPTunnels = asInt(httpDur)
	snap.HTTPEntered = asInt(httpDur + httpActive)
	snap.HTTPUp = int64(asInt(sumMatch(d, "proxy_http_tunnel_bytes_total", map[string]string{"direction": "up"})))
	snap.HTTPDown = int64(asInt(sumMatch(d, "proxy_http_tunnel_bytes_total", map[string]string{"direction": "down"})))
	snap.HTTPRetries = asInt(sumMatch(d, "proxy_http_reused_conn_retry_total", nil))
	snap.BodyByReason = sumLabel(d, "proxy_http_body_abort_total", "reason")
	for _, n := range snap.BodyByReason {
		snap.HTTPBodyAborts += n
	}
	snap.Drops = asInt(sumMatch(d, "proxy_meter_drop_total", nil))

	evMu.Lock()
	snap.Events = append([]l1metrics.MeterEvent(nil), evLog[evBase:]...)
	evMu.Unlock()
	return snap
}

var (
	rejectOrder        = []string{"auth", "no_node", "bad_target", "bad_credential"}
	connectResultOrder = []string{
		"ok", "dial", "l2_write", "l2_deadline", "l2_response", "l2_status", "l2_auth", "client_gone",
	}
	httpResultOrder = []string{
		"ok", "dial", "l2_write", "l2_deadline", "l2_response", "l2_status", "l2_auth", "client_gone", "stale_tunnel",
	}
	setupResultOrder = []string{
		"ok", "dial", "l2_write", "l2_deadline", "l2_response", "l2_status", "l2_auth",
	}
)

func asInt(v float64) int {
	if v < 0.5 {
		return 0
	}
	return int(math.Round(v))
}

func scrapeSeries() map[labKey]float64 {
	out := map[labKey]float64{}
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return out
	}
	for _, mf := range mfs {
		name := mf.GetName()
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			switch mf.GetType() {
			case dto.MetricType_COUNTER:
				out[labKey{name, canonLabs(labels)}] = m.GetCounter().GetValue()
			case dto.MetricType_GAUGE:
				out[labKey{name, canonLabs(labels)}] = m.GetGauge().GetValue()
			case dto.MetricType_HISTOGRAM:
				h := m.GetHistogram()
				out[labKey{name + "_count", canonLabs(labels)}] = float64(h.GetSampleCount())
				out[labKey{name + "_sum", canonLabs(labels)}] = h.GetSampleSum()
			}
		}
	}
	return out
}

func canonLabs(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(labels[k])
	}
	return b.String()
}

func parseLabs(s string) map[string]string {
	out := map[string]string{}
	if s == "" {
		return out
	}
	for _, p := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(p, "=")
		if ok {
			out[k] = v
		}
	}
	return out
}

func deltaSeries(base, cur map[labKey]float64) map[labKey]float64 {
	out := map[labKey]float64{}
	for k, v := range cur {
		out[k] = v - base[k]
	}
	for k, v := range base {
		if _, ok := cur[k]; !ok {
			out[k] = -v
		}
	}
	return out
}

func sumMatch(d map[labKey]float64, name string, want map[string]string) float64 {
	var n float64
	for k, v := range d {
		if k.name != name {
			continue
		}
		if labelsMatch(k.labs, want) {
			n += v
		}
	}
	return n
}

func labelsMatch(canon string, want map[string]string) bool {
	if len(want) == 0 {
		return true
	}
	have := parseLabs(canon)
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

func sumLabel(d map[labKey]float64, name, label string) map[string]int {
	acc := map[string]float64{}
	for k, v := range d {
		if k.name != name {
			continue
		}
		acc[parseLabs(k.labs)[label]] += v
	}
	out := map[string]int{}
	for k, v := range acc {
		if n := asInt(v); n > 0 {
			out[k] = n
		}
	}
	return out
}

func sumTwo(d map[labKey]float64, name, a, b string) map[[2]string]int {
	acc := map[[2]string]float64{}
	for k, v := range d {
		if k.name != name {
			continue
		}
		labs := parseLabs(k.labs)
		acc[[2]string{labs[a], labs[b]}] += v
	}
	out := map[[2]string]int{}
	for k, v := range acc {
		if n := asInt(v); n > 0 {
			out[k] = n
		}
	}
	return out
}

func expandOne(counts map[string]int, order []string) []string {
	var out []string
	seen := map[string]bool{}
	for _, name := range order {
		seen[name] = true
		for i := 0; i < counts[name]; i++ {
			out = append(out, name)
		}
	}
	var rest []string
	for name, n := range counts {
		if seen[name] {
			continue
		}
		for i := 0; i < n; i++ {
			rest = append(rest, name)
		}
	}
	sort.Strings(rest)
	return append(out, rest...)
}

func expandTwo(counts map[[2]string]int) [][2]string {
	keys := make([][2]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i][0] == keys[j][0] {
			return keys[i][1] < keys[j][1]
		}
		return keys[i][0] < keys[j][0]
	})
	var out [][2]string
	for _, k := range keys {
		for i := 0; i < counts[k]; i++ {
			out = append(out, k)
		}
	}
	return out
}
