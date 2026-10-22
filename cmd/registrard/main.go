// Command registrard 是备选适配层。admin 是 Go 程序，主路径是进程内调用 registrar。
// 只有暂时不能改 admin 时，才用这个进程把本机 HTTP 转成 Start / Drain / Ready / Report / Stop。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/f-ishelly/relay-proxy/registrar"
)

type fileConfig struct {
	Endpoints                []string `json:"endpoints"`
	NodeID                   string   `json:"node_id"`
	NodeIDFile               string   `json:"node_id_file"`
	ProxyPort                int      `json:"proxy_port"`
	ControlPort              int      `json:"control_port"`
	IntranetAddr             string   `json:"intranet_addr"`
	ProbeAddr                string   `json:"probe_addr"`
	ProbeUser                string   `json:"probe_user"`
	ProbePass                string   `json:"probe_pass"`
	IPSource                 string   `json:"ip_source"`
	API                      string   `json:"api"`
	StateFile                string   `json:"state_file"`
	TTLSeconds               int64    `json:"ttl_seconds"`
	RenewEveryMS             int      `json:"renew_every_ms"`
	ProbeEveryMS             int      `json:"probe_every_ms"`
	ProbeTimeoutMS           int      `json:"probe_timeout_ms"`
	FailThreshold            int      `json:"fail_threshold"`
	RiseThreshold            int      `json:"rise_threshold"`
	IPGuardEveryMS           int      `json:"ip_guard_every_ms"`
	AZ                       string   `json:"az"`
	Weight                   int      `json:"weight"`
	Version                  string   `json:"version"`
	AssumedDialMS            int      `json:"assumed_dial_ms"`
	AssumedEgressCheckMS     int      `json:"assumed_egress_check_ms"`
	AssumedRedialFailureRate float64  `json:"assumed_redial_failure_rate"`
	InspectionIntervalMS     int      `json:"inspection_interval_ms"`
	IgnoreAdminNetCheck      bool     `json:"ignore_admin_net_check"`
	ResourceFaultAction      string   `json:"resource_fault_action"`
	DrainAttemptMS           int      `json:"drain_attempt_ms"`
	DrainBudgetMS            int      `json:"drain_budget_ms"`
	RedialJitterMS           int      `json:"redial_jitter_ms"`
}

func main() {
	cfgPath := flag.String("config", "", "JSON 配置文件")
	flag.Parse()
	if *cfgPath == "" {
		log.Fatal("需要 -config")
	}
	raw, err := os.ReadFile(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	var fc fileConfig
	if err := json.Unmarshal(raw, &fc); err != nil {
		log.Fatal(err)
	}
	if fc.API == "" {
		fc.API = "127.0.0.1:17101"
	}
	if err := loopback(fc.API); err != nil {
		log.Fatal(err)
	}
	cfg := registrar.DefaultConfig()
	cfg.Endpoints = fc.Endpoints
	cfg.NodeID = fc.NodeID
	cfg.NodeIDFile = fc.NodeIDFile
	cfg.ProxyPort = fc.ProxyPort
	cfg.ControlPort = fc.ControlPort
	cfg.IntranetAddr = fc.IntranetAddr
	cfg.ProbeAddr = fc.ProbeAddr
	cfg.ProbeUser = fc.ProbeUser
	cfg.ProbePassword = fc.ProbePass
	cfg.StateFile = fc.StateFile
	cfg.AZ = fc.AZ
	cfg.Weight = fc.Weight
	cfg.Version = fc.Version
	cfg.IgnoreAdminNetCheck = fc.IgnoreAdminNetCheck
	cfg.ResourceFaultAction = fc.ResourceFaultAction
	if fc.AssumedRedialFailureRate > 0 {
		cfg.AssumedRedialFailureRate = fc.AssumedRedialFailureRate
	}
	cfg.LocalIP = func() string { return readIP(fc.IPSource) }
	cfg.Logger = registrar.JSONLogger{Out: os.Stdout}
	cfg.OnAlert = func(a registrar.Alert) {
		cfg.Logger.Event("alert", "kind", a.Kind, "key", a.Key, "other_lease", a.OtherLease, "msg", a.Message)
	}
	if fc.TTLSeconds > 0 {
		cfg.TTL = time.Duration(fc.TTLSeconds) * time.Second
	}
	setMS(&cfg.RenewInterval, fc.RenewEveryMS)
	setMS(&cfg.ProbeInterval, fc.ProbeEveryMS)
	setMS(&cfg.ProbeTimeout, fc.ProbeTimeoutMS)
	setMS(&cfg.IPGuardInterval, fc.IPGuardEveryMS)
	setMS(&cfg.AssumedDial, fc.AssumedDialMS)
	setMS(&cfg.AssumedEgressCheck, fc.AssumedEgressCheckMS)
	setMS(&cfg.InspectionInterval, fc.InspectionIntervalMS)
	setMS(&cfg.DrainAttempt, fc.DrainAttemptMS)
	setMS(&cfg.DrainBudget, fc.DrainBudgetMS)
	setMS(&cfg.RedialJitter, fc.RedialJitterMS)
	if fc.FailThreshold > 0 {
		cfg.ProbeFailThreshold = fc.FailThreshold
	}
	if fc.RiseThreshold > 0 {
		cfg.ProbeRiseThreshold = fc.RiseThreshold
	}
	if len(cfg.Endpoints) < 3 {
		cfg.Logger.Event("endpoints_warning", "n", len(cfg.Endpoints), "msg", "生产环境必须配置全部 etcd 成员")
	}

	reg, err := registrar.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	// 不在启动时调用 Ready。旧状态文件如果没有 machine_id 或 instance_id，进程会停在 drained，
	// 直到 admin POST /v1/ready。这里没有出口地址，不能替它注册。
	if err := reg.Start(context.Background()); err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/drain", func(w http.ResponseWriter, rq *http.Request) {
		if rq.Method != http.MethodPost {
			http.Error(w, "POST", http.StatusMethodNotAllowed)
			return
		}
		if err := reg.JitterBeforeDrain(rq.Context()); err != nil {
			writeResult(w, err)
			return
		}
		err := reg.Drain(rq.Context())
		writeResult(w, err)
	})
	mux.HandleFunc("/v1/ready", func(w http.ResponseWriter, rq *http.Request) {
		if rq.Method != http.MethodPost {
			http.Error(w, "POST", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			EgressIP   string `json:"egress_ip"`
			PrevEgress string `json:"prev_egress"`
		}
		if err := json.NewDecoder(rq.Body).Decode(&body); err != nil || body.EgressIP == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "err": "egress_ip required"})
			return
		}
		writeResult(w, reg.Ready(context.Background(), body.EgressIP, body.PrevEgress))
	})
	mux.HandleFunc("/v1/report", func(w http.ResponseWriter, rq *http.Request) {
		if rq.Method != http.MethodPost {
			http.Error(w, "POST", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			EgressIP string `json:"egress_ip"`
			NetFault bool   `json:"net_fault"`
			CPUFault bool   `json:"cpu_fault"`
			MemFault bool   `json:"mem_fault"`
		}
		if err := json.NewDecoder(rq.Body).Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "err": "bad json"})
			return
		}
		err := reg.Report(context.Background(), registrar.Report{
			NetFault: body.NetFault, CPUFault: body.CPUFault, MemFault: body.MemFault, EgressIP: body.EgressIP,
		})
		if err != nil {
			writeResult(w, err)
			return
		}
		st := reg.Status()
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "registered_ip": st.EgressIP, "drained": st.Drained})
	})
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, rq *http.Request) {
		writeJSON(w, http.StatusOK, reg.Status())
	})
	srv := &http.Server{Addr: fc.API, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	if err := reg.Stop(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func writeResult(w http.ResponseWriter, err error) {
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	case errors.Is(err, registrar.ErrEgressMismatch), errors.Is(err, registrar.ErrEgressUnchanged), errors.Is(err, registrar.ErrEgressUnconfirmed), errors.Is(err, registrar.ErrNodeConflict):
		writeJSON(w, http.StatusConflict, map[string]any{"ok": false, "err": err.Error()})
	case errors.Is(err, registrar.ErrNotRegistered), errors.Is(err, registrar.ErrDrainTimeout):
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ok": false, "err": err.Error()})
	default:
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ok": false, "err": err.Error()})
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func readIP(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func loopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("api 只能绑在回环地址，当前是 %s", addr)
	}
	return nil
}

func setMS(dst *time.Duration, ms int) {
	if ms > 0 {
		*dst = time.Duration(ms) * time.Millisecond
	}
}
