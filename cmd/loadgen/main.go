// Command loadgen 把 N 个 registrar 打到本机 etcd 上。
// 每台逻辑主机自己拨全部成员，Start 之后 Ready，全部注册完成后再 Drain、换出口、Ready。
// 续租由 registrar 自己的循环完成。-kill-after 时停掉当前 leader。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/f-ishelly/relay-proxy/internal/etcdcluster"
	"github.com/f-ishelly/relay-proxy/registrar"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func main() {
	hosts := flag.Int("hosts", 400, "逻辑主机数")
	conns := flag.Int("conns", 32, "已忽略：每个 registrar 自己拨全部成员")
	ttl := flag.Int64("ttl", 10, "lease TTL 秒")
	renew := flag.Duration("renew", 3*time.Second, "续租间隔")
	keep := flag.Duration("keep", 20*time.Second, "全部重拨完成并切主之后，继续让 registrar 续租这么久")
	killAfter := flag.Duration("kill-after", 0, "全部重拨完成后，再等这么久杀掉 leader；0 表示不杀")
	registerTimeout := flag.Duration("register-timeout", 60*time.Second, "等待全部 Ready，以及之后全部 Drain+Ready 的上限")
	endpoints := flag.String("endpoints", "", "逗号分隔的成员；为空且 -embed 时在本进程拉起 3 节点")
	embedCluster := flag.Bool("embed", true, "在本进程启动 3 节点 etcd")
	label := flag.String("label", "load", "")
	flag.Parse()

	var cluster *etcdcluster.Cluster
	eps := split(*endpoints)
	if *embedCluster {
		dir, err := os.MkdirTemp("", "registrar-load-*")
		if err != nil {
			fatal(err)
		}
		defer os.RemoveAll(dir)
		cluster, err = etcdcluster.Start(dir, 3)
		if err != nil {
			fatal(err)
		}
		defer cluster.Close()
		eps = cluster.Endpoints()
	}
	if len(eps) == 0 {
		fatal(fmt.Errorf("没有 etcd 成员"))
	}
	if len(eps) < 3 {
		fmt.Fprintf(os.Stderr, "warning: 只配置了 %d 个成员，生产环境必须列出全部成员\n", len(eps))
	}

	gostAddr, stopGost := fakeGost()
	defer stopGost()
	stateDir, err := os.MkdirTemp("", "registrar-load-state-*")
	if err != nil {
		fatal(err)
	}
	defer os.RemoveAll(stateDir)

	counts := &countLog{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var watchDel, watchPut atomic.Int64
	go func() {
		wc, err := clientv3.New(clientv3.Config{Endpoints: eps, DialTimeout: 3 * time.Second, Logger: zap.NewNop()})
		if err != nil {
			return
		}
		defer wc.Close()
		for wr := range wc.Watch(ctx, "/relay/l2/", clientv3.WithPrefix()) {
			for _, ev := range wr.Events {
				if ev.Type == clientv3.EventTypeDelete {
					watchDel.Add(1)
				} else {
					watchPut.Add(1)
				}
			}
		}
	}()

	var registered, readyErr, drainOK, drainErr atomic.Int64
	redialStart := make(chan struct{})
	regs := make([]*registrar.Registrar, *hosts)
	perHost := make([]*hostLog, *hosts)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < *hosts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			time.Sleep(time.Duration(i%8000) * time.Millisecond)
			node := fmt.Sprintf("%06d", 600000+i)
			ip1 := egressIP(i, 0)
			ip2 := egressIP(i, 1)
			var cur atomic.Value
			cur.Store(ip1)
			hlog := &hostLog{base: counts}
			perHost[i] = hlog
			cfg := registrar.DefaultConfig()
			cfg.Endpoints = eps
			cfg.NodeID = node
			cfg.IntranetAddr = intranet(i)
			cfg.ProxyPort = 8080
			cfg.ProbeAddr = gostAddr
			cfg.ProbeUser = "load"
			cfg.ProbePassword = "load"
			cfg.StateFile = filepath.Join(stateDir, node+".json")
			cfg.MachineID = "load-" + node
			cfg.LocalIP = func() string {
				s, _ := cur.Load().(string)
				return s
			}
			cfg.TTL = time.Duration(*ttl) * time.Second
			cfg.RenewInterval = *renew
			cfg.ProbeInterval = 30 * time.Second
			cfg.ProbeTimeout = 800 * time.Millisecond
			cfg.ProbeRiseThreshold = 1
			cfg.ProbeFailThreshold = 5
			cfg.IPGuardInterval = time.Minute
			cfg.StartGrantTimeout = 20 * time.Second
			cfg.DialTimeout = 3 * time.Second
			cfg.DialKeepAliveTime = 30 * time.Second
			cfg.DialKeepAliveTimeout = 10 * time.Second
			cfg.Logger = hlog
			reg, err := registrar.New(cfg)
			if err != nil {
				readyErr.Add(1)
				return
			}
			regs[i] = reg
			sctx, scancel := context.WithTimeout(context.Background(), 20*time.Second)
			err = reg.Start(sctx)
			scancel()
			if err != nil {
				readyErr.Add(1)
				return
			}
			deadline := time.Now().Add(8 * time.Second)
			for time.Now().Before(deadline) && !reg.Status().ProbeOK {
				time.Sleep(20 * time.Millisecond)
			}
			rctx, rcancel := context.WithTimeout(context.Background(), 8*time.Second)
			err = reg.Ready(rctx, ip1, "")
			rcancel()
			if err != nil {
				readyErr.Add(1)
				return
			}
			registered.Add(1)
			select {
			case <-ctx.Done():
				return
			case <-redialStart:
			}
			dctx, dcancel := context.WithTimeout(context.Background(), 8*time.Second)
			err = reg.Drain(dctx)
			dcancel()
			if err != nil {
				drainErr.Add(1)
				return
			}
			cur.Store(ip2)
			rctx, rcancel = context.WithTimeout(context.Background(), 8*time.Second)
			err = reg.Ready(rctx, ip2, ip1)
			rcancel()
			if err != nil {
				drainErr.Add(1)
				return
			}
			drainOK.Add(1)
		}(i)
	}

	regDeadline := time.Now().Add(*registerTimeout)
	for time.Now().Before(regDeadline) && registered.Load()+readyErr.Load() < int64(*hosts) {
		time.Sleep(200 * time.Millisecond)
	}
	up := registered.Load()
	fmt.Printf("{\"event\":\"registered\",\"label\":%q,\"hosts\":%d,\"up\":%d,\"ready_err\":%d,\"elapsed_ms\":%d,\"endpoints\":%q}\n",
		*label, *hosts, up, readyErr.Load(), time.Since(start).Milliseconds(), strings.Join(eps, ","))
	if up == int64(*hosts) && readyErr.Load() == 0 {
		close(redialStart)
		redialDeadline := time.Now().Add(*registerTimeout)
		for time.Now().Before(redialDeadline) && drainOK.Load()+drainErr.Load() < up {
			time.Sleep(200 * time.Millisecond)
		}
	}
	fmt.Printf("{\"event\":\"redial\",\"label\":%q,\"drain_ok\":%d,\"drain_err\":%d,\"elapsed_ms\":%d}\n",
		*label, drainOK.Load(), drainErr.Load(), time.Since(start).Milliseconds())

	killed := ""
	if *killAfter > 0 && cluster != nil && up == int64(*hosts) && drainErr.Load() == 0 {
		time.Sleep(*killAfter)
		ep, err := cluster.KillLeader()
		if err != nil {
			fmt.Printf("{\"event\":\"kill_leader_error\",\"err\":%q}\n", err.Error())
		} else {
			killed = ep
			fmt.Printf("{\"event\":\"kill_leader\",\"endpoint\":%q,\"t_ms\":%d}\n", ep, time.Since(start).Milliseconds())
		}
	}
	time.Sleep(*keep)
	cancel()
	wg.Wait()
	time.Sleep(300 * time.Millisecond)

	samples := make([]int, 0, *hosts)
	var kaMin, kaMax int
	for _, h := range perHost {
		if h == nil {
			continue
		}
		n := int(h.ok.Load())
		samples = append(samples, n)
	}
	sort.Ints(samples)
	p50 := 0
	if len(samples) > 0 {
		kaMin = samples[0]
		kaMax = samples[len(samples)-1]
		p50 = samples[len(samples)/2]
	}
	summary := map[string]any{
		"event":                "summary",
		"label":                *label,
		"hosts":                *hosts,
		"conns_flag":           *conns,
		"registrar_clients":    *hosts,
		"client_mode":          "registrar",
		"endpoints":            len(eps),
		"ttl_s":                *ttl,
		"renew_ms":             renew.Milliseconds(),
		"registered":           up,
		"ready_err":            readyErr.Load(),
		"keepalive_ok":         counts.kaOK.Load(),
		"keepalive_ok_min":     kaMin,
		"keepalive_ok_p50":     p50,
		"keepalive_ok_max":     kaMax,
		"keepalive_err":        counts.kaErr.Load(),
		"keepalive_err_sample": counts.sample(),
		"lease_lost":           counts.leaseLost.Load(),
		"drain_ok":             drainOK.Load(),
		"drain_err":            drainErr.Load(),
		"watch_put":            watchPut.Load(),
		"watch_del":            watchDel.Load(),
		"leader_killed":        killed != "",
		"elapsed_ms":           time.Since(start).Milliseconds(),
	}
	b, _ := json.Marshal(summary)
	fmt.Println(string(b))

	stopAll(regs)
	// 每台主机恰好一次计划内删除。多出来的 DELETE 是 lease 过期。
	// keepalive_err 是 registrar 单次 KeepAliveOnce 失败，切主时可能出现，随后会重试。
	// 通过条件是租约没有丢、也没有计划外的 DELETE。
	if up != int64(*hosts) || readyErr.Load() != 0 || drainOK.Load() != up || drainErr.Load() != 0 || counts.leaseLost.Load() != 0 || watchDel.Load() != drainOK.Load() {
		os.Exit(1)
	}
}

func intranet(i int) string {
	n := i
	c := (n % 250) + 1
	n /= 250
	b := n % 256
	n /= 256
	a := (n % 250) + 1
	return fmt.Sprintf("10.%d.%d.%d:8080", a, b, c)
}

func egressIP(i, gen int) string {
	host := i + gen*100000
	return fmt.Sprintf("100.%d.%d.%d", 64+(host/65536)%60, (host/256)%256, host%256)
}

func fakeGost() (string, func()) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fatal(err)
	}
	done := make(chan struct{})
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				select {
				case <-done:
					return
				default:
				}
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					continue
				}
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_ = c.SetDeadline(time.Now().Add(2 * time.Second))
				buf := make([]byte, 1024)
				_, _ = c.Read(buf)
				_, _ = c.Write([]byte("HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n"))
			}(c)
		}
	}()
	return ln.Addr().String(), func() {
		close(done)
		_ = ln.Close()
	}
}

func stopAll(regs []*registrar.Registrar) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, 64)
	for _, reg := range regs {
		if reg == nil {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(reg *registrar.Registrar) {
			defer wg.Done()
			defer func() { <-sem }()
			c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_ = reg.Stop(c)
			cancel()
		}(reg)
	}
	wg.Wait()
}

type countLog struct {
	kaOK, kaErr, leaseLost atomic.Int64
	raw                    atomic.Value
}

func (l *countLog) Event(ev string, args ...any) {
	switch ev {
	case "ka_ok":
		l.kaOK.Add(1)
	case "ka_err":
		l.kaErr.Add(1)
		if l.raw.Load() == nil {
			l.raw.Store(arg(args, "err"))
		}
	case "lease_lost":
		l.leaseLost.Add(1)
		if l.raw.Load() == nil {
			l.raw.Store(arg(args, "err"))
		}
	}
}

func (l *countLog) sample() string {
	s, _ := l.raw.Load().(string)
	return s
}

type hostLog struct {
	base *countLog
	ok   atomic.Int64
}

func (h *hostLog) Event(ev string, args ...any) {
	h.base.Event(ev, args...)
	if ev == "ka_ok" {
		h.ok.Add(1)
	}
}

func arg(args []any, key string) string {
	for i := 0; i+1 < len(args); i += 2 {
		if fmt.Sprint(args[i]) == key {
			return fmt.Sprint(args[i+1])
		}
	}
	return ""
}

func split(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
