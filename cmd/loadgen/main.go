// Command loadgen 按 PoC 的方式把 N 个 L2 lease 打到本机 etcd 上。
// 每个逻辑主机：Grant + Put /relay/l2/{id}，全部注册后做一次带 epoch 标记的 Drain 再写回，
// 之后按续租间隔 KeepAliveOnce。
// 客户端配置全部成员。-kill-after 时停掉当前 leader，用来看续租错误和 lease 过期。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/f-ishelly/relay-proxy/internal/etcdcluster"
	"github.com/f-ishelly/relay-proxy/registrar"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func main() {
	hosts := flag.Int("hosts", 400, "逻辑主机数，也就是 lease 数")
	conns := flag.Int("conns", 32, "共享的 etcd 客户端数，每个都配置全部成员")
	ttl := flag.Int64("ttl", 10, "lease TTL 秒")
	renew := flag.Duration("renew", 3*time.Second, "续租间隔")
	keep := flag.Duration("keep", 20*time.Second, "全部注册完成并切主之后，继续续租这么久")
	killAfter := flag.Duration("kill-after", 0, "全部注册完成后，再等这么久杀掉 leader；0 表示不杀")
	registerTimeout := flag.Duration("register-timeout", 60*time.Second, "等待全部 Grant+Put 的上限")
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

	clis := make([]*clientv3.Client, *conns)
	for i := range clis {
		c, err := clientv3.New(clientv3.Config{
			Endpoints:            eps,
			DialTimeout:          3 * time.Second,
			DialKeepAliveTime:    2 * time.Second,
			DialKeepAliveTimeout: 2 * time.Second,
			Logger:               zap.NewNop(),
		})
		if err != nil {
			fatal(err)
		}
		clis[i] = c
	}
	defer func() {
		for _, c := range clis {
			_ = c.Close()
		}
	}()

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

	var registered, kaErr, kaOK, kaRaw, leaseLost, grantErr, drainOK, drainErr atomic.Int64
	redialStart := make(chan struct{})
	var rawErr atomic.Value
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < *hosts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cli := clis[i%len(clis)]
			time.Sleep(time.Duration(i%5000) * time.Millisecond)
			key := fmt.Sprintf("/relay/l2/%06d", 600000+i)
			lid, err := register(ctx, cli, key, i, *ttl)
			if err != nil {
				grantErr.Add(1)
				return
			}
			registered.Add(1)
			select {
			case <-ctx.Done():
				return
			case <-redialStart:
			}
			// 等全部注册时，先上去的 lease 已经走了一段 TTL。重拨前先续一次。
			{
				c, cancel := context.WithTimeout(ctx, 3*time.Second)
				_, _ = cli.KeepAliveOnce(c, lid)
				cancel()
			}
			time.Sleep(time.Duration(i%500) * time.Millisecond)
			if err := redial(ctx, cli, key, lid, i); err != nil {
				drainErr.Add(1)
			} else {
				drainOK.Add(1)
			}
			tk := time.NewTicker(*renew)
			defer tk.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tk.C:
				}
				var resp *clientv3.LeaseKeepAliveResponse
				var err error
				for attempt := 0; attempt < 3; attempt++ {
					c, cancel := context.WithTimeout(ctx, 3*time.Second)
					resp, err = cli.KeepAliveOnce(c, lid)
					cancel()
					if ctx.Err() != nil {
						return
					}
					if err == nil && resp != nil && resp.TTL > 0 {
						break
					}
					if leaseMissing(err) || (err == nil && (resp == nil || resp.TTL <= 0)) {
						break
					}
					kaRaw.Add(1)
					rawErr.CompareAndSwap(nil, err.Error())
					time.Sleep(200 * time.Millisecond)
				}
				if ctx.Err() != nil {
					return
				}
				if err == nil && resp != nil && resp.TTL > 0 {
					kaOK.Add(1)
					continue
				}
				if leaseMissing(err) || (err == nil && (resp == nil || resp.TTL <= 0)) {
					leaseLost.Add(1)
					lid, err = register(ctx, cli, key, i, *ttl)
					if err != nil {
						grantErr.Add(1)
						return
					}
					continue
				}
				kaErr.Add(1)
			}
		}(i)
	}

	regDeadline := time.Now().Add(*registerTimeout)
	for time.Now().Before(regDeadline) && registered.Load() < int64(*hosts) {
		time.Sleep(200 * time.Millisecond)
	}
	up := registered.Load()
	fmt.Printf("{\"event\":\"registered\",\"label\":%q,\"hosts\":%d,\"up\":%d,\"grant_err\":%d,\"elapsed_ms\":%d,\"endpoints\":%q}\n",
		*label, *hosts, up, grantErr.Load(), time.Since(start).Milliseconds(), strings.Join(eps, ","))
	if up == int64(*hosts) {
		close(redialStart)
		redialDeadline := time.Now().Add(*registerTimeout)
		for time.Now().Before(redialDeadline) && drainOK.Load()+drainErr.Load() < up {
			time.Sleep(200 * time.Millisecond)
		}
	}
	fmt.Printf("{\"event\":\"redial\",\"label\":%q,\"drain_ok\":%d,\"drain_err\":%d,\"elapsed_ms\":%d}\n",
		*label, drainOK.Load(), drainErr.Load(), time.Since(start).Milliseconds())

	killed := ""
	if *killAfter > 0 && cluster != nil && up == int64(*hosts) {
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

	summary := map[string]any{
		"event":             "summary",
		"label":             *label,
		"hosts":             *hosts,
		"conns":             *conns,
		"endpoints":         len(eps),
		"ttl_s":             *ttl,
		"renew_ms":          renew.Milliseconds(),
		"registered":        up,
		"keepalive_ok":      kaOK.Load(),
		"keepalive_err":     kaErr.Load(),
		"keepalive_raw_err": kaRaw.Load(),
		"keepalive_raw_sample": func() string {
			s, _ := rawErr.Load().(string)
			return s
		}(),
		"lease_lost":    leaseLost.Load(),
		"grant_err":     grantErr.Load(),
		"drain_ok":      drainOK.Load(),
		"drain_err":     drainErr.Load(),
		"watch_put":     watchPut.Load(),
		"watch_del":     watchDel.Load(),
		"leader_killed": killed != "",
		"elapsed_ms":    time.Since(start).Milliseconds(),
	}
	b, _ := json.Marshal(summary)
	fmt.Println(string(b))
	// 每台主机恰好一次计划内删除。多出来的 DELETE 才是 lease 过期。
	if up != int64(*hosts) || drainOK.Load() != up || drainErr.Load() != 0 || kaErr.Load() != 0 || leaseLost.Load() != 0 || watchDel.Load() != drainOK.Load() {
		os.Exit(1)
	}
}

func hostBody(i int) string {
	return fmt.Sprintf(`{"addr":"10.0.0.%d:8080","egress_ip":"100.64.%d.%d","proxy_port":8080,"control_port":17001,"node_id":"%06d","az":"az01","weight":100,"capability":"http-connect","version":"gost-v3","registered_at":%d}`,
		(i%250)+1, (i/256)%256, i%256, 600000+i, time.Now().UnixMilli())
}

// redial 模仿持有者的 Drain：只有本 lease 还挂着 key 时，才在同一次事务里写 epoch 标记并删除 key，然后再写回。
func redial(ctx context.Context, cli *clientv3.Client, key string, lease clientv3.LeaseID, i int) error {
	node := fmt.Sprintf("%06d", 600000+i)
	epochKey := registrar.EpochKey(node)
	body := hostBody(i)
	marker := fmt.Sprintf(`{"instance_id":"load-%s","node_id":"%s","addr":"10.0.0.%d:8080","machine_id":"loadgen"}`, node, node, (i%250)+1)
	var last error
	for n := 0; n < 4; n++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c, cancel := context.WithTimeout(ctx, 3*time.Second)
		gr, err := cli.Grant(c, 30)
		if err != nil {
			cancel()
			last = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		tr, err := cli.Txn(c).
			If(clientv3.Compare(clientv3.LeaseValue(key), "=", lease)).
			Then(
				clientv3.OpPut(epochKey, marker, clientv3.WithLease(gr.ID)),
				clientv3.OpDelete(key),
			).
			Commit()
		if err != nil || tr == nil || !tr.Succeeded {
			_, _ = cli.Put(c, key, body, clientv3.WithLease(lease))
			_, _ = cli.Revoke(c, gr.ID)
			cancel()
			if err != nil {
				last = err
			} else {
				last = fmt.Errorf("drain txn lost")
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		_, err = cli.Put(c, key, body, clientv3.WithLease(lease))
		if err != nil {
			cancel()
			last = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		_, err = cli.Txn(c).
			If(clientv3.Compare(clientv3.Value(epochKey), "=", marker)).
			Then(clientv3.OpDelete(epochKey)).
			Commit()
		_, rerr := cli.Revoke(c, gr.ID)
		cancel()
		if err != nil {
			last = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if rerr != nil && !leaseMissing(rerr) {
			last = rerr
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return nil
	}
	if last == nil {
		last = fmt.Errorf("redial failed")
	}
	return last
}

func register(ctx context.Context, cli *clientv3.Client, key string, i int, ttl int64) (clientv3.LeaseID, error) {
	var last error
	for n := 0; n < 8; n++ {
		c, cancel := context.WithTimeout(ctx, 3*time.Second)
		gr, err := cli.Grant(c, ttl)
		if err == nil {
			_, err = cli.Put(c, key, hostBody(i), clientv3.WithLease(gr.ID))
		}
		cancel()
		if err == nil {
			return gr.ID, nil
		}
		last = err
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		time.Sleep(100 * time.Millisecond)
	}
	if last == nil {
		last = fmt.Errorf("register failed")
	}
	return 0, last
}

func leaseMissing(err error) bool {
	return errors.Is(err, rpctypes.ErrLeaseNotFound)
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
