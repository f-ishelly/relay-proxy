// Command admin 演示已有 admin 进程如何在进程内调用 registrar。
// 拨号、出口校验和定期上报仍由 admin 自己做；这里只把注册时机接上。
//
//	RELAY_NODE_ID=626220 go run ./example/admin -demo \
//	  -endpoints http://127.0.0.1:2379,http://127.0.0.1:2380,http://127.0.0.1:2381 \
//	  -addr 10.0.8.26:8080 -egress-ip 100.64.0.1 -next-egress-ip 100.64.0.2 \
//	  -probe 127.0.0.1:8080 -probe-user 'appuser@626220' -probe-pass s3cret \
//	  -state /tmp/registrar-state.json -ip-file /tmp/ppp-ip
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/f-ishelly/relay-proxy/registrar"
)

func main() {
	demo := flag.Bool("demo", false, "对真实 etcd 和 gost 跑一轮上线、重拨、上报、停止")
	endpoints := flag.String("endpoints", "", "全部 etcd 成员，逗号分隔")
	nodeID := flag.String("node-id", os.Getenv(registrar.NodeIDEnv), "nodeId，默认读环境变量 RELAY_NODE_ID")
	nodeIDFile := flag.String("node-id-file", "", "RELAY_NODE_ID 也没有时才读的文件")
	addr := flag.String("addr", "", "内网 IP:gost 端口，写入 value.addr")
	egress := flag.String("egress-ip", "", "当前出口 IP")
	nextEgress := flag.String("next-egress-ip", "", "重拨后外部看到的出口 IP，必须和重拨前不同")
	probe := flag.String("probe", "", "本机 gost 探测地址")
	probeUser := flag.String("probe-user", "", "探测用户名")
	probePass := flag.String("probe-pass", "", "探测口令")
	state := flag.String("state", "", "lease / drained 状态文件")
	ipFile := flag.String("ip-file", "", "本机 ppp 地址文件；空文件表示链路断开")
	dial := flag.Duration("dial", registrar.DefaultAssumedDial, "admin 拨号耗时（registrar 不睡眠，只有演示循环使用）")
	check := flag.Duration("egress-check", registrar.DefaultAssumedEgressCheck, "admin 出口校验耗时")
	inspect := flag.Duration("inspection-interval", registrar.DefaultInspectionInterval, "admin 巡检间隔，默认 1 分钟；查外网、CPU 和内存")
	flag.Parse()
	if !*demo {
		fmt.Fprintf(os.Stderr, "嵌入方式：\n\n%s\n", snippet)
		fmt.Fprintln(os.Stderr, "加 -demo 才会连接 etcd。-h 查看参数。")
		os.Exit(2)
	}
	if *endpoints == "" || *addr == "" || *egress == "" || *probe == "" || *state == "" || *ipFile == "" || *nextEgress == "" {
		fmt.Fprintln(os.Stderr, "缺少 -endpoints/-addr/-egress-ip/-next-egress-ip/-probe/-state/-ip-file")
		os.Exit(2)
	}
	if *nextEgress == *egress {
		fmt.Fprintln(os.Stderr, "重拨后出口 IP 与重拨前相同，校验失败，不会注册")
		os.Exit(2)
	}
	if err := os.WriteFile(*ipFile, []byte(*egress+"\n"), 0o600); err != nil {
		fatal(err)
	}
	_, portText, err := net.SplitHostPort(*addr)
	if err != nil {
		fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		fatal(err)
	}
	cfg := registrar.DefaultConfig()
	cfg.Endpoints = strings.Split(*endpoints, ",")
	cfg.NodeID = *nodeID
	cfg.NodeIDFile = *nodeIDFile
	cfg.IntranetAddr = *addr
	cfg.ProxyPort = port
	cfg.ProbeAddr = *probe
	cfg.ProbeUser = *probeUser
	cfg.ProbePassword = *probePass
	cfg.StateFile = *state
	cfg.AssumedDial = *dial
	cfg.AssumedEgressCheck = *check
	cfg.InspectionInterval = *inspect
	cfg.LocalIP = func() string { return readTrim(*ipFile) }
	cfg.Logger = registrar.JSONLogger{Out: os.Stdout}
	cfg.OnAlert = func(a registrar.Alert) {
		fmt.Fprintf(os.Stderr, "ALERT %s %s\n", a.Kind, a.Message)
	}

	reg, err := registrar.New(cfg)
	if err != nil {
		fatal(err)
	}
	ctx := context.Background()
	if err := reg.Start(ctx); err != nil {
		fatal(err)
	}
	defer func() { fatal(reg.Stop(ctx)) }()

	if err := readyRetry(ctx, reg, *egress, ""); err != nil {
		fatal(err)
	}
	fmt.Println("在线", reg.Status().Key, "lease", reg.Status().LeaseID)

	// 计划内重拨：先确认 key 删除，再拨号。外部出口必须变化，否则 Ready 拒绝注册。
	if err := reg.Drain(ctx); err != nil {
		fatal(err)
	}
	fmt.Println("已从 etcd 摘除，开始拨号")
	time.Sleep(cfg.AssumedDial + cfg.AssumedEgressCheck)
	if err := os.WriteFile(*ipFile, []byte(*nextEgress+"\n"), 0o600); err != nil {
		fatal(err)
	}
	if err := readyRetry(ctx, reg, *nextEgress, *egress); err != nil {
		fatal(err)
	}
	fmt.Println("重拨后重新在线，lease", reg.Status().LeaseID)

	// 巡检每分钟一次：外网、CPU、内存都正常才保持注册。
	time.Sleep(cfg.InspectionInterval)
	if err := reg.Report(ctx, registrar.Report{EgressIP: *nextEgress}); err != nil {
		fatal(err)
	}
	fmt.Println("上报完成", reg.Status())
}

func readyRetry(ctx context.Context, reg *registrar.Registrar, egress, prev string) error {
	var err error
	for i := 0; i < 30; i++ {
		err = reg.Ready(ctx, egress, prev)
		if err == nil || !errors.Is(err, registrar.ErrNotRegistered) {
			return err
		}
		time.Sleep(200 * time.Millisecond)
	}
	return err
}

func readTrim(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func fatal(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

const snippet = `cfg := registrar.DefaultConfig()
cfg.Endpoints = []string{"http://etcd-1:2379", "http://etcd-2:2379", "http://etcd-3:2379"}
// 留空时 New 会读 RELAY_NODE_ID。运维保证唯一，冲突检测只是兜底。
cfg.NodeID = os.Getenv(registrar.NodeIDEnv)
cfg.IntranetAddr = intranetIP + ":" + strconv.Itoa(gostPort)
cfg.ProxyPort = gostPort
cfg.ProbeAddr = "127.0.0.1:" + strconv.Itoa(gostPort)
cfg.ProbeUser, cfg.ProbePassword = proxyUser, sharedSecret
cfg.StateFile = "/var/lib/relay-registrar/" + cfg.NodeID + ".json"
cfg.LocalIP = readPPP // 链路断开时返回 ""
reg, err := registrar.New(cfg)
err = reg.Start(ctx)
// 重拨 API。一次拨号加一次出口校验大约 5 秒，失败率低于 1%。
err = reg.Drain(ctx) // etcd 确认删除后才挂断
dial()
egressIP, prevEgress, changed := egressCheck() // 外部看到的出口必须变
if !changed {
    return // 不调用 Ready
}
err = reg.Ready(ctx, egressIP, prevEgress)
// 每分钟巡检。零值表示健康；外网、CPU、内存故障要把对应字段设为 true。
err = reg.Report(ctx, registrar.Report{NetFault: !netOK, CPUFault: cpuFault, MemFault: memFault, EgressIP: egressIP})
err = reg.Stop(ctx)`
