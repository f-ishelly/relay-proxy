// Command admin 演示已有 admin 进程如何在进程内调用 registrar。
// 拨号、出口校验和定期上报仍由 admin 自己做；这里只把注册时机接上。
//
//	go run ./example/admin -demo \
//	  -endpoints http://127.0.0.1:2379,http://127.0.0.1:2380,http://127.0.0.1:2381 \
//	  -node-id 626220 -addr 10.0.8.26:8080 -egress-ip 100.64.0.1 \
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
	nodeID := flag.String("node-id", "", "主机已有的 nodeId")
	nodeIDFile := flag.String("node-id-file", "", "nodeId 文件；与 -node-id 二选一")
	addr := flag.String("addr", "", "内网 IP:gost 端口，写入 value.addr")
	egress := flag.String("egress-ip", "", "当前出口 IP")
	nextEgress := flag.String("next-egress-ip", "", "重拨后的出口 IP，默认同 -egress-ip")
	probe := flag.String("probe", "", "本机 gost 探测地址")
	probeUser := flag.String("probe-user", "", "探测用户名")
	probePass := flag.String("probe-pass", "", "探测口令")
	state := flag.String("state", "", "lease / drained 状态文件")
	ipFile := flag.String("ip-file", "", "本机 ppp 地址文件；空文件表示链路断开")
	dial := flag.Duration("dial", registrar.DefaultAssumedDial, "admin 拨号耗时（registrar 不睡眠，只有演示循环使用）")
	check := flag.Duration("egress-check", registrar.DefaultAssumedEgressCheck, "admin 出口校验耗时")
	inspect := flag.Duration("inspection-interval", registrar.DefaultInspectionInterval, "admin 巡检/上报间隔")
	flag.Parse()
	if !*demo {
		fmt.Fprintf(os.Stderr, "嵌入方式：\n\n%s\n", snippet)
		fmt.Fprintln(os.Stderr, "加 -demo 才会连接 etcd。-h 查看参数。")
		os.Exit(2)
	}
	if *endpoints == "" || *addr == "" || *egress == "" || *probe == "" || *state == "" || *ipFile == "" {
		fmt.Fprintln(os.Stderr, "缺少 -endpoints/-addr/-egress-ip/-probe/-state/-ip-file")
		os.Exit(2)
	}
	if *nextEgress == "" {
		*nextEgress = *egress
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

	if err := readyRetry(ctx, reg, *egress); err != nil {
		fatal(err)
	}
	fmt.Println("在线", reg.Status().Key, "lease", reg.Status().LeaseID)

	// 计划内重拨：先确认 key 删除，再拨号，校验通过才 Ready。校验失败就不要调用 Ready。
	if err := reg.Drain(ctx); err != nil {
		fatal(err)
	}
	fmt.Println("已从 etcd 摘除，开始拨号")
	time.Sleep(cfg.AssumedDial)
	if err := os.WriteFile(*ipFile, []byte(*nextEgress+"\n"), 0o600); err != nil {
		fatal(err)
	}
	time.Sleep(cfg.AssumedEgressCheck)
	if err := readyRetry(ctx, reg, *nextEgress); err != nil {
		fatal(err)
	}
	fmt.Println("重拨后重新在线，lease", reg.Status().LeaseID)

	// 定期上报。netOK 来自 admin 自己的巡检；这里演示一次健康上报。
	time.Sleep(cfg.InspectionInterval)
	if err := reg.Report(ctx, true, *nextEgress); err != nil {
		fatal(err)
	}
	fmt.Println("上报完成", reg.Status())
}

func readyRetry(ctx context.Context, reg *registrar.Registrar, egress string) error {
	var err error
	for i := 0; i < 30; i++ {
		err = reg.Ready(ctx, egress)
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
cfg.NodeID = nodeID
cfg.IntranetAddr = intranetIP + ":" + strconv.Itoa(gostPort)
cfg.ProxyPort = gostPort
cfg.ProbeAddr = "127.0.0.1:" + strconv.Itoa(gostPort)
cfg.ProbeUser, cfg.ProbePassword = proxyUser, sharedSecret
cfg.StateFile = "/var/lib/relay-registrar/" + nodeID + ".json"
cfg.LocalIP = readPPP // 链路断开时返回 ""
reg, err := registrar.New(cfg)
err = reg.Start(ctx)
// 重拨 API：
err = reg.Drain(ctx)          // etcd 确认删除后才挂断
dial()                        // 只动外网出口
if !egressCheck() { dial() }  // 失败不注册
err = reg.Ready(ctx, egressIP)
// 原有巡检：
err = reg.Report(ctx, netOK, egressIP)
// 进程退出：
err = reg.Stop(ctx)`
