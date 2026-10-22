# relay-proxy

L2 节点向 etcd 注册的实现在 Go 包 [`registrar`](registrar)。每个节点是一台虚拟机，admin 也是 Go 程序，在进程内调用这个包。`cmd/registrard` 只是暂时不能改 admin 时的本机 HTTP 备选。

nodeId 来自环境变量 `RELAY_NODE_ID`，由运维保证唯一。客户端必须配置全部 etcd 成员。一次拨号加一次出口校验大约 5 秒；计划内重拨后，外部看到的出口 IP 如果没变，就不注册。

```go
cfg := registrar.DefaultConfig()
cfg.Endpoints = []string{"http://etcd-1:2379", "http://etcd-2:2379", "http://etcd-3:2379"}
cfg.NodeID = os.Getenv(registrar.NodeIDEnv) // RELAY_NODE_ID
cfg.IntranetAddr = intranetIP + ":" + strconv.Itoa(gostPort)
cfg.ProxyPort = gostPort
cfg.ProbeAddr = "127.0.0.1:" + strconv.Itoa(gostPort)
cfg.StateFile = "/var/lib/relay-registrar/" + cfg.NodeID + ".json"
cfg.LocalIP = readPPP // 链路断开时返回 ""
reg, err := registrar.New(cfg)
err = reg.Start(ctx)
err = reg.Drain(ctx) // 确认删除后再挂断
// dial + egress check，合计约 5s。出口没变就不要 Ready。
err = reg.Ready(ctx, egressIP, prevEgress)
err = reg.Report(ctx, registrar.Report{NetFault: !netOK, CPUFault: cpuFault, MemFault: memFault, EgressIP: egressIP})
err = reg.Stop(ctx)
```

完整演示在 [`example/admin`](example/admin)。注册模型、巡检和 L1 发现见 [`docs/openspec-proxy-design.md`](docs/openspec-proxy-design.md) §8。
