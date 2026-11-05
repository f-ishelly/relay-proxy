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

设计文档 [`docs/openspec-proxy-design.md`](docs/openspec-proxy-design.md) 现为 v0.14。告警范围、明文 HTTP 和计量事件以这一版为准。

明文 HTTP 目前只在 `internal/docfixture` 里实现，构建标签是 `docfixture`。生产 L1 还没接上这套数据面。

文档里的代码块与夹具是否一致，跑 `go test -tags docfixture -race ./internal/docfixture/...`。`GOST_REQUIRED=1` 时缺少 gost 即失败。gost 为 v3.3.1，用 `GOST_BIN` 指定，或自动下载并校验 sha256。

CI 在 `push` 和 `pull_request` 上跑。矩阵是 `ubuntu-latest`（amd64）和 `ubuntu-24.04-arm`（arm64），每个 job 的环境变量是 `GOST_REQUIRED=1`。每个 job 依次执行：

1. `go vet ./...`
2. `go vet -tags docfixture ./...`
3. `go list ./...` 去掉路径含 `/internal/docfixture` 的包；若还剩包，就对这些包执行 `go test -race`
4. `go test -tags docfixture -race -count=3 ./internal/docfixture/...`
5. 下载 Prometheus v2.55.1 里与该架构对应的归档（amd64 或 arm64）并校验 sha256，然后执行 `promtool check rules internal/docfixture/prom/sli_rules.yml` 和 `promtool test rules internal/docfixture/prom/sli_test.yml`

`MeterEvent` 的 `client_ip`：含 IP 的原始日志保留 30 天。以后进 ClickHouse 不留原始 IP，或只留 /24。附录 B 里 Decodo 用量统计写的 180 天是对方产品的用量统计保留期，不是原始 `client_ip` 的保留期。
