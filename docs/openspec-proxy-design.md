# OpenSpec 设计文档：多级高性能隧道代理（Pro 模式）

- **文档版本**：v0.4（预研阶段）
- **日期**：2026-10-22
- **状态**：Draft / 可进入 PoC
- **目标**：参考商业隧道代理 Pro 模式，设计“一级代理 + 二级代理”多级转发系统，支持通过 `username` 指定二级节点、TLS 指纹透传、应用层与 eBPF 监控、etcd 自动服务发现。
- **v0.4 变更**：落地 L2 向 etcd 的注册（issue #5），并吸收 v0.3.1 的文档勘误，不另立版本号。节点是虚拟机，admin 是 Go，进程内调用 `registrar`；`nodeId` 来自环境变量 `RELAY_NODE_ID`。key 为 `/relay/l2/{nodeId}`，`value.addr` 是内网地址。每台虚拟机一个 lease（TTL 10s，每 3s 续租）。一次拨号加出口校验约 5s、失败率低于 1%；计划内重拨后出口 IP 必须变化，否则不注册。巡检每 1 分钟，外网或 CPU/内存异常都 Delete key。同一宿主机异常时一批 key 一起过期。详见 §8。勘误（原 v0.3.1）：§10 示例对空口令显式回 `407`；§10 与 §12 写明整条隧道空闲兜底（默认 5 分钟）会关掉长时间静默的 WebSocket、长轮询等长连接，应用需自带心跳，TCP keepalive 不算数据；§4.2 TunnelForwarder 补上 L2 `407` 透传和两级空闲计时；附录 A.5 写明缺端口回 `400`；§4.1 架构图删去 L2 到 Prometheus 的指标连线，gost 自带指标不在 M1；§7.3 注明等待 L2 应答时客户端先断开也计入 `l2_response`。v0.2.1 变更说明补全了当时引入的 400/407 规则。
- **v0.3 变更**：M1 PoC 的可观测改为 L1 应用层 Prometheus 指标。L1 用 `prometheus/client_golang` 暴露 `proxy_conn_active`、`proxy_connect_latency_ms`、`proxy_upstream_fail_total`，以及本地拒绝计数 `proxy_client_reject_total`。eBPF 只采集内核指标，移到 M2。两类来源和告警在 §7 分开。与 v0.2.1 的半关闭和 10s 应答等待合在同一份数据面上打点。`node_id` 失败关闭：只有 `Resolve` 返回 `ErrNoHealthyAddr` 时保留原来的 ID，其余解析错误记 `unknown`。
- **v0.2.1 变更**：修正 §10 的隧道拆除和 L2 应答等待。某一方向 `io.Copy` 返回错误（对端 RST 或写失败）时立刻完全关闭两端。干净 EOF 只 `CloseWrite`。反方向是空闲读超时而不是一次性截止：默认 30s（`halfCloseIdleTimeout`，可改），每次成功读到字节都重新计时，客户端先 FIN 和上游先 FIN 用同一套。整条隧道另有空闲兜底，默认 5 分钟（`tunnelIdleTimeout`，可改），任一方向有数据就重新计时；gost 的 `idleTimeout` 默认是 0，不靠它收尸。等待 L2 `200` 的默认上限是 10s，因为 gost 先拨通源站再应答。可观测性改版另记为 v0.3，本版不改 §7。L1 在本地校验共享口令（SHA-256 后常量时间比较），为空或不匹配直接回 `407`、不拨 L2；目标缺端口或用户名切不出 `node_id` 回 `400`，`node_id` 取最后一个 @ 之后；L2 返回的 `407` 连同 `Proxy-Authenticate` 交给客户端，其余失败仍为 `502`（§5.1、§5.2）。
- **v0.2 变更**：补上 v0.1 的链路缺口。L1 用第二次 HTTP `CONNECT` 把目标 `host:port` 交给 L2；L2 定为 gost v3 的 HTTP 正向代理，不再使用 HAProxy / Envoy 的纯 TCP 模式。对比与理由见附录 A。
- **已确认**：v1 的 L2 就是 gost v3，3proxy 与锁死配置的 Envoy 只作为同接口替换项。出口拒绝范围只有环回和链路本地，不含 RFC1918。L1→L2 使用客户端登录 L1 的那一份共享口令，不轮换。

---

## 1. 背景与目标

### 1.1 背景
当前需要构建一个可替代商业 Pro 隧道代理能力的内部系统，满足以下核心诉求：

1. 支持 HTTP/HTTPS 隧道代理（重点 CONNECT）。
2. 初版通过认证用户名参数指定二级代理节点 ID。
3. 一级代理将流量转发到指定二级代理，再由二级代理访问目标站点。目标 `host:port` 由 L1 在第二次 `CONNECT` 中显式交给 L2。
4. 对目标网站的 TLS 指纹信息透传（不在一级/二级进行 TLS 中间人解密，也不重新生成 ClientHello）。
5. 可观测分两层：M1 由 L1 暴露应用层 Prometheus 指标（建连时延、活跃隧道、上游失败、本地拒绝）；M2 用 eBPF 采集内核网络与系统指标。
6. 二级节点采用开源高性能代理方案；一级节点基于开源方案二次开发。
7. 通过 etcd 实现二级节点自动注册、发现、健康感知。

### 1.2 非目标（初版）

- 不做 HTTPS MITM 解密与内容审计。
- 不做复杂计费系统（仅预留指标）。
- 不做跨地域智能调度（初版可基于静态策略 + etcd 健康状态）。
- 不在本文件决定明文 HTTP（非 CONNECT）的头部处理，以及 L1↔L2 传输加密方式。etcd 注册的实现形态已在 §8 定下来：admin 进程内嵌 `registrar`，不再使用独立的 HAProxy / Envoy 注册 sidecar。

---

## 2. 需求分解

### 2.1 功能需求

- 认证：支持 `BasicAuth`/`Proxy-Authorization`。全系统一份共享口令，不轮换。客户端用它登录 L1；L1 校验后把同一组用户名和密码写进发往 L2 的 `Proxy-Authorization`。用户名仍是 `user@node_id`，供 L1 选路。v1 接受由此带来的暴露面：任一 L2 都能看到这把钥匙，泄露后它对 L1 和全部 L2 继续有效，也不能按调用方吊销。
- 路由：从 `username` 解析二级节点 ID（如：`user@node-az01-003`），再从本地路由表得到该节点的 L2 监听地址。
- 转发：L1 向选中的 L2 发送 HTTP `CONNECT host:port`，L2 返回 `200` 后，双向原样转发隧道字节。
- 协议：支持 HTTP 请求代理 + HTTPS CONNECT 隧道。本版把 CONNECT 隧道的目标传递定下来；明文 HTTP 仍是独立数据面。
- 透传：TLS 握手报文、ALPN、SNI、JA3/JA4 特征在链路上不被篡改。`200` 之后只有 TCP 字节转发，没有 TLS 终结或重新握手。

### 2.2 非功能需求

- 高性能：单节点万级并发连接能力（视机器规格）。
- 高可用：一级无状态可水平扩展；二级可动态扩容缩容。
- 可观测：QPS、时延、成功率、连接生命周期、错误码分布。
- 可运维：etcd 自动发现，心跳 + TTL 下线。

---

## 3. 开源方案预研与对比

> 说明：以下为工程实践视角的预研建议，用于“一级二次开发 + 二级直接落地”的选型输入。完整对比表、TLS 结论与配置红线在附录 A。

### 3.1 候选组件

L1 与 L2 的职责不同，不能拿同一类软件硬套。

1. **一级（自研 Go）**
   - 要做终端用户认证，以及 `username -> node_id -> L2 地址` 的强制定向。
   - 要做的协议动作很小：向已选定的 L2 发一次 HTTP `CONNECT`，读到 `200` 后双向拷贝。
   - 这一跳的稳定接口是 HTTP `CONNECT`。L2 进程可以替换，L1 不用改。

2. **二级候选（动态正向代理，而不是固定后端的 L4 负载均衡）**
   - **gost v3**（v1 已确认）：HTTP handler 默认就是 `CONNECT` 之后双向转发。
   - **3proxy**：同一 CONNECT 接口下，C + Linux `splice`，连接密度更高。
   - **Envoy dynamic forward proxy**：能终止 `CONNECT` 并原始 TCP 转发，指标与热重启最好；安全配置与官方 HTTP 示例相反，容易配成重新发起 TLS。
   - **不采用 HAProxy / Nginx stream 的纯 TCP 模式**：它们只会把连接转到预先配置的后端，不能按 `CONNECT` 里的目标动态拨号；v0.1 那种裸 TCP 透传里也没有目标地址，L2 无法知道该连哪台源站。

### 3.2 推荐组合（初版）

- **一级代理（需二开）**：Go 自研网关（借鉴/嵌入高性能开源网络库能力，例如 `gnet`/`fasthttp`/标准库 netpoll 方案）。
- **L1→L2 目标传递**：第二次 HTTP `CONNECT`（代理链）。请求目标使用客户端原始 authority（`host:port`），DNS 在 L2 做。
- **二级代理（开箱即用）**：gost v3，`handler: http` + `listener: tcp`。关闭 sniffing、MITM 与压缩。这是 v1 的确定选择。
- **认证**：一份共享口令。L2 的 `auth.username` 写成客户端访问本节点时使用的用户名（含 `@node_id`），`auth.password` 与客户端登录 L1 的密码相同。L1 转发这同一组凭据。L2 只校验它，不从用户名里做选路。v1 每个 L2 节点只对应一个客户端用户名；若以后同一节点要接受多个用户名，需要把 `auth` 换成 gost 的多用户认证配置。
- **出口拒绝**：L2 在解析之后、拨号之前拒绝环回与链路本地。RFC1918 及其他私网地址本版放行，此范围以后可以再收紧。见附录 A.5。
- **服务发现**：etcd。L1 拨的地址是 `value.addr`（内网 IP:gost 端口），不随 ADSL 重拨变化。注册方式见 §8。
- **监控**：M1 由 L1 暴露应用层 Prometheus 指标；M2 用 eBPF 采集内核指标。两者都进 Prometheus + Grafana。见 §7。
- **同接口替换项**（不改变 L1 的 CONNECT）：要更高连接密度用 3proxy；要 Envoy 级指标或 L1↔L2 的 HTTP/2 多路复用时，用锁死配置的 Envoy DFP。二者都不是 v1 的 L2。红线见附录 A.5，替换条件见附录 A.6。

gost 的配置形态：

```yaml
services:
  - name: l2
    addr: ":8080"
    bypass: egress-deny
    handler:
      type: http
      auth:
        username: appuser@node-az01-003
        password: <shared-secret>
      metadata:
        sniffing: false
    listener:
      type: tcp
bypasses:
  - name: egress-deny
    matchers:
      - 127.0.0.0/8
      - 169.254.0.0/16
      - ::1/128
      - fe80::/10
```

`listener` 保持 `tcp`。`http+tls` / `https` 监听会终结的是 L1↔L2 这一跳的外层 TLS，不是客户端到源站的 TLS；外层加密不在本文件决定。`mitm.certFile` / `mitm.keyFile` 不应出现。上面的 `bypass` 是黑名单，只比请求里的主机字符串；域名解析进这些网段时不会命中。拨号前还要对解析结果做一次同样的网段判断，见附录 A.5。

### 3.3 选择理由

- 一级要实现“用户名 -> 二级节点 ID -> 强定向转发”，逻辑定制性高，自研（基于 Go）效率最高。选路在拨号之前完成，L2 不解析 `node_id`。
- v0.1 把 L2 做成纯 TCP 透传时，链路上没有任何字段携带源站 `host:port`，二级无法拨号。HAProxy 也没有“按 CONNECT 目标动态拨号”的正向代理模式。
- 第二次 `CONNECT` 用一次往返把目标、端口和同一份共享口令放进同一条标准请求；L2 连上源站后再回 `200`。SOCKS5 能表达同样的目标，但握手往返更多，而且 Envoy 不能作为现成的 SOCKS 服务端接入。PROXY protocol 传递的是本跳的源/目的地址，不是正向代理的拨号目标。
- TLS 指纹要保持，靠的是 `200` 之后原样转发客户端已经写出的 TLS 记录，而不是“随便找一个 L4 代理”。gost 的默认数据面就是这条隧道；Envoy 必须额外写对 `connect_config`，并且不能配置上游 TLS。
- 单节点万级连接落在 gost 可运行的范围内。默认临时端口约 2.8 万（`32768–60999`），一条 L1 源地址到一个 L2 地址的 HTTP/1.1 隧道数量与此同一量级，覆盖本版“万级”目标。

---

## 4. 总体架构设计

## 4.1 架构图

```mermaid
flowchart LR
    C[Client SDK / Browser] -->|HTTP Proxy / CONNECT| L1[一级代理集群\nGo 二次开发]
    L1 -->|watch| E[(etcd)]
    L1 -->|HTTP CONNECT 携带目标| L2A[二级代理 A\ngost HTTP]
    L1 -->|HTTP CONNECT 携带目标| L2B[二级代理 B\ngost HTTP]
    L1 -->|HTTP CONNECT 携带目标| L2N[二级代理 N\ngost HTTP]
    L2A -->|TCP 原样转发| T[Target Website]
    L2B -->|TCP 原样转发| T
    L2N -->|TCP 原样转发| T

    subgraph Obs[可观测体系]
      BPF[eBPF Agent\nM2 内核指标]
      P[Prometheus]
      G[Grafana]
    end

    L1 -. 应用层指标 .-> P
    BPF -. 内核指标 .-> P
    P --> G
```

### 4.2 关键模块

- **AuthParser**：解析客户端 `Proxy-Authorization`，用全系统共享口令完成认证，并保留用户名与密码供转发。
- **RouteResolver**：解析 `username` 中 node_id 并在本地路由表匹配。
- **EtcdWatcher**：Watch `/relay/l2/`，维护 `node_id -> addr`。`addr` 是 value 里的内网 `host:port`。key 存在即可路由，不读状态字段。见 §8.2。
- **TunnelForwarder**：向选定 L2 写 `CONNECT host:port`，`Proxy-Authorization` 使用客户端刚刚提交的同一组用户名和密码。确认 `200` 后与客户端双向原样转发。不在这条路径上 `tls.Dial`。L2 返回 `407` 时连同 `Proxy-Authenticate` 交给客户端。隧道阶段有半关闭空闲超时和整条隧道的空闲兜底，见 §10。
- **Metrics/Trace**：M1 在 L1 进程内暴露应用层 Prometheus 指标。M2 的 eBPF 内核指标另行采集，不在转发路径上计算。见 §7。

---

## 5. 关键流程设计

### 5.1 请求时序图（HTTPS CONNECT）

```mermaid
sequenceDiagram
    participant U as User Client
    participant L1 as L1 Proxy (Go)
    participant E as etcd
    participant L2 as L2 gost
    participant T as Target

    U->>L1: CONNECT target:443 + Proxy-Authorization（共享口令）
    L1->>L1: 校验共享口令并解析 username -> node_id
    L1->>E: (本地缓存失效时) 查询 node_id 可用地址
    E-->>L1: 返回 L2 的 HTTP 监听地址
    L1->>L2: CONNECT target:443 + 同一份 Proxy-Authorization
    L2->>L2: 校验同一口令；解析目标；拒绝环回与链路本地
    L2->>T: TCP 握手
    L2-->>L1: 200 Connection Established
    L1-->>U: 200 Connection Established
    U->>L1: TLS ClientHello（客户端原始字节）
    L1->>L2: 原样转发隧道字节
    L2->>T: 原样转发隧道字节
    T-->>U: TLS ServerHello 及后续记录（经 L2、L1 原样返回）
```

L1 必须先收到 L2 的 `200`（L2 已经与目标完成 TCP 握手），再向客户端回 `200`。在此之前客户端不应发出 ClientHello。客户端口令为空或不匹配时，L1 直接回 `407`，不拨 L2，计入 `proxy_client_reject_total{reason="auth"}`。目标不是 `host:port`（缺少端口）回 `400`，`reason="bad_target"`；用户名里没有 `node_id` 回 `400`，`reason="no_node"`。用户名或口令含 CR/LF 也是本地 `400`，`reason="bad_credential"`。这些本地 400/407 都不拨 L2。L2 返回 `407` 时，L1 把 `407` 和 `Proxy-Authenticate` 交给客户端，不改写成 `502`，计入 `proxy_upstream_fail_total{reason="l2_auth"}`。L2 的其他非 2xx、应答超时或拨号失败才转为 `502`，按 §7.1 的 `reason` 计入 `proxy_upstream_fail_total`。等待状态行时的读超时是 `reason="l2_response"`。这些失败路径上都不会开始传输隧道字节。建连时延和活跃隧道的打点见 §7.1。

### 5.2 流程图（路由决策）

```mermaid
flowchart TD
    A[收到代理请求] --> B{认证是否通过}
    B -- 否 --> X[返回 407]
    B -- 是 --> C[解析 username]
    C --> D{是否包含 node_id}
    D -- 否 --> Y[返回 400]
    D -- 是 --> T{target 是否为 host:port}
    T -- 否 --> Y
    T -- 是 --> E{本地缓存是否有可用节点}
    E -- 否 --> F[从 etcd 拉取并刷新缓存]
    F --> G{节点可用?}
    E -- 是 --> G
    G -- 否 --> Z[返回 502]
    G -- 是 --> H[向 L2 发送 CONNECT 并等待应答]
    H --> J{L2 状态码}
    J -- 200 --> K[向客户端返回 200]
    J -- 407 --> X
    J -- 其他或超时 --> Z
    K --> I[双向原样转发并记录指标]
```

图中的 `X`、`Y`、`Z`、`H`、`J`、`K` 与 §7.1 对应。`X`（本地 407）和 `Y`（本地 400）计入 `proxy_client_reject_total`，不拨 L2。进入 `Z` 的 `502` 计入 `proxy_upstream_fail_total`，`reason` 按路径分开。`J -- 407` 不进入 `Z`，单独记 `reason="l2_auth"`。`proxy_connect_latency_ms` 在 `J` 为 200 时观察，区间从 `H` 拨号到读到 L2 的 `200`。`proxy_conn_active` 在 `K` 向客户端写出 `200` 之后随隧道增减；客户端断开后，这个 gauge 最长还会多停半关闭空闲（默认 30s）或隧道空闲（默认 5 分钟）。标签、桶，以及和 §10 每一处回写的对应，见 §7.1。

---

## 6. TLS 指纹透传设计

### 6.1 原则

- 不启用 MITM，不在 L1 或 L2 上对客户端握手调用 TLS 栈。
- 客户端 `CONNECT` 与 L1→L2 的 `CONNECT` 都只是隧道建立前的 HTTP 控制面。两条 `200` 都写出之后，负载是客户端已经生成的 TLS 记录。
- L1 与 L2 对这段负载只做字节转发，不重写、不重分段重组后再编码、不插入 PROXY 协议头。
- L2 到目标是 L2 自己发起的 TCP 连接。目标看到的 TCP/IP 指纹属于 L2；目标看到的 TLS ClientHello 属于客户端。JA3/JA4 只覆盖后者。

### 6.2 风险点与规避

- 风险：代理对源站重新发起 TLS（会换成该代理自己的 ClientHello，JA3/JA4 都变）。
  - 规避：L2 只做 `CONNECT` 终止 + 原始 TCP 转发。gost 不配置 `mitm.*`；不把 Envoy 集群配成 `transport_socket: tls`；3proxy 不启用 `ssl_mitm` / `ssl_cli`。
- 风险：L2 把还没剥掉的 PROXY 协议头、HTTP 头或压缩后的字节写给源站。源站会直接握手失败，而不只是指纹变化。
  - 规避：不使用 PROXY protocol 传递目标；`200` 之后的第一个字节就是客户端写出的第一个字节。
- 风险：为了“看一眼 SNI”而解析并重新序列化 ClientHello（扩展顺序、GREASE、padding 都会变，JA3 必变）。
  - 规避：目标已经在 `CONNECT` 的 authority 里，L2 不需要读 ClientHello。sniffing 保持关闭。
- 风险：透明代理/NAT 设备插入行为导致特征变化。
  - 规避：在出口链路做抓包校验，对比客户端侧与目标侧的 ClientHello 字节和 JA3/JA4。

### 6.3 验证方法

- 使用 `tcpdump` + `ja3`/`ja4` 解析工具，比较客户端发出的 ClientHello 与目标侧收到的 ClientHello。除 TCP 分段位置外，握手字节应一致。
- 构建回归测试：不同客户端（Chrome/curl/Go TLS）一致性验证。
- 配置回归：带有 `mitm`、上游 TLS 或 `ssl_bump` 的 L2 配置应无法通过上述比对。

---

## 7. 可观测性

指标有两个来源，都进 Prometheus，采集位置不同。

- **应用层（M1）**：L1 进程用 `prometheus/client_golang` 暴露 `proxy_conn_active`、`proxy_connect_latency_ms`、`proxy_upstream_fail_total` 和 `proxy_client_reject_total`。这些指标在 L1 的请求路径上更新。前三项是 §7.3 的 SLI。
- **内核（M2）**：eBPF Agent 采集 `kernel_tcp_retrans_total`、RTT、run queue latency 等内核网络与系统指标。

### 7.1 应用层指标（M1）

L1 注册下面四项，并在独立的管理监听地址上提供 `/metrics`。管理端口与代理端口分开，不接受 `CONNECT`，也不检查 `Proxy-Authorization`。每个 L1 副本暴露自己的值，集群总量由 Prometheus 聚合。默认 registry 同时导出 client_golang 自带的 Go 运行时和进程指标。本节的 SLI 是 `proxy_conn_active`、`proxy_connect_latency_ms`、`proxy_upstream_fail_total`。`proxy_client_reject_total` 只计拨号 L2 之前的本地拒绝，不进 §7.3 的失败率。

L2 不负责这些指标。gost 自带的 metrics/observer 不在 M1 范围内。本版只覆盖 CONNECT 隧道；明文 HTTP 的计数不在本文件决定。

标签取值有上界：

- `node_id`：本地路由表中已有的二级节点 ID，包含在表里但当前没有健康地址的节点。用户名里写出、但路由表从未收录的 ID 不进入标签，固定为 `unknown`。§10 只在 `Resolve` 返回 `ErrNoHealthyAddr` 时保留原来的 ID。`ErrUnknownNode`、其他任何错误，以及成功但地址为空，都记 `unknown`，避免把用户名里的任意 ID 写进标签。这些失败的 `reason` 都是 `no_upstream`。
- `reason`（`proxy_upstream_fail_total`）：`no_upstream`、`dial`、`l2_write`、`l2_deadline`、`l2_response`、`l2_status`、`l2_auth`。常量导出，§10 在另一个包里按 `l1metrics.ReasonDial` 这种名字使用。
- `reason`（`proxy_client_reject_total`）：`auth`、`no_node`、`bad_target`、`bad_credential`。这个计数没有 `node_id`，避免把未收录的 ID 或用户名写进标签。

不使用目标主机、URL、用户名、源地址或错误原文做标签。

| 指标 | 类型 | 标签 | 含义 |
| --- | --- | --- | --- |
| `proxy_conn_active` | Gauge | `node_id` | 已经向客户端写出 `200`、`handleConnect` 尚未返回的隧道数。客户端断开后 gauge 仍可能多计这条隧道：半关闭后反方向空闲最长再等 `halfCloseIdleTimeout`（默认 30s）；两边都没有字节时由 `tunnelIdleTimeout`（默认 5 分钟）收尸 |
| `proxy_connect_latency_ms` | Histogram | `node_id` | L2 返回 `200` 时，从开始拨号 L2 到读完该状态的毫秒数。抓取文本里的桶列为 `proxy_connect_latency_ms_bucket`，同族还有 `_sum` 与 `_count` |
| `proxy_upstream_fail_total` | Counter | `node_id`, `reason` | 没有建成隧道的上游结果。除 `l2_auth` 外，客户端收到的都是 `502`。`l2_auth` 是 L2 返回的 `407`，客户端看到的仍是 `407` |
| `proxy_client_reject_total` | Counter | `reason` | L1 在拨 L2 之前返回的本地 `400` / `407` |

`proxy_connect_latency_ms` 的桶上界（毫秒）：`5, 10, 25, 50, 100, 200, 500, 1000, 3000, 10000`。`200` 与 §7.3 的成功建连 P95 阈值对齐，`3000` 与 §10 的 `DialTimeout`（3s）对齐。成功样本的等待不止这 3s：§10 还要等 L2 的状态行，默认 `l2ResponseTimeout` 为 10s，gost 先拨通源站才回 `200`。因此 3s 到 10s 的成功建连会落入 `10000` 这个桶，该桶会有样本。拨号时间加上这 10s 仍可能超过 10000ms，那些观察落在 `+Inf` 桶。失败路径不观察，不进任何桶。

计时起点是 §10 拨号 L2 之前，对应 §5.2 的节点 `H`。拨号函数是 `dialUpstream`，默认就是 `net.DialTimeout("tcp", addr, 3*time.Second)`。`node_id` 在拨号及之后来自路由表命中项。

| 位置 | `proxy_upstream_fail_total` | `proxy_connect_latency_ms` | `proxy_conn_active` | `proxy_client_reject_total` |
| --- | --- | --- | --- | --- |
| 用户名或口令含 CR/LF，本地 `400` | 不增加 | 不观察 | 不变 | `reason="bad_credential"` |
| 口令为空或不匹配，本地 `407`，不拨 L2 | 不增加 | 不观察 | 不变 | `reason="auth"` |
| 用户名没有 `node_id`，本地 `400` | 不增加 | 不观察 | 不变 | `reason="no_node"` |
| `target` 不是 `host:port`（缺少端口等），本地 `400` | 不增加 | 不观察 | 不变 | `reason="bad_target"` |
| §5.2 `G -- 否`；§10 `Resolve` 失败或返回空地址并写 `502`。只有 `ErrNoHealthyAddr` 保留该 ID；`ErrUnknownNode`、其他错误和空地址都是 `node_id="unknown"` | `reason="no_upstream"` | 不观察。尚未拨号 | 不变 | 不增加 |
| §10 `dialUpstream` 失败并写 `502`。默认实现是 `net.DialTimeout` 3s | `reason="dial"` | 不观察 | 不变 | 不增加 |
| §10 `upstreamConn.Write` 失败并写 `502` | `reason="l2_write"` | 不观察 | 不变 | 不增加 |
| §10 `SetReadDeadline` 失败并写 `502`。包括设上 `l2ResponseTimeout`，以及读完后清掉截止时间 | `reason="l2_deadline"` | 不观察 | 不变 | 不增加 |
| §10 读取 L2 状态行失败并写 `502`，包括读超时。等待期间客户端先断开、导致这次读取失败，也走这里 | `reason="l2_response"` | 不观察 | 不变 | 不增加 |
| §5.2 `J -- 407`；§10 L2 返回 `407`，原样交给客户端 | `reason="l2_auth"` | 不观察 | 不变 | 不增加 |
| §5.1 L2 的其他非 2xx；§5.2 `J -- 其他或超时` 里的非 2xx；§10 状态码不是 200 也不是 407，并写 `502` | `reason="l2_status"` | 不观察 | 不变 | 不增加 |
| L2 返回 200 | 不增加 | 观察，在写客户端 `200` 之前 | 不变 | 不增加 |
| §5.1 / §10 向客户端写出 `200` 成功，随后两侧拷贝 | 不增加 | 不再观察 | 写出成功后 `Inc`；`handleConnect` 返回时 `Dec`。返回可能晚于客户端断开最多 30s（半关闭空闲）或 5 分钟（隧道空闲） | 不增加 |

`502` 与 L2 的 `407`（`l2_auth`）进入 `proxy_upstream_fail_total`。时延直方图只收录 L2 已返回 `200` 的尝试。本地 `400` / `407` 只进入 `proxy_client_reject_total`。

下面是注册与打点函数。打点落在 §10 `handleConnect` 的这些位置，常量都是导出的，另一个包可以引用：

1. 本地 `400` / `407`：按上表调用 `RejectClient(ReasonRejectBadCredential)`、`RejectClient(ReasonRejectAuth)`、`RejectClient(ReasonRejectNoNode)` 或 `RejectClient(ReasonRejectBadTarget)`，再 `reply`。不调用 `Fail502`。
2. `Resolve` 失败，或返回的地址为空：`inTable := errors.Is(err, ErrNoHealthyAddr)`（空地址时 `err` 为 nil，`inTable` 为 false），然后 `Fail502(NodeLabel(inTable, nodeID), ReasonNoUpstream)`，再 `badGateway`。不是 `ErrNoHealthyAddr` 的错误，以及空地址，标签都是 `unknown`。
3. 拨号前 `started := time.Now()`。`dialUpstream` 失败：`Fail502(nodeID, ReasonDial)`，再 `badGateway`。
4. `upstreamConn.Write` 失败：`Fail502(nodeID, ReasonL2Write)`，再 `badGateway`。
5. `SetReadDeadline` 失败：`Fail502(nodeID, ReasonL2Deadline)`，再 `badGateway`。
6. 读取状态行失败，含读超时，以及等待期间客户端先断开导致的读失败：`Fail502(nodeID, ReasonL2Response)`，再 `badGateway`。
7. 状态码是 407：`Fail502(nodeID, ReasonL2Auth)`，再把 `407` 和 `Proxy-Authenticate` 交给客户端。
8. 状态码不是 200 也不是 407：`Fail502(nodeID, ReasonL2Status)`，再 `badGateway`。
9. 状态码是 200：先 `ObserveConnect(nodeID, started)`，再写客户端 `200`。写成功后 `defer EnterTunnel(nodeID)()`，然后进入两侧拷贝。

```go
package l1metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	// NodeUnknown 是路由表从未收录的 node_id 在标签里的固定值。
	NodeUnknown = "unknown"

	ReasonNoUpstream = "no_upstream"
	ReasonDial       = "dial"
	ReasonL2Write    = "l2_write"
	ReasonL2Deadline = "l2_deadline"
	ReasonL2Response = "l2_response"
	ReasonL2Status   = "l2_status"
	ReasonL2Auth     = "l2_auth"

	ReasonRejectAuth          = "auth"
	ReasonRejectNoNode        = "no_node"
	ReasonRejectBadTarget     = "bad_target"
	ReasonRejectBadCredential = "bad_credential"
)

var (
	proxyConnActive = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "proxy_conn_active",
		Help: "已经向客户端返回 200、handleConnect 尚未返回的 L1 隧道数。客户端断开后最多滞后 30s（半关闭空闲）或 5 分钟（隧道空闲）。",
	}, []string{"node_id"})

	proxyConnectLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "proxy_connect_latency_ms",
		Help:    "L2 返回 200 的 CONNECT 所经过的毫秒数，从开始拨号 L2 算起。",
		Buckets: []float64{5, 10, 25, 50, 100, 200, 500, 1000, 3000, 10000},
	}, []string{"node_id"})

	proxyUpstreamFail = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "proxy_upstream_fail_total",
		Help: "没有建成隧道的上游结果。l2_auth 对应交给客户端的 L2 407，其余 reason 对应 502。",
	}, []string{"node_id", "reason"})

	proxyClientReject = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "proxy_client_reject_total",
		Help: "拨号 L2 之前，L1 本地返回 400 或 407 的次数。",
	}, []string{"reason"})
)

func init() {
	prometheus.MustRegister(proxyConnActive, proxyConnectLatency, proxyUpstreamFail, proxyClientReject)
}

// Mount 在管理 mux 上暴露 /metrics。该 mux 使用独立监听地址，不挂在代理端口上。
func Mount(mux *http.ServeMux) {
	mux.Handle("/metrics", promhttp.Handler())
}

// NodeLabel 只接受路由表里已有的 node_id。未收录或空字符串折叠为 unknown。
func NodeLabel(inTable bool, nodeID string) string {
	if !inTable || nodeID == "" {
		return NodeUnknown
	}
	return nodeID
}

// Fail502 增加 proxy_upstream_fail_total。
// ReasonNoUpstream、ReasonDial、ReasonL2Write、ReasonL2Deadline、ReasonL2Response、ReasonL2Status
// 由调用方接着对客户端写 502。ReasonL2Auth 时调用方把 L2 的 407 原样交还，不改写成 502。
func Fail502(nodeID, reason string) {
	proxyUpstreamFail.WithLabelValues(nodeID, reason).Inc()
}

// RejectClient 记录一次拨号 L2 之前的本地 400 或 407。
// reason 取 ReasonRejectAuth、ReasonRejectNoNode、ReasonRejectBadTarget、ReasonRejectBadCredential。
func RejectClient(reason string) {
	proxyClientReject.WithLabelValues(reason).Inc()
}

// ObserveConnect 记录从 started 到 L2 返回 200 的毫秒数。失败路径不调用。
func ObserveConnect(nodeID string, started time.Time) {
	elapsed := float64(time.Since(started)) / float64(time.Millisecond)
	proxyConnectLatency.WithLabelValues(nodeID).Observe(elapsed)
}

// EnterTunnel 在向客户端写出 200 成功之后调用。返回值在 handleConnect 返回时把 gauge 减一。
// 客户端断开后，减一仍可能再等半关闭空闲（默认 30s）或隧道空闲（默认 5 分钟）。
func EnterTunnel(nodeID string) (leave func()) {
	gauge := proxyConnActive.WithLabelValues(nodeID)
	gauge.Inc()
	return gauge.Dec
}
```

### 7.2 内核指标（M2，eBPF）

M2 在一级与二级节点部署 eBPF Agent（如 Cilium，或自研 exporter），采集内核看到的网络与系统指标。连接数、CONNECT 时延和上游失败以 §7.1 为准。

- `kernel_tcp_retrans_total`：内核统计的 TCP 重传次数（Counter）。
- RTT：TCP RTT 分布。
- run queue latency：CPU 运行队列延迟。
- 同层还包括丢包、软中断和 socket backlog。

应用层建连时延包含 L2 拨号、出口 DNS 和源站 TCP 握手。内核 RTT 是本机 TCP 栈看到的往返时间。两个数分开告警。

### 7.3 告警建议

应用层（M1，只用 §7.1 的三项）：

- 5 分钟上游失败率 > 2%。分子是 `sum(increase(proxy_upstream_fail_total[5m]))`，包含 `l2_write`、`l2_deadline` 和 `l2_auth`。分母是该分子加上 `sum(increase(proxy_connect_latency_ms_count[5m]))`。`_count` 只包含 L2 已返回 `200` 的尝试，所以和失败计数相加就是全部已结束的上游尝试。`proxy_client_reject_total` 不进入这个比率。等待 L2 状态行时客户端先断开也记作 `l2_response`（§7.1），虽然不是上游故障，却会进入分子；比率升高时，先单独看 `reason="l2_response"` 的速率。
- 成功建连的 P95 > 200ms：`histogram_quantile(0.95, sum by (le) (rate(proxy_connect_latency_ms_bucket[5m])))`。直方图里只有 L2 返回 `200` 的样本。按节点告警时改为 `sum by (le, node_id)`。

内核（M2，eBPF）：

- `kernel_tcp_retrans_total` 的 5 分钟增量相对该节点基线明显升高。
- RTT 分布相对该节点基线明显升高。
- run queue latency 持续高于该节点基线。这是调度延迟，与上面的 CONNECT P95 分开看。

发现与摘除走 etcd lease（§8），单独执行，不读 §7.1 或 §7.2 的时间序列：

- 进程还在时，重拨、出口校验失败（含重拨后出口 IP 没变）、gost 探测失败、外网或 CPU/内存巡检失败、ppp 地址变化都是 Delete key，不等 TTL。
- 进程消失才等 lease 到期。节点是虚拟机，计划外掉线只在宿主机异常时发生，同一宿主机上的一批 key 会一起过期，L1 要承受这批 DELETE（§8.3）。TTL 10s，每 3s 续租；大约 3 次续租没到就会摘除。etcd 切主会把未到期的 lease 续满，这种情况下最长大约 16s。

---

## 8. etcd 自动服务发现设计

### 8.1 注册模型（已实现）

节点是虚拟机。注册跑在这台虚拟机已有的 admin 里，不另起 sidecar。admin 是 Go 程序，主路径是进程内调用本仓库的 Go 包 `registrar`，入口是 `Start`、`Drain`、`Ready`、`Report`、`Stop`。gost v3 仍是独立的 systemd 服务，只做 §3.2 / 附录 A.5 的 HTTP `CONNECT`。admin 原有的重拨 API 和定期上报保留；重拨是进程内先 `Drain`，再挂断拨号，出口校验通过后才 `Ready`。`example/admin` 是这段调用的参考。`cmd/registrard` 把同一组调用暴露在 `127.0.0.1`，只在暂时不能改 admin 时使用。

- Key：`/relay/l2/{nodeId}`，例如 `/relay/l2/626220`。一台虚拟机一个 gost，不再使用 `{instance_id}`。`nodeId` 来自环境变量 `RELAY_NODE_ID`（`registrar.NodeIDEnv`），由运维保证唯一。事务里的冲突检测保留，只作为兜底。
- Lease：每台主机一个，TTL 10s，每 3s `KeepAliveOnce`。重拨不 `Revoke`，同一次在线期间 lease ID 不变。
- 客户端：`Config.Endpoints` 原样传给 etcd，`AutoSyncInterval = 0`，避免成员列表被自动改写。生产环境必须列出全部成员，否则切主时连在已死成员上的 lease 会过期。

Value：

```json
{
  "addr": "10.0.8.26:8080",
  "egress_ip": "100.64.12.34",
  "proxy_port": 8080,
  "control_port": 17001,
  "node_id": "626220",
  "instance_id": "9f1c0e2a4b6d8f01a3c5e7b9d1f30426",
  "az": "az01",
  "weight": 100,
  "capability": "http-connect",
  "version": "gost-3.3.1",
  "registered_at": 1792655781744
}
```

`addr` 是 L1 拨的内网地址，重拨前后不变。`egress_ip` 只作信息。`weight` 会写进 value；v1 每个 nodeId 只有一个 key，L1 按 node_id 精确选路，不看 weight，所以资源异常不靠把 weight 写小来躲流量。

写入用事务：key 不存在、lease 属于本进程、lease 是本机状态文件里的上一任且 value 的 `instance_id` 也是本实例，或该 lease 已经失效，才允许 Put / Delete。否则视为 nodeId 冲突，不覆盖，并调用 `OnAlert`。活着的别人的 lease 用 `kind = node_id_conflict`。`instance_id` 对不上时用 `kind = foreign_instance`，并且不 Revoke 那条 lease。

`instance_id` 是第一次启动时生成的随机数，和当时的 `/etc/machine-id` 一起写进状态文件，也写进 value。状态文件被拷到另一台机器（`machine-id` 不同）时，新进程不认里面的 lease，也不会把原机的 key 撤掉。`machine-id` 或 `instance_id` 有一边是空的，同样不认、不撤。即便 `machine-id` 也被一起拷走，撤销之前会先读当前 key：`value.addr` 不是本机 `IntranetAddr`，或 `instance_id` 对不上，或这两个字段有一个是空的，都不 Revoke，也不覆盖。只有地址和实例都对得上，才把状态文件里的 lease 当成自己的。没有状态文件时，仅 `machine-id` 和内网地址相同不够拿回 key，必须等重拨标记过期。同一台机器丢掉状态文件再启动也一样：新进程的 `instance_id` 是新的，标记还在就不能注册，必须等到 EpochTTL（默认 30s）结束。克隆体如果连 `machine-id`、`instance_id`、内网地址全部相同，registrar 仍然分不出来，这是残留风险：克隆虚拟机时要重新生成 machine-id，不要把状态文件当成镜像的一部分。

状态文件（例如 `/var/lib/relay-registrar/{nodeId}.json`）记录 instance、lease、`drained`、当前出口，以及计划内重拨前的出口（`prev_egress`）。写文件时先把临时文件 fsync，再 rename，然后 fsync 目录。没有 `machine_id` 或 `instance_id` 的旧状态文件视为身份不明：启动后保持 `drained`，不撤旧 lease，也不自动注册，要等 admin 调用 `Ready`。`example/admin` 在 `Start` 之后会调用 `Ready`。`cmd/registrard` 只是 HTTP 适配，启动时不调用 `Ready`，等原来的 admin `POST /v1/ready`。

`Drain` 先把 `drained` 和“必须换出口”落盘。只有本进程登记过这个 key（写成功过，或状态文件认下了自己的身份）才会动 `/relay/l2-epoch/{nodeId}`。别人拿着同一个 nodeId 来 `Drain`，删不掉当前 key，也写不上标记；它自己的 lease 丢失同样不会写标记。持有者摘 key 时，标记和删除在同一次事务里：先确认业务 key 还是刚刚读到的那一版，再写上自己的标记并删除 key。别人提前留下的标记挡不住这次删除。客户端记着的 epoch lease 如果已经在服务端被撤销，这次事务会整笔失败；registrar 丢掉这个 lease id，重新 Grant，再提交一次。仍然失败才退回只删 key。刚 Grant 出来、事务又没写上标记的 lease 会马上 Revoke，不会留着空转续租。key 已经不在时，持有者只会在标记不存在或本来就是自己的时候补写，不会盖掉别人的标记。刷新标记之后会撤销被换掉的那条 epoch lease。

标记不在 `/relay/l2/` 前缀下，L1 的 Watch 看不到。它挂在自己的 epoch lease 上（默认 max(30s, 3×业务 TTL)，业务 TTL 为 10s 时就是 30s；进程还活着就按 EpochRenewInterval 续租，默认 10s），正文是 `instance_id`、`node_id`、`addr`、`machine_id`。所以业务 lease 在 drained 期间丢失时，标记还在。创建业务 key，以及靠“lease 已死”这条路径覆盖残留 key，都要求标记不存在，或属于本实例：`instance_id` 相同，并且标记里的 `addr` 为空（旧格式）或等于本机 `IntranetAddr`。只凭相同的 `addr` 和 `machine_id` 不能注册。注册成功后只删除本实例的这些标记。

重拨中崩溃并且状态文件也丢了时，新进程的 `instance_id` 是新的。标记上的内网地址和 `machine_id` 如果仍是这台机器，就不报 `foreign_instance`，但在标记过期之前仍然拒绝注册：`Ready` 即使带了不同的 `prevEgress` 也返回 `ErrNodeConflict`，也不会删掉持有者的标记。同一台机器必须等到 EpochTTL（默认 30s）之后才能重新注册。拷走状态文件、因此 `instance_id` 相同、但 `IntranetAddr` 已经不同的机器不算持有者，按 `foreign_instance` 拒绝。地址或 machine-id 对不上的标记同样是 `foreign_instance`。持有者一直停在 drained、不再调用 `Ready` 时，标记最多再续 `MaxDrained`（默认 10×EpochTTL；EpochTTL 为 30s 时是 5 分钟），然后停止续租并 `OnAlert`（`kind = epoch_hold_expired`），标记在剩余的一个 EpochTTL 内自己过期。续租循环到点时如果别的操作还占着锁，随后进来的 `Drain` 不会把标记再挂回这条已经停止续租的 epoch lease，也不会把这次新的窗口报成 `epoch_hold_expired`。它重新计算 `MaxDrained`，并 Grant 一条新的 epoch lease。进程退出后 epoch lease 不再续租。这是残留风险。

上次在线且本机 ppp 地址未变、状态文件也属于本机时，新进程 Grant 新 lease，用事务盖住原 key，再撤销旧 lease，Watch 不会看到 DELETE。这是进程重启，不是重拨，不要求出口变化。客户端如果误以为自己的 lease 已经丢失（例如切主后本地截止时间到了，服务端租约其实还在），旧 lease 会留在本实例的上一任列表里，重新 Put 时认领，而不是报成和自己冲突。

### 8.2 生命周期

| 调用 | 行为 |
| --- | --- |
| `Start` | 连接配置里的全部成员并 Grant。业务 lease 和 epoch lease 在独立 goroutine 里续租（默认每 3s / 10s），不跟 Drain、Ready、探测共用 `r.mu`。`r.mu` 也不跨过 etcd 调用和状态文件 fsync。探测每 1s，ppp 地址每 500ms。探测未通过不 Put |
| `Drain` | 同步 Delete 本 key。注册成功时就 Grant 一条还没有标记的 epoch lease 并继续续租，所以持有者的删除是一次事务：业务 lease 仍是自己的，才写上标记并删除 key。不再先 Get、再 Grant、再提交。单次尝试默认 2s；续租已经量到更大的 RTT 时，这一次拉到 1.5×RTT，但不超过剩余预算。失败后按指数退避加抖动再试（大约从 100ms 起到 500ms），总预算 6s，etcd 确认后才返回。不要用 700ms 的紧重试，延迟一高就会打成重试风暴。业务 lease 继续由独立循环续租，Drain 持有状态机锁时也续得上。记下重拨前的出口。还没注册过就 `Drain` 时没有出口可记，也不写 epoch 标记，之后的 `Ready` 必须由调用方传入一个不同的 `prevEgress`。别人的标记不会让这次删除失败。epoch lease 已在服务端撤销时，重新 Grant 并再试一次合并事务，然后才退回只删 key。事务在客户端超时但标记已经写成这次的 lease 时，不 Revoke 这条刚写上的标记。没写上标记的新 lease 会撤销。停在 drained 超过 `MaxDrained` 后不再续租标记；下一次 `Drain` 另 Grant，不复用那条已经停续的 lease |
| `Ready(egressIP, prevEgress)` | 本机 ppp 地址必须等于 `egressIP`，且 gost 探测已通过，才 Put。创建 key 的事务会同时看 epoch 标记；lease 已死的覆盖也看。能穿过标记的只有本实例：`instance_id` 相同，且 `addr` 为空或等于本机内网地址。重拨中：状态文件里的上一任出口和调用方传入的 `prevEgress` 都用来比对，相同则 `ErrEgressUnchanged`。状态文件损坏、`node_id` 对不上，或文件整个缺失时，都先按 `drained` 处理，`Report` 不能注册。损坏或 `node_id` 不符时，不传 `prevEgress` 得到 `ErrEgressUnconfirmed`。文件缺失且没有标记时，和第一次启动无法区分：`Ready` 不传 `prevEgress` 会注册。文件缺失但标记上的地址和 machine-id 是本机时，不报 `foreign_instance`，标记过期前 `Ready` 返回 `ErrNodeConflict`，不同的 `prevEgress` 也不能接管。`instance_id` 相同但 `addr` 不同，按 `foreign_instance` 拒绝 |
| `Report` | 每分钟一次。零值表示健康。`NetFault` 表示访问外部失败，`CPUFault` / `MemFault` 表示 CPU 或内存异常。任一故障则 Delete，恢复后（再传零值或没有故障位）Put。`drained` 期间，以及还没有一次通过出口校验的 `Ready`（或本机接管）时，忽略，不会把出口写进 etcd。上报正文不写 etcd |
| `Stop` | `Revoke` lease。进程被杀或宿主机异常时没有这一步，key 等 TTL |

后台探测向本机 gost 发带口令的 `CONNECT 127.0.0.1:1`，期望 `403`。这和 gost v3 的 HTTP 行为一致：认证通过之后，bypass 命中的 `CONNECT` 回 `403`，不是 `407`（go-gost 的 `TestHTTPConnect/bypass-403`）。连续 3 次失败就 Delete，连续 2 次成功再 Put，单次超时 800ms。ppp 守卫发现已注册出口和本机地址不一致，或链路断开（`LocalIP` 返回空），立刻 Delete 并进入 `drained`，直到下一次 `Ready`。这条路径不是计划内重拨，不额外要求出口必须变化。

一次拨号加一次出口校验合计约 5s（规划上按 4s 拨号、1s 校验，失败率低于 1%）。这段时间里 lease 继续每 3s 续租，所以 TTL 仍是 10s，`Drain` 的 6s 预算也不用拉长：`Drain` 在挂断之前就返回。registrar 不替 admin 睡眠，这两个时长只放在 `AssumedDial` 和 `AssumedEgressCheck` 里。

计划内、按时刻对齐的批量重拨必须先调用 `JitterBeforeDrain`。默认 `RedialJitter` 是 60s，每台机器在 `Drain` 之前随机等待 `[0, 60s)`。这是硬要求：同一时刻一起 `Drain` 会把 etcd 打满，业务 lease 的 10s TTL 保不住。`example/admin` 的演示可以用 `-redial-jitter 0` 跳过等待，生产配置不能设 0。`cmd/registrard` 没写 `redial_jitter_ms` 时也是这 60s。配置里把该字段写成 0 会拒绝启动，只有测试或演示加上 `-allow-zero-jitter` 才接受。`POST /v1/drain` 接受请求后立刻返回 202，抖动和 `Drain` 使用独立于 HTTP 请求的 context。客户端超时短于 60s、连接断开，也不会取消这次 `Drain`，key 不会因此留在 etcd。调用方用 `GET /v1/status` 看 `drained`。

下线用 Delete，不用 `status=draining`。key 在即可路由，不依赖每个 L1 都正确过滤状态。业务 key 仍是一次重拨删一次、写一次。持有者的 `Drain` 在同一次事务里写 epoch 标记，成功注册后再按条件删掉，所以重拨路径上的 etcd 写大约翻倍（4000 台同时重拨时，这部分大约从 2.2 次/s 抬到 4.4 次/s）。在线节点的 3s 续租不写这个标记。没登记过的实例不产生这些写。

### 8.3 L1 消费

- 启动时按前缀 `/relay/l2/` 全量拉取，之后 Watch 同一前缀。断线后从最后看到的 revision 续上。
- key 存在即可路由，拨 `value.addr`。不要拨 `egress_ip`。
- 拨号超时默认跟 §10 的 `dialUpstream` 一样，是 3s。§7.1 直方图里 `3000` 这个桶按的就是这 3s。进程消失后 key 还会留到 TTL：本仓库内嵌 etcd 上，约 10.3s 后 key 消失；消失 3s 后再切主，约 14.5s（验收上限 16s，etcd 会在选主时把 lease 续满）。如果部署要把拨号超时收到 1–2s，少坐这段空窗，要同时改 §7.1 里对齐 3s 的桶，不能只改拨号。
- 节点都是虚拟机。计划外掉线只在宿主机异常时发生，而且很少。同一宿主机上的一批节点会同时停止续租，key 在同一个 TTL 窗口里成批 DELETE。L1 的 Watch 和路由更新要扛住这一批事件：不能因为事件数量把发现循环卡住，也不能把同批里尚未过期的其他节点误删。
- 路由就是精确的 `node_id`。没有健康地址则快速失败。v1 不做多实例权重选择。

### 8.4 已确认的运行参数

拨号和巡检的时钟在 admin，registrar 不替它睡眠。下面这些都可以改，列出的是现在用的默认值。

| 事项 | 决定 | 哪里改 |
| --- | --- | --- |
| 一次拨号加一次出口校验 | 合计约 5s，失败率低于 1%。规划上按拨号 4s、校验 1s。计划内重拨后外部出口必须变化，否则不注册。状态文件缺失或损坏时按 drained 处理：`Report` 不能注册，必须先 `Ready`。文件缺失时，`Ready` 不传 `prevEgress` 仍会注册；调用方如果还知道旧出口，传进来才能挡住 | `AssumedDial`、`AssumedEgressCheck`、`AssumedRedialFailureRate`。`Ready` 的第二个参数 |
| 巡检 | 每 1 分钟。检查访问外部是否正常，以及 CPU、内存是否正常。默认两类失败都摘掉 key，恢复后再写回。`Report` 的零值是健康 | `InspectionInterval`。故障用 `NetFault`、`CPUFault`、`MemFault`。外网巡检不可靠时设 `IgnoreAdminNetCheck`。资源异常不想摘除时把 `ResourceFaultAction` 设为 `ignore` |
| 定期上报发往哪里 | 不外发，上报正文不写 etcd。L1 只读注册 value | `OnReport` 交给 admin 原有上报通道 |
| 计划外掉线 | 节点是虚拟机，只有宿主机异常时才发生，而且很少。同一宿主机上的一批节点会一起过期。不因此改 TTL | `TTL` 10s，`RenewInterval` 3s。L1 要承受批量 DELETE，见 §8.3 |
| nodeId | 环境变量 `RELAY_NODE_ID`，运维保证唯一。变量存在但只有空白是配置错误，不会改去读文件。事务冲突检测保留作兜底 | `NodeID` 可直接传入；为空且环境变量未设置时才读 `NodeIDFile`。告警走 `OnAlert` |
| admin 语言 | Go。主路径是进程内调用 `registrar`。`cmd/registrard` 只是备选，启动时不调用 `Ready` | `example/admin` 在 `Start` 之后调用 `Ready` |
| 计划内重拨的时间拉开 | 必须。调用 `JitterBeforeDrain`，默认在 Drain 之前随机等待 0–60s。同一时刻一起 Drain 会丢 lease。`example/admin` 的演示可以设 0。`registrard` 生产配置写成 0 会拒绝启动，只有 `-allow-zero-jitter` 才接受；`POST /v1/drain` 返回 202，不跟请求 context 一起取消 | `RedialJitter`，默认 `DefaultRedialJitter` |
| 同一台机器丢掉状态文件再启动 | 新 `instance_id` 对不上还活着的重拨标记。不报 `foreign_instance`，但标记过期前不能注册。最多等一个 EpochTTL，默认 30s | `EpochTTL` |
| 旧状态文件 | 没有 `machine_id` 或 `instance_id` 时保持 `drained`，不撤旧 lease，直到 `Ready`。旧 epoch 标记如果只有 `instance_id` 和 `node_id`、没有 `addr`，仍然算本实例；带了 `addr` 就必须和本机内网地址一致。文件整个丢失见上一行 | 无自动升级开关 |
| drained 期间的标记 | 持有者停在 drained 时继续续租标记，默认最多 10×EpochTTL（30s 时为 5 分钟）。到期停止续租，`OnAlert` 的 kind 为 `epoch_hold_expired`，标记再在一个 EpochTTL 内过期 | `MaxDrained` |

### 8.5 本仓库里怎么验证

`go test ./registrar/` 用内嵌 etcd 覆盖验收里的计划内重拨、重拨后出口 IP 没变时不注册、校验失败与崩溃恢复、未 `Drain` 的地址变化、gost 挂死与外网/资源巡检失败、切主时在线节点不删 key 且 `Drain` 在 3s 内完成、进程消失后的 TTL / 切主上限、nodeId 冲突，以及 etcd 单次操作延迟 1.5s 时续租间隔仍低于 TTL。另有：默认 `DrainAttempt` / `DrainBudget` 下，1.5s 的 etcd 延迟时持有者 `Drain` 只有一次事务，实测约 1.5s，低于 3s。leader 进程被杀掉（连接重置）时，验收 5 的 3s 仍然成立。leader 的客户端口和 peer 口被 iptables 丢包时，第一次尝试会坐满 `DrainAttempt` 再换成员；一次实测是 2.08s，仍低于 3s，但第二次如果也慢就会超过 3s。这条测试断言的是 6s 预算内删掉 key、业务 lease 不丢，不把 3s 当成丢包时的上限。压测入口是 `cmd/loadgen` 和 `scripts/loadtest.sh`。

启动错开是 `(staggerBase+i) % 8000ms`。单进程 `staggerBase` 为 0，公式就是 `i % 8000ms`。`hosts=4000` 时下标最大是 3999，所以实际错开是 0–3999ms，不是 8s。多进程时父进程把已经分出去的主机数当作 `staggerBase`，4000 台仍然铺在这 4s 里，而不是每个进程各自从 0 再铺一遍。loadgen 把公式和 `start_sleep_max_ms` 写进 summary。`-redial-inflight 0` 表示不限制同时进行的 `Drain`+`Ready`。

续租和 `Drain` 不再共用一把锁。锁拆开之前，单进程、内嵌 etcd、4000 路不限并发、同一时刻 `Drain`：`docs/acceptance/acceptance7-unbounded-before.log` 由 loadgen 在干净工作区上自己写入（commit `7c4cd62`），`drain_err` 4000，`lease_lost` 4000，`keepalive_ok` 1918，`keepalive_err` 247。同一形状在锁拆开之后（`acceptance7-unbounded-after.log`，commit `8d2b963`）：`lease_lost` 0，`keepalive_err` 0，`keepalive_ok` 26574（每台 min/p50/max 6/7/7），`drain_ok` 3968，`drain_err` 32。这 32 次当时没有分开记：`drain_err` 把 `Drain` 失败和 `Drain` 成功之后的 `Ready` 失败加在了一起，无法再拆。租约没有丢。loadgen 现在分开写 `drain_fail` 和 `redial_ready_fail`，`drain_err` 仍是两者之和。同形状、拆开计数的运行是 `docs/acceptance/acceptance7-unbounded-split.log`（loadgen 在 commit `82650a7` 的干净工作区上自己写入）：`drain_ok` 4000，`drain_fail` 0，`redial_ready_fail` 0，`drain_err` 0，`lease_lost` 0，`keepalive_err` 0，`keepalive_ok` 25809（每台 min/p50/max 6/6/7），`watch_del` 4000，`watch_put` 8000。同一次提交上还有一次当时没入库的运行，loadgen 自己写入的原文在 `docs/acceptance/acceptance7-unbounded-split-pre-fix.log`（头里的 commit 是 `82650a7`）：`drain_ok` 4000，`drain_fail` 0，`redial_ready_fail` 0，`drain_err` 0，`lease_lost` 824，`keepalive_err` 1970，样本 `rpc error: code = Unknown desc = not a primary lessor`，`keepalive_ok` 30235（min/p50/max 5/8/9），`watch_del` 4768，`watch_put` 8824。多出来的 768 次 DELETE 是注册真的从 etcd 消失了（计划内 Drain 只有 4000 次）。`lease_lost` 824 比这 768 多，是因为有的租约已经在本地被放弃、测试结束时 etcd 还没把它删完。

`not a primary lessor` 是 etcd `lease.ErrNotPrimary`。客户端能看到它，是因为 `EtcdServer.LeaseRenew` 在 `isLeader()` 为真但 `ensureLeadership()` 失败时直接把这个错误返回。`ensureLeadership` 在成员超过 3 个 tick 没有处理心跳时做一次线性读：读失败，或者读完 raft lead 已经不是自己，就失败。服务端注释写的是把错误交给客户端，让客户端改连其他成员再续。lessor 在自己不是 primary、或者一条已经过期的租约正要撤销时被 demote 打断，也会返回同一个错误；`LeaseRenew` 对 lessor 的这份错误会转发给当前 leader，不会原样回到 gRPC。所以这次样本是「这个成员没能确认自己仍是 primary lessor」，包含领导权刚交出去或线性读失败，不是已经知道自己是 follower 却还在本地续租。这条错误不在 `toGRPCErrorMap` 里，gRPC 状态码是 `Unknown`。`clientv3.KeepAliveOnce` 只对 `Unavailable` 和 `Internal` 在调用内部重试，`Unknown` 立刻返回。`leader changed` 映射成 `Unavailable`，会在单次超时里被客户端自己重试；这次日志里的样本不是它。

错误本身不是 `lease not found`，租约当时还在。旧续租循环在本地记下的 TTL 截止时间（上次成功加 10s）之前把这次失败记成 `ka_err`，200ms 后再试。4000 路以这个间隔打在 `ensureLeadership` 已经失败的成员上，会让领导权检查更难成功。截止时间一过，任何错误（包括这一条）都走 `loseLeaseLocked`：清掉业务 lease，停止 KeepAlive。停止之后 etcd 才按剩余 TTL 把 key 删掉，所以上面多出来的 DELETE 是这个循环造成的过期，不是 etcd 先把租约删了再回报这个错误。

修复在续租循环里：`not a primary lessor` 和 `leader changed` 在本地截止时间之前按 0.5–1.5s 抖动退避，只记 `ka_err`，不判丢失。过了截止时间先 `TimeToLive`。TTL 仍大于 0 就按剩余时间拨回本地截止时间并继续续。`lease not found` 或 TTL≤0 才 `lease_lost`。核对请求自己也是 not-primary 或超时，则不判丢失，退避后再试。`TestNotPrimaryPastDeadlineKeepsLiveLease`、`TestNotPrimaryPastDeadlineLosesRevokedLease`、`TestNotPrimaryProbeFailureDoesNotLoseLease` 覆盖这三条。修复提交 `f1e0753` 上同一形状再跑了三次，loadgen 在干净工作区自己写入，三次都入库：`acceptance7-unbounded-split-1.log`、`acceptance7-unbounded-split-2.log`、`acceptance7-unbounded-split-3.log`。三次都是 `lease_lost` 0，`drain_fail` 0，`redial_ready_fail` 0，`drain_ok` 4000，`watch_del` 4000。第 2 次 `keepalive_err` 100，样本是 `context deadline exceeded`（单次续租超时，发生在本地截止时间之前，所以只退避重试）；`keepalive_ok` 62577（min/p50/max 15/16/17），`watch_del` 仍是 4000，这 100 次超时没有让注册过期。第 1 次 `keepalive_err` 0，`keepalive_ok` 25754（6/6/7）。第 3 次 `keepalive_err` 0，`keepalive_ok` 23909（5/6/7）。

验收 7（`acceptance7.log`，commit `0695611`）是 8 个 OS 进程、每个 500 个 registrar，对 **127.0.0.1 上的 3 个 etcd 进程**（同一台机器，不是分在不同主机上的集群），`-redial-inflight 0`，重拨时刻对齐。合计 `registered` 4000，`ready_err` 0，`drain_ok` 4000，`drain_err` 0，`lease_lost` 0，`keepalive_err` 0，`keepalive_ok` 36522，`watch_del` 4000，`watch_put` 8000。日志里的 `kill_leader` 写在全部 `redial` 事件之后：leader 是在所有 `Drain` 已经结束之后才停的，不是重拨进行中。`leader_killed` true 只表示这次运行后来停过 leader。每个进程都 Watch 整个 `/relay/l2/` 前缀，汇总时取最大的那一份，不把次数乘上进程数。日志头里的 commit、`git status`、Go 版本、开始和结束时间都是运行时写的，不要手改。

Watch DELETE 次数等于这次 `Drain` 的次数；多出来的 DELETE 才是 lease 过期。续租次数来自 registrar 自己的续租循环，不会正好是主机数乘一个固定次数。loadgen 默认同时只进行 64 路 `Drain`+`Ready`，那只是可选的压测调度，不是 registrar 的限制。最近一次验收 5、验收 7 的输出在 `docs/acceptance/`。

---

## 9. 部署方案（初版）

### 9.1 环境拓扑

- K8s（推荐）或 VM。
- 一级代理：Deployment（无状态，多副本）。
- 二级代理：2000–4000 台虚拟机，分布在若干宿主机上。每台虚拟机一个 gost v3 systemd 服务，监听内网地址。admin 是 Go 进程，在进程内调用 §8 的 registrar，保留原有重拨 API 和每分钟巡检。同一宿主机异常时，上面这批节点的 key 会一起过期。
- etcd：3 节点，走内网，与拨号出口分开。registrar 和 L1 的客户端都配置全部成员。
- 监控：Prometheus 抓取 L1 管理地址上的 `/metrics`（M1 应用层指标）。M2 另外部署 eBPF Agent。Grafana + Loki（可选）。

### 9.2 发布策略

1. 先部署二级节点并注册 etcd。
2. 发布一级代理灰度（5% 流量）。
3. 对比关键 SLI（成功率/时延/错误码，取自 M1 应用层指标），并做一次 ClientHello 字节比对。
4. 全量切换。

### 9.3 容灾策略

- 一级跨可用区部署 + LB 健康检查。
- 二级按地域/可用区分组；node_id 命名包含 AZ。
- etcd 多副本 + 定期快照备份。
- 宿主机故障会让其上的 L2 虚拟机成批失去续租。L1 按 §8.3 承受这一批 DELETE，不把发现循环停在事件洪峰上。

---

## 10. Go 使用示例（一级代理核心逻辑）

下面是 L1 处理 CONNECT 的数据面，共享口令校验就在这个函数里完成。口令先做 SHA-256，再用 `subtle.ConstantTimeCompare` 与全系统共享口令比较；为空或不匹配时直接回 `407`，不拨 L2。用户名以最后一个 `@` 切分，`node_id` 是它后面的那一段，因此用户名本身可以含 `@`。目标不是 `host:port`，或切不出 `node_id`，回 `400`。通过后再向 L2 发第二次 `CONNECT`，`Proxy-Authorization` 使用客户端提交的同一组用户名和密码。读取 L2 应答的默认上限是 10 秒（`l2ResponseTimeout`，部署时可改）。gost 要先拨通源站才回 `200`，2 秒会把 2.5 秒才拨通的源站误判成 `502`。读完无论成败都清掉这个截止时间，隧道阶段不再受它限制。客户端若在等待期间先断开，L1 关掉这条上游连接，读立刻返回，不必坐满 10 秒。L2 返回 `407` 时，连同 `Proxy-Authenticate` 交给客户端；解析节点、拨号、向 L2 写出 `CONNECT`、设置或清除读截止时间、读应答失败，以及除 `200`/`407` 以外的状态，先写 `502` 再返回错误（见 §5.1）。确认 `200` 后才应答客户端。之后两个方向各自拷贝：`io.Copy` 返回非 nil 错误时立刻完全关闭两端，另一方向不会继续堵着；干净 EOF 只对对端 `CloseWrite`。反方向接着用空闲读超时（`halfCloseIdleTimeout`，默认 30s，可改）：每次成功读到字节都把截止时间重新拨到「现在 + 30s」，不是从 `CloseWrite` 那一刻起只拨一次。间隔只要短于这个值，响应可以很慢，也可以一直分片；连续这段时间没有字节才放弃。客户端先 FIN 和上游先 FIN 走同一套。整条隧道另有兜底（`tunnelIdleTimeout`，默认 5 分钟，可改）：任一方向读到字节都重新计时，两边都安静这么久才关闭。gost 的 `idleTimeout` 默认是 0，这条兜底不能省。两个方向连续 5 分钟都没有字节的连接会被关掉，例如长时间静默的 WebSocket，或挂起超过 5 分钟才返回的长轮询；这类应用要自己发心跳（WebSocket ping/pong 或应用层 keepalive），间隔短于 `tunnelIdleTimeout`，TCP keepalive 不带载荷，不会重置这个计时。两个方向都结束后再由 defer 完全关闭。出口网段拒绝在 L2 解析之后做，不在本函数里。示例不包含 etcd。打点使用 §7.1 导出的函数，按包名 `l1metrics` 调用：本地 `400` / `407` 走 `RejectClient`；对客户端的 `502`，以及原样交还的 L2 `407`，走 `Fail502`；L2 返回 `200` 后先 `ObserveConnect`，向客户端写出 `200` 成功后再 `EnterTunnel`。`upstreamConn.Write` 失败和 `SetReadDeadline` 失败都回 `502`，`reason` 分别是 `l2_write`、`l2_deadline`。涉及 `bufio`、`crypto/sha256`、`crypto/subtle`、`encoding/base64`、`errors`、`fmt`、`io`、`net`、`net/http`、`strconv`、`strings`、`sync`、`sync/atomic`、`time`。

```go
// ErrUnknownNode 表示本地路由表从未收录该 node_id。
var ErrUnknownNode = errors.New("unknown node")

// ErrNoHealthyAddr 表示该 node_id 已在路由表里，但当前没有健康地址。
// 只有这个错误保留原来的 node_id 标签。
var ErrNoHealthyAddr = errors.New("no healthy address")

// Resolver 按 node_id 给出一个健康的 L2 地址。
// 成功必须返回非空地址。返回 ("", nil) 按解析失败处理：502，node_id="unknown"，reason 为 no_upstream。
// 从未收录返回 ErrUnknownNode；已收录但没有健康地址返回 ErrNoHealthyAddr。
// 调用方只在 errors.Is(err, ErrNoHealthyAddr) 时把原来的 node_id 写入标签。
// 其他错误，包括 ErrUnknownNode 和任何未预期的错误，都记 node_id="unknown"。
// 这两种失败的 reason 都是 no_upstream。
// 经 errors.Join 同时带上 ErrNoHealthyAddr 的错误也会留下原来的 ID，
// 例如 errors.Join(ErrUnknownNode, ErrNoHealthyAddr)。Resolver 不得把这两个哨兵 Join 在一起。
type Resolver interface {
    Resolve(nodeID string) (addr string, err error)
}

// sharedSecret 是全系统那一份共享口令，与客户端登录 L1 以及 L2 的 auth.password 相同。
// 生产环境从配置注入；示例写成常量。
var sharedSecret = "s3cret"

// l2ResponseTimeout 是等待 L2 CONNECT 状态行的上限，部署时可改这个变量。
// gost 在拨通源站之后才回 200。2s 会把 2.5s 才拨通的源站写成 502。
// 默认 10s，覆盖一次较慢的源站拨号。读完应答后必须清掉，隧道拷贝不再受它限制。
var l2ResponseTimeout = 10 * time.Second

// halfCloseIdleTimeout 是半关闭之后，反方向连续没有读到字节的上限，部署时可改。
// 每次成功 Read 之前都把截止时间重新拨到现在 + 该值，所以是空闲超时，不是从 CloseWrite 起的一次性截止。
// 客户端先 FIN 与上游先 FIN 用的是同一套。默认 30s。
var halfCloseIdleTimeout = 30 * time.Second

// tunnelIdleTimeout 是整条隧道的空闲兜底，部署时可改。任一方向读到字节都重新计时。
// gost 的 idleTimeout 默认是 0（不限制），L1 不能靠它把两边都安静的隧道收掉。默认 5 分钟。
var tunnelIdleTimeout = 5 * time.Minute

// dialUpstream 拨向选定 L2 的 HTTP 监听地址。默认 TCP，超时 3s。
var dialUpstream = func(addr string) (net.Conn, error) {
    return net.DialTimeout("tcp", addr, 3*time.Second)
}

// username 格式示例: appuser@node-az01-003。
// node_id 取最后一个 '@' 之后的部分，用户名本身可以含 '@'（例如 user@corp@node1 → node1）。
func parseNodeID(username string) (user string, nodeID string, err error) {
    i := strings.LastIndex(username, "@")
    if i <= 0 || i == len(username)-1 {
        return "", "", fmt.Errorf("invalid username format")
    }
    user, nodeID = username[:i], username[i+1:]
    if strings.ContainsAny(nodeID, " \r\n/") {
        return "", "", fmt.Errorf("invalid username format")
    }
    return user, nodeID, nil
}

// sharedPasswordOK 先哈希再比较，避免口令长度不同时 ConstantTimeCompare 直接返回。
func sharedPasswordOK(password string) bool {
    got := sha256.Sum256([]byte(password))
    want := sha256.Sum256([]byte(sharedSecret))
    return subtle.ConstantTimeCompare(got[:], want[:]) == 1
}

// target 为客户端 CONNECT 的 authority，形如 host:port；IPv6 字面量保留方括号。
// password 与客户端登录 L1 的密码是同一份共享口令，不轮换。
func handleConnect(clientConn net.Conn, username, password, target string, resolver Resolver) error {
    defer clientConn.Close()

    // 隧道字节还没开始。400 / 407 / 502 都在这里写回客户端（见 §5.1、§5.2）。
    reply := func(status int, reason, header string, err error) error {
        msg := fmt.Sprintf("HTTP/1.1 %d %s\r\n", status, reason)
        if header != "" {
            msg += header + "\r\n"
        }
        msg += "\r\n"
        _, _ = io.WriteString(clientConn, msg)
        return err
    }
    badGateway := func(err error) error {
        return reply(http.StatusBadGateway, "Bad Gateway", "", err)
    }

    if strings.ContainsAny(username, "\r\n") || strings.ContainsAny(password, "\r\n") {
        l1metrics.RejectClient(l1metrics.ReasonRejectBadCredential)
        return reply(http.StatusBadRequest, "Bad Request", "", fmt.Errorf("invalid credential"))
    }
    if password == "" || !sharedPasswordOK(password) {
        l1metrics.RejectClient(l1metrics.ReasonRejectAuth)
        return reply(http.StatusProxyAuthRequired, "Proxy Authentication Required",
            `Proxy-Authenticate: Basic realm="relay"`, fmt.Errorf("invalid credential"))
    }

    _, nodeID, err := parseNodeID(username)
    if err != nil {
        l1metrics.RejectClient(l1metrics.ReasonRejectNoNode)
        return reply(http.StatusBadRequest, "Bad Request", "", err)
    }

    host, port, err := net.SplitHostPort(target)
    if err != nil || host == "" || strings.ContainsAny(host, " \r\n/") {
        l1metrics.RejectClient(l1metrics.ReasonRejectBadTarget)
        return reply(http.StatusBadRequest, "Bad Request", "", fmt.Errorf("invalid connect target"))
    }
    if _, err := strconv.Atoi(port); err != nil {
        l1metrics.RejectClient(l1metrics.ReasonRejectBadTarget)
        return reply(http.StatusBadRequest, "Bad Request", "", fmt.Errorf("invalid connect target"))
    }

    upstreamAddr, err := resolver.Resolve(nodeID)
    if err != nil || upstreamAddr == "" {
        inTable := errors.Is(err, ErrNoHealthyAddr)
        l1metrics.Fail502(l1metrics.NodeLabel(inTable, nodeID), l1metrics.ReasonNoUpstream)
        if err == nil {
            err = errors.New("empty address")
        }
        return badGateway(fmt.Errorf("resolve node %s failed: %w", nodeID, err))
    }

    started := time.Now()
    upstreamConn, err := dialUpstream(upstreamAddr)
    if err != nil {
        l1metrics.Fail502(nodeID, l1metrics.ReasonDial)
        return badGateway(fmt.Errorf("connect l2 failed: %w", err))
    }
    defer upstreamConn.Close()

    token := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
    req := "CONNECT " + target + " HTTP/1.1\r\n" +
        "Host: " + target + "\r\n" +
        "Proxy-Authorization: Basic " + token + "\r\n\r\n"
    if _, err := upstreamConn.Write([]byte(req)); err != nil {
        l1metrics.Fail502(nodeID, l1metrics.ReasonL2Write)
        return badGateway(err)
    }

    // 此后只有 clientPump 经 clientR 读 clientConn，隧道从 clientPipe 读，避免两个读端抢数据。
    // 等待 L2 时客户端先断开，则关掉 upstream，让状态行读取立刻返回。
    // 半关闭之后 idleConn 每次 Read 都重新拨空闲截止时间；读到字节则拨回整条隧道的兜底计时。
    idle := &tunnelIdle{client: clientConn, upstream: upstreamConn}
    defer idle.stop()
    clientR := &idleConn{Conn: clientConn, onBytes: idle.touch}
    upstreamR := &idleConn{Conn: upstreamConn, onBytes: idle.touch}
    clientPipe, clientPumpW := io.Pipe()
    defer clientPipe.Close()
    clientGone := make(chan struct{})
    go func() {
        _, copyErr := io.Copy(clientPumpW, clientR)
        _ = clientPumpW.CloseWithError(copyErr)
        close(clientGone)
    }()

    type l2Status struct {
        code              int
        proxyAuthenticate string
        br                *bufio.Reader
        err               error
        deadline          bool
    }
    statusc := make(chan l2Status, 1)
    go func() {
        var st l2Status
        if st.err = upstreamConn.SetReadDeadline(time.Now().Add(l2ResponseTimeout)); st.err != nil {
            st.deadline = true
            statusc <- st
            return
        }
        st.br = bufio.NewReader(upstreamR)
        st.code, st.proxyAuthenticate, st.err = readConnectStatus(st.br)
        if clearErr := upstreamConn.SetReadDeadline(time.Time{}); clearErr != nil && st.err == nil {
            st.err = clearErr
            st.deadline = true
        }
        statusc <- st
    }()
    var st l2Status
    select {
    case st = <-statusc:
    case <-clientGone:
        upstreamConn.Close()
        st = <-statusc
        if st.err == nil {
            st.err = fmt.Errorf("client closed during l2 handshake")
        }
    }
    if st.err != nil {
        if st.deadline {
            l1metrics.Fail502(nodeID, l1metrics.ReasonL2Deadline)
        } else {
            l1metrics.Fail502(nodeID, l1metrics.ReasonL2Response)
        }
        return badGateway(st.err)
    }
    code, proxyAuthenticate, br := st.code, st.proxyAuthenticate, st.br
    if code == http.StatusProxyAuthRequired {
        challenge := proxyAuthenticate
        if challenge == "" || strings.ContainsAny(challenge, "\r\n") {
            challenge = `Basic realm="relay"`
        }
        l1metrics.Fail502(nodeID, l1metrics.ReasonL2Auth)
        return reply(http.StatusProxyAuthRequired, "Proxy Authentication Required",
            "Proxy-Authenticate: "+challenge, fmt.Errorf("l2 connect status 407"))
    }
    if code != http.StatusOK {
        l1metrics.Fail502(nodeID, l1metrics.ReasonL2Status)
        return badGateway(fmt.Errorf("l2 connect status %d", code))
    }

    l1metrics.ObserveConnect(nodeID, started)
    if _, err := io.WriteString(clientConn, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
        return err
    }
    defer l1metrics.EnterTunnel(nodeID)()

    // br 可能已经缓冲了与 200 粘在一起的后续字节，转发必须从 br 读，不能另起一次 net.Conn 读。
    // 从这里开始不再解析 HTTP，也不建立 TLS。io.Copy 会保持字节内容；是否换成 splice 不影响 JA3/JA4。
    // 读或写出错：完全关闭两端，另一方向的 io.Copy 会立刻返回。
    // 干净 EOF：只 CloseWrite。反方向若已经堵在 Read 里，先拨一次空闲截止时间把它唤醒；
    // 之后每次 Read 再拨（见 idleConn）。连续 halfCloseIdleTimeout 没有字节才算超时。
    idle.start()
    errc := make(chan error, 2)
    relay := func(dst net.Conn, src io.Reader, reverse *idleConn) {
        _, copyErr := io.Copy(dst, src)
        if copyErr != nil {
            clientConn.Close()
            upstreamConn.Close()
            errc <- copyErr
            return
        }
        if werr := closeWrite(dst); werr != nil {
            clientConn.Close()
            upstreamConn.Close()
            errc <- werr
            return
        }
        reverse.halfClosed.Store(true)
        _ = reverse.SetReadDeadline(time.Now().Add(halfCloseIdleTimeout))
        errc <- nil
    }
    // 客户端先 FIN：反方向是 upstreamR。上游先 FIN：反方向是 clientR。同一套空闲计时。
    go func() { relay(upstreamConn, clientPipe, upstreamR) }()
    go func() { relay(clientConn, br, clientR) }()
    err1 := <-errc
    err2 := <-errc
    if err1 != nil {
        return err1
    }
    return err2
}

// idleConn 在半关闭之后把读变成空闲超时：每次 Read 都先把截止时间拨到现在 + halfCloseIdleTimeout。
// 读到字节时触碰整条隧道的兜底计时。半关闭之前不改截止时间，握手阶段的读超时仍然有效。
type idleConn struct {
    net.Conn
    halfClosed atomic.Bool
    onBytes    func()
}

func (c *idleConn) Read(p []byte) (int, error) {
    if c.halfClosed.Load() {
        _ = c.Conn.SetReadDeadline(time.Now().Add(halfCloseIdleTimeout))
    }
    n, err := c.Conn.Read(p)
    if n > 0 && c.onBytes != nil {
        c.onBytes()
    }
    return n, err
}

// tunnelIdle 在任一方向读到字节时把关闭时刻推后 tunnelIdleTimeout。
// 到点仍没有新字节，就同时关掉客户端和上游。
type tunnelIdle struct {
    client, upstream net.Conn
    mu               sync.Mutex
    timer            *time.Timer
    stopped          bool
}

func (t *tunnelIdle) start() {
    t.mu.Lock()
    defer t.mu.Unlock()
    if t.stopped {
        return
    }
    t.timer = time.AfterFunc(tunnelIdleTimeout, t.fire)
}

func (t *tunnelIdle) touch() {
    t.mu.Lock()
    defer t.mu.Unlock()
    if t.timer == nil || t.stopped {
        return
    }
    t.timer.Reset(tunnelIdleTimeout)
}

func (t *tunnelIdle) fire() {
    t.mu.Lock()
    if t.stopped {
        t.mu.Unlock()
        return
    }
    t.stopped = true
    t.mu.Unlock()
    t.client.Close()
    t.upstream.Close()
}

func (t *tunnelIdle) stop() {
    t.mu.Lock()
    defer t.mu.Unlock()
    t.stopped = true
    if t.timer != nil {
        t.timer.Stop()
    }
}

// closeWrite 关闭写端，使对端读到 EOF，同时保留本端读端。不是 TCP 连接时退化为完全关闭。
func closeWrite(conn net.Conn) error {
    if cw, ok := conn.(interface{ CloseWrite() error }); ok {
        return cw.CloseWrite()
    }
    return conn.Close()
}

func readConnectStatus(r *bufio.Reader) (int, string, error) {
    line, err := r.ReadString('\n')
    if err != nil {
        return 0, "", err
    }
    parts := strings.SplitN(strings.TrimSpace(line), " ", 3)
    if len(parts) < 2 {
        return 0, "", fmt.Errorf("bad status line")
    }
    code, err := strconv.Atoi(parts[1])
    if err != nil {
        return 0, "", err
    }
    var proxyAuthenticate string
    for i := 0; i < 64; i++ {
        line, err = r.ReadString('\n')
        if err != nil {
            return 0, "", err
        }
        if line == "\r\n" || line == "\n" {
            return code, proxyAuthenticate, nil
        }
        key, val, ok := strings.Cut(strings.TrimRight(line, "\r\n"), ":")
        if ok && proxyAuthenticate == "" && strings.EqualFold(strings.TrimSpace(key), "Proxy-Authenticate") {
            proxyAuthenticate = strings.TrimSpace(val)
        }
    }
    return 0, "", fmt.Errorf("l2 response headers too long")
}
```

不要用会自行完成 TLS 的客户端来实现这一跳，例如对目标调用 `tls.Dial`，或让 `http.Transport` 在 `CONNECT` 之后继续做 TLS 握手。那会丢掉客户端的 ClientHello。`http.ReadResponse` 对没有 `Content-Length` 的 `200` 会把连接余下的字节当成响应体，因此这里用手写的状态行读取，把隧道字节留给后面的 `io.Copy`。

---

## 11. 里程碑建议

- **M1（1~2 周）**：PoC 跑通（L1 向 gost 发 `CONNECT`、etcd、username 指定路由、ClientHello 字节比对），并用 `prometheus/client_golang` 暴露 §7.1 的应用层指标（三项 SLI，另加 `proxy_client_reject_total`）。应用层告警见 §7.3。
- **M2（2~4 周）**：部署 eBPF Agent，采集 `kernel_tcp_retrans_total`、RTT、run queue latency 等内核指标，接入 Prometheus 看板与 §7.3 的内核告警。
- **M3（4~6 周）**：压测调优、故障演练、灰度发布。

---

## 12. 风险与对策

1. **风险：二级节点不可用导致路由失败**
   - 对策：重拨、探测失败、外网或 CPU/内存巡检失败时 Delete key；重拨后出口 IP 没变则不注册。进程消失时靠 lease TTL 摘除。同一宿主机上的一批虚拟机会一起过期。见 §8。
2. **风险：高并发下文件句柄与端口耗尽**
   - 对策：ulimit、`net.ipv4.ip_local_port_range`、按出口拆分 L2。单源地址到单 L2 的 HTTP/1.1 并发大约受临时端口数量限制；超出后再评估 Envoy 的 HTTP/2 `CONNECT` 多路复用。
3. **风险：TLS 指纹透传被链路设备或错误的 L2 配置破坏**
   - 对策：分层抓包比对；发布配置时拒绝 MITM、上游 TLS 与 sniffing。变更流程加回归用例。
4. **风险：L2 作为正向代理访问本机或云元数据地址**
   - 对策：L2 在拨号前拒绝 `127.0.0.0/8`、`::1`、`169.254.0.0/16`（含 `169.254.169.254`）和 `fe80::/10`。RFC1918 等私网本版放行，以后可以再收紧。比对的是解析后的地址，见附录 A.5。
5. **风险：共享口令从任一 L2 泄露后，可以用来登录 L1 和全部 L2**
   - 已接受：v1 就用这一份口令，不轮换，也不另设跳级口令。更换口令会同时更换客户端、L1 和全部 L2。
6. **风险：长时间静默的长连接会被 L1 的隧道空闲兜底关闭**
   - 对策：`tunnelIdleTimeout` 默认 5 分钟，可按部署调大。接入方对 WebSocket、长轮询等长连接做应用层心跳，间隔短于该值；TCP keepalive 不算数据，不能代替。见 §10。

---

## 13. 结论（预研阶段）

该方案在“可落地性、性能、可观测、可扩展”四个维度具备较高可行性：

- 一级采用 Go 二开，可快速承载定制路由逻辑，并以 HTTP `CONNECT` 把目标交给已选中的 L2。
- 二级采用 gost v3 的 HTTP 正向代理。默认路径就是隧道字节转发，能保住客户端 TLS 指纹；3proxy 与锁死配置的 Envoy DFP 是同一接口上的替换项。
- etcd 负责二级节点发现。观测分两层：M1 用 L1 应用层 Prometheus 指标覆盖建连、活跃隧道、上游失败和本地拒绝，M2 用 eBPF 补内核网络与系统指标。

建议进入 PoC 实施，优先验证三件事：
1. username 路由稳定性，以及 L2 `200` 之后才向客户端应答；
2. 客户端与目标侧 ClientHello / JA3 / JA4 一致；
3. 高并发下连接成功率与时延目标，用 §7.1 的三项 SLI 核对。

---

## 附录 A. L1→L2 目标传递与 L2 选型

v0.1 让 L1 与 L2 之间只建立一条裸 TCP，然后把客户端字节拷进去。L2 若是 HAProxy 或 Envoy 的 TCP 模式，它只能连到配置里写好的后端，看不到客户端 `CONNECT` 行里的 `target:port`。HAProxy 也不是动态正向代理：Lua `core.tcp()` 可以临时拨号，但那就变成了自研代理，不再是开箱的 L4 转发。

本附录比较三种目标传递方式，以及能真正按目标拨号的 L2。评价顺序是：能不能把客户端 TLS ClientHello 原样送到源站，然后才是正确性、性能、运维，以及和 `username` 选路的配合。

### A.1 TLS 指纹结论

JA3 是 ClientHello 里版本、密码套件、扩展、曲线和点格式按**线上顺序**拼出来的哈希。JA4 对同一份 ClientHello 做另一种规范化（密码套件和扩展会排序，并单独记下 ALPN 与是否有 SNI）。两边都只看握手内容，不看 TCP 窗口、TTL 或分段。

因此只要客户端在 `200` 之后写出的字节流，原样出现在 L2 到源站的 TCP 载荷里，JA3 和 JA4 就与客户端直接访问时一致。推荐路径满足这一点：L1 和 L2 都不终止、也不重新发起客户端这次 TLS。gost 默认的 `CONNECT` 转发、3proxy 默认的 `mapsocket`/`splice`、以及按 A.5 锁死的 Envoy，都是字节转发。

下面这些会换掉 ClientHello，指纹不再是客户端的：

- Envoy 集群配置 `envoy.transport_sockets.tls`。官方 DFP 示例就是这样连上游的，它会用 Envoy 自己的握手。拷贝该示例再拿来做隧道，会直接破坏指纹。
- 漏配 `connect_config`。此时 Envoy 把 `CONNECT` 请求本身代理到上游，而不是把后续载荷当原始 TCP。
- Envoy 的 compressor、buffer 等过滤器作用在隧道 body 上。
- gost 配置 `mitm.certFile` 与 `mitm.keyFile`（中间人，必然重新握手）。`sniffing: true` 会去看隧道开头的 TLS，本设计直接关闭，避免解析后再编码。
- 3proxy 的 `ssl_mitm` 或 `ssl_cli`。
- Squid 的 `ssl_bump` 动作 `bump`（`splice` 才是不解密的 TCP 隧道）。
- sing-box 的 uTLS、reality、vision 等主动构造 ClientHello 的出站。

两件容易和“端到端透传”混在一起的事：

- 源站看到的 TCP 握手是 L2 发起的。客户端的 TCP 指纹不会出现在源站。这不影响 JA3/JA4。
- QUIC / HTTP/3 不在 TCP `CONNECT` 里。本版不承诺 UDP 上的 JA4。

L1↔L2 若以后另加外层加密，加密的是这一跳的隧道，源站仍应收到原来的客户端 TLS 记录。外层加密的选型不在本文件。

### A.2 目标传递协议

| 方案 | 能否表达源站 host:port | 正确性 | 性能 | 运维与排障 | username 选路 | ClientHello |
| --- | --- | --- | --- | --- | --- | --- |
| 第二次 HTTP `CONNECT` | 能。请求目标就是 authority，`Host` 与之一致 | L2 在出口侧 DNS，TCP 成功后回 `200`；失败有状态码。L1 在此之后才应答客户端 | L1↔L2 一次往返，Basic 认证在同一组头部里。之后是字节转发 | `curl -x`、访问日志里的目标主机、标准 502/504 | 选路在 L1 拨号前完成。发往 L2 的是客户端刚刚提交的同一组用户名和密码；L2 只校验这份口令，不解析 `node_id` | `200` 之后不解析。是否保持指纹取决于 L2 是否原样转发 |
| SOCKS5 `CONNECT`（ATYP=0x03） | 能。域名最长 255 字节，另带端口，由 L2 解析 | 与 `CONNECT` 一样是远程解析的 TCP 隧道 | 无认证 2 次往返；RFC 1929 用户名密码再加 1 次。跨地域出口上这是可见的额外时延 | 要用 SOCKS 客户端才能复现这一跳 | 选路仍在 L1。跳级认证能做，但塞不进“先选节点”这个动作 | 成功应答后同样是字节转发 |
| PROXY protocol v1 | 不能 | 文本头只有双方 IP 和端口。目的地址是本跳接入地址，没有域名，也没有“请拨到该主机”的语义 | 不适用 | 头一旦被写进源站连接，TLS 直接失败 | 无认证，不参与选路 | 只有定制接收方先剥头再拨号才有可能保持 |
| PROXY protocol v2，含 TLV | 不能按规范表达拨号目标 | `PP2_TYPE_AUTHORITY`（0x02）是客户端在**这条连接**上给出的主机名（定义上常为 SNI），不含端口。L1 上的客户端连接是 HTTP，不是 TLS，此时还没有 SNI。自定义 TLV（0xE0–0xEF）可以私自塞 `host:port`，但接收方必须自己实现拨号 | 头部本身很短 | HAProxy `accept-proxy` 与 Envoy 的 proxy protocol listener filter 只恢复源地址、做 ACL/日志，不会向任意 authority 新开连接 | 无标准认证 | 剥头不完整时，头字节会成为源站看到的“ClientHello” |

选用第二次 HTTP `CONNECT`。它和客户端已经在用的语义相同，一次往返同时完成目标、端口和口令校验，gost、3proxy、Envoy、Squid 都能接。SOCKS5 是正确的备选，但往返更多，而且把 Envoy 排除在现成实现之外。PROXY protocol 解决的是反向代理丢失客户端源地址，不是正向代理的目的地址。

### A.3 L2 组件

共同前提：L1 已经选定实例。L2 校验同一份共享口令、按 `CONNECT` 拨号、原样转发。用户名里的 `node_id` 会随这份凭据到达 L2，L2 不用它选路。

| 组件 | 动态拨号 | 性能与连接规模 | 运维 | 共享口令 | TLS 字节 |
| --- | --- | --- | --- | --- | --- |
| **gost v3 HTTP**（v1 已确认） | 原生 `CONNECT`，出口侧 DNS | Go 运行时。万级连接在放宽 fd 之后可达到；每连接内存和 GC 高于 C。M1 为 HTTP/1.1，每条隧道一条 L1↔L2 TCP | 单二进制、短 YAML、自带 Basic 认证。有 metrics/observer。功能面很宽，配置要钉死 | 原生 username/password，配成客户端的同一组 | 默认就是隧道拷贝。MITM 与 sniffing 是显式开关，本版关闭 |
| **3proxy** `proxy` | 原生 `CONNECT`，`mapsocket` 双向转发 | C，Linux 上可用 `splice`。相同机器上连接密度和 CPU 通常更好，适合作为性能替换项 | 配置方言老、无内建 Prometheus，要靠日志或节点级 eBPF。行为稳定 | `auth strong` + `users` / `allow` | 默认不碰 TLS。`ssl_mitm` 与 `ssl_cli` 必须保持关闭 |
| **Envoy DFP** | 可以。路由匹配 `connect_matcher`，`upgrade_configs` 使用 `CONNECT` 且带 `connect_config: {}`，集群类型为 dynamic forward proxy，DNS 缓存两边一致 | 上限最高，热重启和统计最全。二进制和配置都更重 | 官方 DFP 页面的示例带上游 TLS，那是 Envoy 自己去握手。过滤器链也容易改写隧道 body。校验同一份 Basic 口令需要额外 filter 或 ext_authz | 能做，配置比 gost/3proxy 长 | 只有 A.5 的锁死配置才是字节转发。2020 年 #11197 起，终止 `CONNECT` 时在上游 TCP 建立后就回 `200`，不等源站先发数据，因此不会和“客户端等 200 再发 ClientHello”死锁。升级概述里“等上游第一个字节再合成 200”描述的是更早的行为 |
| **Squid** | 原生 `CONNECT`，默认把隧道当 TCP 铲数据 | 进程模型更重，不是本版的性能选项目标 | 文档成熟，配置体积大 | ACL 成熟 | 端口未开 `ssl-bump`，或动作为 `splice` 时字节保持。`bump` 会解密并重新握手 |
| **sing-box** `http` 入站 + `direct` 出站 | 能接 `CONNECT` 并直连 | Go，与 gost 同一量级 | 发布节奏快，但产品方向包含主动改指纹的传输 | `users` 列表 | 纯 `direct` 且关闭 sniff 时应当原样转发。uTLS / reality 一类出站会故意换 ClientHello，和本目标相反。社区也报告过 sniff 打开时直连 TLS 失败的案例 |
| **HAProxy TCP、Nginx stream** | 不能按请求动态拨号 | L4 性能很好，用错了地方 | 适合做 L1 前面的负载均衡，不适合做 L2 | 不承载这层选路 | 到固定后端时字节可以不变，但后端不是客户端指定的源站 |
| **microsocks / Dante** | 仅 SOCKS | 很小或足够 | 只在选择 SOCKS5 时有意义 | SOCKS 认证 | 默认是字节转发 |

gost 的取舍：正确的隧道是默认路径，HTTP `CONNECT` 又是 L1 的稳定接口。v1 已确定用 gost。3proxy 和锁死配置的 Envoy 留作以后替换：3proxy 更省资源，Envoy 的上限和可观测性更好，但要靠一份与官方 TLS 示例相反的配置才能保住指纹。

### A.4 与 username 路由的配合

1. 客户端对 L1：`CONNECT example.com:443`，`Proxy-Authorization` 的用户名是 `appuser@node-az01-003`，密码是全系统那一份共享口令。
2. L1 校验这份口令，取出 `node-az01-003`，从 etcd 缓存得到 `l2-az01-003.internal:8080`。
3. L1 对这个地址发 `CONNECT example.com:443`，`Proxy-Authorization` 仍是 `appuser@node-az01-003` 和同一份密码。
4. L2 校验同一组用户名和密码，解析 `example.com`。解析结果落在环回或链路本地时拒绝拨号；否则 TCP 连接成功后回 `200`。
5. L1 向客户端回 `200`，双向拷贝。客户端的 SNI 在 ClientHello 内部，L1 不读、不改。`CONNECT` 里的主机名按客户端原样交给 L2，L1 不先解析成 IP。

v1 接受这个认证取舍：L2 持有客户端登录 L1 的口令，全系统只有这一份，而且不轮换。任一出口泄露后，这把钥匙对 L1 和其他 L2 仍然有效，也不能按调用方单独吊销。更换时要同时改客户端、L1 和全部 L2。

出口侧 DNS 让源站看到的地址属于出口地域。L1 若先解析再把 IP 写进第二次 `CONNECT`，虚拟主机仍可能靠 SNI 工作，但会丢掉出口侧 DNS，也会让 L1 变成 DNS 出口。

### A.5 配置红线

gost（v1）：

- `listener.type` 为 `tcp`，`handler.type` 为 `http`。
- `auth.username` 等于客户端访问该节点的用户名，`auth.password` 等于那份共享口令。v1 每个节点只有这一个用户名（见 §3.2）。监听保持认证开启。
- `sniffing` 为 false；不设置 `mitm.certFile`、`mitm.keyFile`、`compression`。

3proxy（替换项，口令与上面相同）：

```
auth strong
users appuser@node-az01-003:CL:<shared-secret>
allow appuser@node-az01-003
proxy -p8080
```

不写 `ssl_mitm`、`ssl_cli`。需要数据过滤插件时也不要挂会改写字节的 filter；`splice` 只在没有这类 filter 时启用。

Envoy DFP（替换项）必须同时满足：

- HTTP 连接管理器允许 `CONNECT`，路由使用 `connect_matcher`。
- `upgrade_configs` 为 `upgrade_type: CONNECT` 且存在 `connect_config: {}`，用于在本地终止 `CONNECT`，把载荷当原始 TCP 送出。
- 集群为 `envoy.clusters.dynamic_forward_proxy`，与 HTTP 过滤器共用同一份 DNS cache；不配置 `transport_socket`。
- 升级过滤器链里只有路由器，不放 compressor / buffer。
- 用与客户端相同的 Basic 口令限制调用方。从这台机器能路由到的地址，调用方就可能尝试去连，因此下面的出口拒绝同样适用。

出口拒绝（当前决定，以后可以再收紧）：

- 拒绝：`127.0.0.0/8`、`::1`、`169.254.0.0/16`（含云元数据 `169.254.169.254`）、`fe80::/10`。
- 放行：RFC1918（`10.0.0.0/8`、`172.16.0.0/12`、`192.168.0.0/16`）以及其他私网地址。
- 判断发生在 L2 解析之后、TCP 拨号之前，对象是即将连接的 IP。IPv4-mapped IPv6（`::ffff:a.b.c.d`）先还原成 IPv4 再比。
- gost 服务级 `bypass` 黑名单能拒绝请求行里直接写出的这些 IP。它的 CIDR 规则不解析域名：`example.com` 解析到 `169.254.169.254` 时不会命中。v1 还要在解析结果上做同一次判断（例如 `gost-geo-plugin` 黑名单，或 L2 侧等价检查）。只贴上面的 `bypass` 名单不够。

客户端 `CONNECT` 缺少端口时直接拒绝（回 `400`，见 §5.1），L1 不替它补端口。

### A.6 什么时候换掉 gost

- 单机内存或 GC 先于带宽打满：换成 3proxy，L1 仍发 HTTP `CONNECT`。
- 单条 L1 源地址到单个 L2 的并发超过临时端口，或需要 Envoy 的热重启与内建指标：换成 A.5 的 Envoy 配置。HTTP/2 `CONNECT` 可以在一条连接上复用多条隧道，从而避开源端口上限。
- 两种替换都要先重跑 A.1 的 ClientHello 字节比对，再灰度。替换不改变共享口令，也不放宽 A.5 的出口拒绝范围。
