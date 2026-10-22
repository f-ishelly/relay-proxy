// Package registrar 是跑在 ADSL L2 主机上的 etcd 注册模块，嵌进已有的 admin 进程。
//
// 每台主机一个 gost（独立 systemd）和一个 lease。key 为 /relay/l2/{nodeId}。
// value.addr 是内网 IP:端口，不随重拨变化；egress_ip 只是信息。
// 重拨前 Drain 删除 key 并等 etcd 确认，lease 继续续租；出口校验通过后 Ready
// 用同一个 lease 再 Put。进程消失时不 Revoke，key 等 TTL 到期后消失。
//
// 对外入口是 Start、Drain、Ready、Report、Stop。后台每秒探测 gost，每 500ms
// 检查本机 ppp 地址。etcd 客户端会使用 Config.Endpoints 里的全部成员，生产环境
// 必须把集群每个成员都写进去。
package registrar
