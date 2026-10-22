// Package registrar 是嵌进 L2 admin 进程的 etcd 注册模块。admin 是 Go 程序，主路径是进程内调用。
//
// 每个节点是一台虚拟机，上面一个 gost（独立 systemd）和一个 lease。key 为 /relay/l2/{nodeId}，
// nodeId 来自环境变量 RELAY_NODE_ID，由运维保证唯一。空白的环境变量是配置错误。
// value.addr 是内网 IP:端口，不随重拨变化；egress_ip 只是信息。value.instance_id 标识这一次安装。
// 重拨前 Drain 删除 key 并等 etcd 确认，lease 继续续租。一次拨号加一次出口校验大约 5 秒；
// Ready 只在外部看到的出口 IP 与重拨前不同时才用同一个 lease Put。状态文件损坏时必须由调用方
// 确认上一次出口。进程消失时不 Revoke，key 等 TTL 到期后消失。同一宿主机上的一批虚拟机会一起过期。
//
// 对外入口是 Start、Drain、Ready、Report、Stop，计划内批量重拨还要先调用 JitterBeforeDrain。
// 业务 lease 和重拨标记的续租在独立的 goroutine 里，不跟 Drain、Ready、探测共用锁。
// 后台每秒探测 gost，每 500ms 检查本机 ppp 地址。
// Report 的零值表示健康；NetFault、CPUFault、MemFault 为真时删除 key。etcd 客户端会使用
// Config.Endpoints 里的全部成员，生产环境必须把集群每个成员都写进去。
package registrar
