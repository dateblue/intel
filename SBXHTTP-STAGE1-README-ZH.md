# SB-XHTTP Stage 1 — 双端、Reality 必选、最小 HTTP/2（候选源码）

> **状态：源码已经接入 sing-box PQC Slim，尚未完成 Go 1.27.3 全量构建、Reality/PQC 真正握手和 KR/SG 实测。因此不是“断流已解决”的正式版。**
>
> 只要求 sing-box ↔ sing-box；**不兼容 Xray XHTTP**。在测试成功前严禁替换现在的 Reality+Vision 生产二进制。

## 设计中的减法

- VLESS + `sbxhttp` + Reality + PQC Hybrid：客户端、服务端均必须使用 Reality。普通 TLS 或裸 H2 不能通过 VLESS 构造器检查；Vision flow 和 sing-box mux 明确拒绝。
- HTTP/2 单逻辑流独占单 TCP/TLS 连接，**没有 XMUX 共享连接池**。解决的是多条 H2 Stream 同属一个 TCP 时的连带故障传播，而不是声称线路永远不会坏。
- 服务端显式调用 `tls.ServerHandshake`（由现有 sing-box Reality/PQC 完成），检查 h2 ALPN，再用 `http2.Server.ServeConn` 处理封装后的 TLS 连接。不能依赖 `net/http.Server` 把 Reality 封装连接自动识别为 `*tls.Conn`。
- 客户端仍通过 sing-box 的 `tls.NewDialer` 进行 Reality/uTLS/PQC 握手，h2 ALPN 必选。每条 VLESS 会话创建单独 `http2.Transport`，结束就取消请求、关闭响应体、底层 TLS TCP 和传输池。
- `io.Pipe` 提供有界背压；服务端 `ResponseController.Flush()` 显式返回错误。关闭操作不等待可能被 H2 流控阻塞的 Write 锁；不会因“长时间没收到数据”自动判定故障。
- 服务端每个底层 TCP 至多 1 条 H2 Stream；最多 512 个并发 socket，超额立即拒绝，无动态扩容算法。服务端接收流控窗口为 4MiB（每流与每连接）。
- 仅有两端都无需指定的稳定默认行为；客户端/服务端仅须 `transport.type` 与一致的 `path`。

## 重要边界

这不是“双端业务流无损迁移”协议。**若某条 TCP 已经死掉，该条 VLESS 业务流仍会失败**，需要应用建立新连接。此版本只排除了 XMUX 跨流连带故障和部分软件资源管理隐患；能否改善 KR/SG 的实际卡顿必须经 A/B 测试，不能提前宣称解决。

## 最小配置范例（占位符必须替换）

**服务端新增单独 VLESS 入站**（保持原 Reality+Vision 2053 不变；示例 2083 仅在端口空闲时使用）：

```json
{
  "type": "vless", "tag": "sb-xhttp-in", "listen": "::", "listen_port": 2083,
  "users": [{"name":"lab", "uuid":"REPLACE-WITH-OWN-UUID"}],
  "tls": {"enabled":true,"server_name":"www.amd.com",
    "reality":{"enabled":true,"handshake":{"server":"www.amd.com","server_port":443},
      "private_key":"REPLACE-WITH-REALITY-PRIVATE-KEY", "short_id":["REPLACE-WITH-SHORT-ID"]}},
  "transport": {"type":"sbxhttp","path":"/sbxhttp"}
}
```

**客户端新增 VLESS 出站**（请保留你当前成功运行的 Reality/PQC 字段；注意**不写** `flow`）：

```json
{
  "type":"vless", "tag":"SG-sbxhttp-test",
  "server":"sg.servehttp.com","server_port":2083,
  "uuid":"REPLACE-WITH-OWN-UUID",
  "tls":{"enabled":true,"server_name":"www.amd.com",
    "utls":{"enabled":true,"fingerprint":"chrome"},
    "reality":{"enabled":true,"public_key":"REPLACE-WITH-PUBLIC-KEY",
      "short_id":"REPLACE-WITH-SHORT-ID", "key_share":"hybrid"}},
  "transport":{"type":"sbxhttp","path":"/sbxhttp"}
}
```

这些只是结构例子；请与当前实际的 sing-box `tls.reality` 配置字段及端口使用情况核对，正式测试时用你自己的密钥。并未在本机验证整个配置 `sing-box check`。

## Windows Go 1.27.3 运行测试

```powershell
cd D:\dev\sing-box
$tags = 'with_quic,with_utls,with_clash_api,with_naive_outbound,with_musl,badlinkname,tfogo_checklinkname0'
go version
if ((go version) -notmatch 'go1\.27\.3') { throw '必须先使用 Go 1.27.3 验证' }
go test -tags $tags -ldflags='-checklinkname=0' ./transport/sbxhttp ./transport/v2ray ./protocol/vless ./option -count=1
if ($LASTEXITCODE -ne 0) { throw '单元/编译测试未通过，不得部署' }
New-Item -ItemType Directory -Force .\dist | Out-Null
go build -trimpath -tags $tags -ldflags='-s -w -checklinkname=0' -o .\dist\sing-box-sbxhttp-test.exe ./cmd/sing-box
if ($LASTEXITCODE -ne 0) { throw '编译失败，不得部署' }
.\dist\sing-box-sbxhttp-test.exe check -c .\YOUR-CONFIG.json
```

**额外重要验证**：Linux AMD64/ARM64 要跑 `go test -race ./transport/sbxhttp`（按构建标签），并用独立测试端口检查客户端→SB Reality/PQC 服务端：正常 4K 视频、持续 FAST、H2 大流量/慢读取、断线后资源回收、48–72h 运行；必须与同线路的 Reality+Vision 对照。此阶段还没有这些成绩。

## 旧版 ATE 不继承

没有 `tcp_stall_recovery`、Turbo、FlowScope、XMUX pool-evict、h2_read_idle_seconds 或随机调速逻辑，也没有 Xray XHTTP 模式、填充、分包上行和 XHTTP 接口兼容代码。

## 文件

- `transport/sbxhttp/client.go`：独立 Reality H2 客户端
- `transport/sbxhttp/server.go`：独立 Reality H2 服务端
- `transport/sbxhttp/conn.go`：双向 net.Conn 适配、关闭和背压
- `transport/sbxhttp/conn_test.go`：纯 Go 并发关闭、流数据和路径单元测试
- sing-box 核心只修改 `constant/v2ray.go`、`option/v2ray_transport.go`、`transport/v2ray/transport.go`、`protocol/vless/inbound.go`、`protocol/vless/outbound.go` 共 5 个文件。

## 当前已做的测试

- 以随附 2026-10-30 PQC Slim 源码快照为基线进行改动，没有重写 PQC/Reality 和 Naive/HY2/TUIC 等代码。
- 当前容器只有 Go 1.23.2 且无法连接 Go 模块服务，**无法验证 Go 1.27.3 / sing-box 的完整编译**。
- 把 `conn.go` 与 `conn_test.go` 放在无外部依赖的 Go1.23 单独模块执行 `go test -race -count=20`：通过。这验证了若干连接关闭并发规则，**不验证 Reality、PQC、HTTP/2 出入站整体**。
- 新增源码完成 `gofmt`、语法分析、差异检查。正式验收仍需上述 Go 1.27.3 测试。
