# SB-XHTTP MultiTCP v0.1 实验版（2026-11-24）

## 核心结论

本实验版基于用户 2026-11-23 的 `SBXHTTP_CDN_H3_20261123_源码.zip`，**不移植 XMUX**，而是把同一条 VLESS 应用字节流拆分给 1/2/4 条真正独立的底层 TCP/TLS/HTTP2 物理连接。`http2.Transport.NewClientConn(raw)` 对每条 lane 使用单独调用 `DialTLSContext` 获得的 TCP；因此它不把 H2 的多个 Stream 误当成多个 TCP。每条 lane 独立的拥塞窗口有机会增加单业务流吞吐，但若限制来自整条线路、账号、CF 回源或 VPS 总带宽，仍无法突破。

- **REALITY 直连**：每 lane 一条 REALITY/TLS + H2 物理 TCP，独立 GET 下行和长连接 POST 流式上行。上下行均使用 64-bit 字节偏移和帧重组（32KiB 帧）；直连不采用 CDN 的逐包 HTTP POST 以避免 RTT/request 速率限制。
- **Cloudflare CDN**：每 lane 一条标准 TLS + H2 客户端到 CF 物理 TCP，下行每 lane 独立 GET，上行保持有限分包 POST（与现有 CDN v1 一致的思路）。CF→源站的物理连接仍由 CF 自行管理。
- **两端互通**：同一传输 `type=sbxhttp`、一致的 `mode` / `path`；服务端 `down_connections=4` 表示最多允许 4；客户端 `down_connections=1/2/4` 用于 A/B 对照。
- **安全边界**：这是一套新的私有 `SBXM1` 帧格式，不兼容 Xray 原版 XHTTP，也不兼容旧 SB-XHTTP 端点。新模式以 `X-SBXHTTP-Multi` 请求头及 `/multi/...` 路径识别，不会变更旧模式的线协议。
- **避免虚假无损承诺**：v0.1 单 lane 断开会结束整个逻辑会话；未实现 Packet-down 跨连接 ACK/重传、会话无损迁移、跨 VPS 聚合、自适应连接数量，也未整合 Replay-v2（配置中不可同时使用 `reliable:true`）。
- **协议约束**：`down_connections` 仅支持 0、1、2、4。0=保持旧传输不变；1=新协议单 TCP 基线；2、4=实验叠加。MultiTCP CDN **仅支持 H2**，不支持 H3/auto。源站使用 H2，要求 CDN 对 GET 响应及有限 POST 不做整流式缓冲。

## 修改的源码

- `option/v2ray_transport.go`：新配置字段。
- `transport/sbxhttp/client.go`：实例验证、新传输分支、重置/退出清理、调试日志。
- `transport/sbxhttp/server.go`：实验模式路由、H2 流并发上限、生命周期清理。
- `transport/sbxhttp/multitcp.go`：客户端物理 TCP/H2 连接、两种上行方式、服务端 lane 管理、上下行分片与会话生命周期。
- `transport/sbxhttp/multitcp_wire.go`：12 字节帧头、乱序重组和有界缓存，纯标准库。
- `transport/sbxhttp/multitcp_wire_test.go`、`multitcp_logic_test.go`：帧、乱序、分发、限额和竞态测试。

## 客户端配置示例（仅修改原有 transport 字段）

直连 REALITY（服务端与客户端保持你原有的 TLS/REALITY/UUID/ENC/Vision 配置；**千万不要从示例覆盖已有密钥配置**）：

```json
"transport": {
  "type": "sbxhttp",
  "path": "/sbxhttp",
  "down_connections": 2
}
```

CDN H2（SNI 保持 CF 代理域名，源站保留相同 `path`）：

```json
"transport": {
  "type": "sbxhttp",
  "mode": "cdn",
  "cdn_protocol": "h2",
  "path": "/sbx-cdn",
  "down_connections": 2
}
```

服务端把对应入站的 `down_connections` 设为 **4**（最大支持数量），客户端可按 1→2→4 依次测速。不要将现有生产入站原地改成新模式；建议另建测试入站。新协议双端都需要本源码编译的新二进制。

## 本地 Windows 构建（从完整源码的 sing-box 子目录）

在 Windows PowerShell 运行：

```powershell
cd D:\dev\sing-box
$tags = 'with_quic,with_utls,with_clash_api,badlinkname,tfogo_checklinkname0'
go version
python .\scripts\verify_slim_profile.py
if ($LASTEXITCODE -ne 0) { throw 'Slim profile check failed' }
go test -tags $tags '-ldflags=-checklinkname=0' -count=1 ./transport/sbxhttp ./option ./protocol/vless
if ($LASTEXITCODE -ne 0) { throw '测试未通过，禁止部署' }
New-Item -ItemType Directory -Force .\dist | Out-Null
$env:CGO_ENABLED='0'
$env:GOOS='windows';$env:GOARCH='amd64'
go build -trimpath -buildvcs=false -tags $tags '-ldflags=-s -w -checklinkname=0' -o .\dist\sing-box-multitcp-windows.exe ./cmd/sing-box
if ($LASTEXITCODE -ne 0) { throw 'Windows build failed' }
$env:GOOS='linux';$env:GOARCH='arm64'
go build -trimpath -buildvcs=false -tags $tags '-ldflags=-s -w -checklinkname=0' -o .\dist\sing-box-multitcp-linux-arm64 ./cmd/sing-box
if ($LASTEXITCODE -ne 0) { throw 'KR/SG arm64 build failed' }
Remove-Item Env:GOOS -ErrorAction SilentlyContinue
Remove-Item Env:GOARCH -ErrorAction SilentlyContinue
Remove-Item Env:CGO_ENABLED -ErrorAction SilentlyContinue
```

项目 go.mod 需要 Go 1.25.5+，本地实际以 Windows 已能编译旧版的 Go 环境为准。可复用已有 `cdn-deployment/scripts/verify-and-build.ps1` 的更完整测试集。

## 真实 AB 验收

每次只改 `down_connections=1/2/4`，固定同一 KR 或 SG、同一 IPv4/IPv6、同一客户端和服务器、同一测速服务器、同一时段。至少测试：

1. 单个 TCP 业务流的持续下载速率（比如单线程 HTTP 文件、iperf3 单连接），不能只用本身多连接的 FAST/Speedtest 结论证明单流聚合。
2. 持续上传、双向同时下载上传、30 分钟视频连接、多个并发业务连接。
3. 观察实时 TCP Socket 个数：每个 VLESS 业务连接应有对应的 1/2/4 条客户端→服务端/CF 独立 TCP（每条 H2 上会承载 GET + POST Stream），`debug` 日志有独立本地端口。
4. 记录平均速率、P95/P99 时延、丢包重传、4 lane 乱序缓存是否超限、CPU/内存。
5. 强制断开一条 lane，确认程序清理整个 session 并允许新业务连接重新建立；v0.1 **不保证已有业务不断线**。

## 已完成与未完成的验证

已完成：新增源码 gofmt；使用 Go 1.23.2 在隔离标准库协议测试中验证分帧、短写、异常长度、乱序重组、重复帧；用替代外部依赖的 Go 接口桩类型检查新增 MultiTCP 文件，并执行 `go test -race -count=30` 与 `go vet`（局部通过）。

**未完成**：本环境没有项目要求的 Go 1.25.5+，无法下载工具链和依赖，因而没有完成整个 sing-box 的 `go test/go build`、真实 REALITY/H2 双端握手、Cloudflare CDN 连通性、Windows/Android 客户端实测、KR/SG 真实带宽测速。请勿把该版本视作已通过生产验收。

## 重要局限

- 新增额外 TCP 握手、HTTP 请求和 32KiB 分帧调度，某些好线路可能比旧版单 TCP 更慢；多流竞争还可能增大 P99 延迟。
- 4MiB 的接收重排缓冲属于有界内存的实验参数；长时间不平衡可能触发关闭而非无限堆积。
- CDN 入口要求 H2；CF 按源 IP、账号、站点或回源的聚合限速不会因为新增 TCP 自动解除。
- CDN 模式仍通过固定私有请求头/URL 指示协议，**不应将实验架构等同于隐匿性已经得到验证**。
- 正式投产前，需要完整编译测试和真实环境数据，再确定下一轮是否增加 lane 自适应调度、下行 ACK/有限重传。
