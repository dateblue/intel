# SB-XHTTP CDN Multi-IP + MultiTCP v0.2 实验版（2026-11-24）

## 重要状态

在 SB-XHTTP MultiTCP v0.1（用户 11 月 23 日 sing-box 源码为基线）的基础上，**仅增加 CDN 客户端的 IP 池选择和相关校验**。现有 REALITY 直连的物理拨号、传输与默认行为不变。**本版本未完成全量 sing-box 编译、KR/SG 真实部署或 CF 带宽 A/B 测试**；不要代替已稳定工作的生产版本。

v0.1 的 MultiTCP 把**单条 VLESS 双向业务字节流**分到 N 条独立 TCP/TLS/H2 连接（每个 lane 的客户端都独立 `DialTLSContext` 并调用 `http2.Transport.NewClientConn(raw)`；不存在把单一 H2 的多个 Stream 当多 TCP 的情况）。v0.2 让每条 lane 可选择不同 Cloudflare Anycast IP，同时复用原有单会话分帧及重组机制：

- **上传方向（本地 → CF IP 池 → KR/SG VPS）**：客户端分包 POST，按 seq 分配给 lane；服务端按 seq 重组后交付 VLESS。每个 POST 使用对应 lane 自己的 HTTP/2 ClientConn。临时请求错误可重试其它 lane。
- **下载/回程（目标站 → VPS → CF IP 池 → 本地）**：VPS 把一条字节流切成 32 KiB 帧，按偏移/有界队列分配给各条独立的 GET 响应；客户端对各 lane 的帧做乱序缓存和按序交付。每个 GET 与指定 IP 对应的客户端 TCP 绑定。回程是在已经建立的 GET 请求上发回数据；VPS **不能**指定 CF 内部回程的边缘 IP 或源站连接路径。
- 一条下载文件的流量可以同时使用 2/4 条 CDN 通道，但实际加速需实测。多个 CF IP **不等于**多个独立机房，也不能保证绕开共享源站/账户/路由瓶颈。Cloudflare 是 Anycast，不可据此推定各 IP 有独立容量。
- 既有 `down_connections` 配置实际同时控制**上下行**物理通道数量，保留此字段是为了兼容 v0.1。`cdn_ips` 只设置在**客户端**，服务端不得设置。`cdn_ips` 不会改变源站、TLS SNI、HTTP Host 或 VLESS/会话身份。

## 客户端配置（改原有 CDN 出站的 transport，**示意 IP 必须替换**）

```json
"transport": {
  "type": "sbxhttp",
  "mode": "cdn",
  "cdn_protocol": "h2",
  "path": "/sbx-cdn",
  "down_connections": 4,
  "cdn_ips": ["CF_IP_A", "CF_IP_B", "CF_IP_C", "CF_IP_D"]
}
```

`CF_IP_A` 等仅为占位符，不是可用 IP。填入你自行测试确认能正常代理**同一个橙云域名**的真实 Cloudflare 边缘 IP（只填 IPv4 或 IPv6，不带端口）。原有 outbound 的 `server`、`server_port`、TLS `server_name` 必须保持原 CDN 配置（SNI 应为真实代理域名）；四个 IP 只是每条物理 TCP 的目的地址。

`cdn_ips` 数量可为 1～`down_connections`，各 IP 不重复；不足 lane 数时按顺序循环分配。例如 4 条 lane、2 个 IP：A/B/A/B。配 1 个 IP 可以作为**同 IP 多 TCP**的对照实验。不设置 `cdn_ips` 时保留 v0.1 原有拨号行为。IP 列表是静态的，不作自动测速、自动换 IP 或未知 IP 扫描。CDN MultiTCP 当前只接受 H2，不支持 H3/auto。

### 服务端

源站仅需要开启 MultiTCP 最大接入条数，**不要填写 `cdn_ips`**：

```json
"transport": {
  "type": "sbxhttp",
  "mode": "cdn",
  "path": "/sbx-cdn",
  "down_connections": 4
}
```

新协议双方都需使用兼容的 MultiTCP 版本。保留生产节点，另设测试端口/域名或测试入站，不要原地替换。客户端若不填写 `cdn_ips` 可以用于“同 IP 多 TCP”测试，填写多个才能验证“跨入口 IP”。

## 测试建议：区分单连接瓶颈与多入口收益

用同一个测试文件、同一 VPS、同一时段和同一 URL，跑以下 3 组：

1. `down_connections=1` + `cdn_ips=["IP_A"]`：单 TCP、单 IP 基线。
2. `down_connections=4` + `cdn_ips=["IP_A"]`：同 IP 四 TCP。
3. `down_connections=4` + `cdn_ips=["IP_A","IP_B","IP_C","IP_D"]`：四 IP 四 TCP。

除单文件持续下载，必须检查单文件持续上传与上/下行同时跑的情况；FAST、Speedtest 往往会自行开多业务连接，不足以证明**同一条 VLESS 业务流**被叠加。客户端 debug 日志应记录每条 lane 的 `remote` IP:port 和独立 `local` TCP 端口。CF 回源 H2 是否多 TCP 不受客户端控制。发现某个 IP 不响应 SNI/Host，整条会话会在初始化阶段失败（首版故意失败关闭，而非静默回退）。

## 编译与验证边界

改动涉及 `option/v2ray_transport.go`、`transport/sbxhttp/client.go`、`transport/sbxhttp/server.go`、`transport/sbxhttp/multitcp.go`；新增 `multitcp_ips.go` 和相关 IP 单测，原有 v0.1 MultiTCP 上下行分片模块未改。Go 1.23.2 离线环境完成标准库单测和带 mock 依赖的局部类型测试及 race 测试，**无法替代项目要求的 Go 1.25.5+ 全工程编译**，需要你本地 Windows Go 环境执行：

```powershell
cd D:\dev\sing-box
$tags='with_quic,with_utls,with_clash_api,badlinkname,tfogo_checklinkname0'
go version
go test -tags $tags '-ldflags=-checklinkname=0' -count=1 ./transport/sbxhttp ./option ./protocol/vless
if ($LASTEXITCODE -ne 0) { throw 'Go tests failed' }
$env:CGO_ENABLED='0'
$env:GOOS='windows'; $env:GOARCH='amd64'
go build -trimpath -buildvcs=false -tags $tags '-ldflags=-s -w -checklinkname=0' -o .\dist\sbxhttp-multiip-win.exe ./cmd/sing-box
if ($LASTEXITCODE -ne 0) { throw 'Windows build failed' }
$env:GOOS='linux'; $env:GOARCH='arm64'
go build -trimpath -buildvcs=false -tags $tags '-ldflags=-s -w -checklinkname=0' -o .\dist\sbxhttp-multiip-arm64 ./cmd/sing-box
if ($LASTEXITCODE -ne 0) { throw 'Linux arm64 build failed' }
Remove-Item Env:GOOS,Env:GOARCH,Env:CGO_ENABLED -ErrorAction SilentlyContinue
```

## 已知局限

- 不做真正无损 lane 断线恢复；任意 lane 失败可能导致整个逻辑会话断线（v0.1 已有边界）。
- 固定轮询分片与有限重排缓存：如果 IP 路径时延/速度差异过大，会出现队头阻塞，甚至 4 MiB 重排上限触发关闭，叠加不一定增速。
- 客户端开启多 lane 时建连、TLS、HTTP 开销增加；不保证 CDN 运营商/CF 的单 IP 限速是实际瓶颈。
- 不支持对任意服务器 IP 伪造 Cloudflare 域名；CF 必须真的能为该域名提供正常 TLS/HTTP 代理服务。
- `cdn_ips` 客户端固定配置，不包含动态路由探测、自动失效替换；不要在配置里填随机公网 IP。
