# SB-XHTTP v0.7 — Single-CDN 稳定精简版

构建基线：用户自研 sing-box / `pqc-new` 的 SB-XHTTP v0.6 完整源码（非 Xray XHTTP 协议）。

## 为什么做这版

真实使用中，MultiTCP 2/4/6 通道均未证明比单通道有持续净吞吐增益，反而有卡顿、连接超时、重排和断流。v0.7 的原则是保留已实测好用的单通道 CDN 与原有 REALITY 直连，不为了“优化”改动稳定的数据路径。

## 实际改动

1. 完整移除 `transport/sbxhttp/multitcp*.go` 与其测试、会话协议、ACK/补传/调度、H2 多 IP 池及重试退避路径。多路分片传输不会再由配置激活。
2. 从 `client.go` / `server.go` 中删除 MultiTCP 会话字段、初始化、拨号分支、HTTP 请求分流与 Reset/Close 逻辑。
3. 对已废弃的 `down_connections` 和 `cdn_ips` 配置参数显式报错，避免配置仍写旧参数却静默退化成其他传输。它们暂留在 JSON 解析结构中，专用于检测旧配置。
4. 保持 `cdn.go` 单通道 `packet-up`（有限大小并发 POST，序号重排及有界重试）+ `stream-down`（单条长 GET、心跳、HTTP/2 流）行为不变；保留可选 H3 客户端模式及 H3 失败回退 H2。
5. 保留 REALITY/ Vision、Replay-v2、VLESS Encryption、QUIC/HY2、TUN 与已有编译标签。**不导入、不照搬 XMUX**；不修改系统 tcp_brutal 设置。
6. 补充已废弃选项的回归测试，校验无多通道配置可以通过、旧配置将被拒绝。

这是**精简与回归版**，不是承诺单通道吞吐提升的调速补丁。尤其没有擅自把 sing-box testing 或 Xray 的整套传输代码覆盖自研实现。

## 客户端 CDN 出站配置示例（只展示相关字段）

```json
{
  "type": "vless",
  "tag": "KR-cdn",
  "server": "172.64.229.79",
  "server_port": 8443,
  "tls": {
    "enabled": true,
    "server_name": "kr.xxxw.eu.org",
    "alpn": ["h2"]
  },
  "transport": {
    "type": "sbxhttp",
    "mode": "cdn",
    "cdn_protocol": "h2",
    "path": "/sbx-cdn"
  }
}
```

说明：省略了必须填入的原有 VLESS 凭据与加密参数。若采用单 IP 优选，则 `server` 填该 CF 入口 IP；`tls.server_name` 仍为绑定的 Cloudflare 域名。`server` 不是 VPS 出口 IP。

## 服务端传输配置

```json
"transport": {
  "type": "sbxhttp",
  "mode": "cdn",
  "path": "/sbx-cdn"
}
```

删除**客户端和服务端**旧有的 `down_connections`、`cdn_ips`。其他 REALITY 直连配置保持不动。

## Windows：将完整源码覆盖到旧仓库后只提交确有变更的路径

```powershell
cd D:\dev\sing-box
git switch pqc-new
git status --short
# 重要：直接解压覆盖旧仓库不会删除 v0.6 遗留文件。
# v0.7 已彻底删除所有 MultiTCP 源码及其专属测试，必须先清理旧文件：
Get-ChildItem -Path .\transport\sbxhttp -Filter 'multitcp*.go' -File |
    Remove-Item -Force
# 仅暂存本次单通道回归影响的内容
git -c core.autocrlf=input add -A -- transport/sbxhttp option/v2ray_transport.go SBXHTTP-v0.7-README.md
git diff --cached --ignore-space-at-eol --stat
git diff --cached --check
git commit -m "SB-XHTTP v0.7 Single-CDN cleanup"
git push origin pqc-new
```

注意：若使用 `git apply` 增量补丁，删除操作已由补丁执行，不需要另外运行上述手工删除命令。若直接使用完整 ZIP 覆盖旧仓库，则必须运行删除命令，以免旧 Go 文件残留造成重复定义或引用错误。

不要删除 `cdn_test.go`、`reliable_*_test.go`、`vision_*_test.go` 等仍在测试当前功能的文件：它们不会进入正式二进制，也有助于防止再次发生旧测试与功能不一致的问题。

如果需要连同删除旧 `SBXHTTP-MultiIP-README-ZH.md`、`SBXHTTP-MultiTCP-README-ZH.md`、`SBXHTTP-v0.4-README.md`、`SBXHTTP-v0.6-README.md`，可追加暂存这些被删除的文件。不要使用 `git add -A` 扫描整个仓库，以免将无关的 Windows CRLF 变更一并提交。

## 验收

1. 先由 GitHub Actions 使用项目 Go 1.25.5+ 运行 `go test ./transport/sbxhttp ./option ./protocol/vless`（按既有构建标签），确认没有编译错误。
2. 保留原服务端二进制以备恢复，先升级 KR/SG 服务端，再升级客户端。
3. 运行已知的单通道 CDN 配置，检查握手、网页、YouTube/FAST 持续吞吐及晚高峰稳定性；不要把瞬间峰值当作提升证据。
4. 以相同 CF IP、相同客户端与同一时间窗口，对比 v0.6 *不启用多通道* 和 v0.7。两者数据路径理论上相同，期望不退步，而不是凭删代码就更快。

## 与官方代码对照（2026-11-24 查询）

- SagerNet/sing-box：正式发布版 1.14.4，测试版 1.15.0-alpha.13；自研 fork 已含单独的 VLESS/REALITY/QUIC 修改，不能简单整库替换。
- XTLS/Xray-core：11 月公开版本 v26.11.9；XHTTP 支持上下行拆分和独立下载设置，继续强化 HTTP/2 服务端参数。SB-XHTTP 单通道本身已有独立 POST/GET 请求，不需要为了照搬 XMUX 额外引入多路重排。
- Cloudflare 边缘至最终客户端的 TCP 由 CF 发送端决定拥塞控制；无法通过 VPS 内核 tcp_brutal 设置替换 CF 的拥塞算法。

## 测试边界

开发环境没有项目所需的 Go 1.25.5 全套依赖及 CF 实网条件；源码静态检查与单文件回归测试不等于全工程、Windows/Linux/Android 编译已成功。请以 GitHub Actions 结果为准。
