# SB-XHTTP CDN Multi-IP v0.6 — 稳定主通道 + 辅助通道竞争增益（实验版）

基线：2026-11-24 v0.5 完整源码 + “按需通道与补传止损”小补丁。本版把小补丁完整合并，无需单独应用它。继续使用 v0.5 CDN resilient wire v2（`X-SBXHTTP-Multi: 2`），不改变 REALITY/直连路径。适用于私有 SB-XHTTP，并非 Xray XHTTP 协议。

## 核心设计

- **主通道保持稳定**：服务器从首次成功附着的健康 GET 中选定主通道。它不因某条辅通道速度超过自己而切换。只有主 GET 真正掉线时，从现存健康通道中提升新的主通道；新主通道也保持稳定。首次选择不是“六个 IP 全部做大文件测速后选绝对最快”，没有历史样本时无法预知真实最快速度。
- **辅通道可比主通道快**：分片调度权重与主辅角色无关，主要看客户端实际收到的各条下行字节数，服务端对速率做 EWMA。辅助通道若明显更快，可以承载大部分下行数据，但不夺取主通道身份。
- **谨慎探测，按需增加**：主 GET 启动后先用最多两个工作通道；第 3～6 个通道在累计交付达到 256KiB/512KiB/1MiB/2MiB 时逐步启用，健康场景下至少间隔约 2 秒，减少多通道建连突发。尚未证明吞吐的辅助通道有较小的试探窗口，不能无限占满共享 ACK 窗口。
- **实际接收反馈**：客户端在原有 ACK POST 加入 `X-SBXHTTP-Lane-RX`（格式如 `0:12345,1:67890`）来上报单业务会话中每条 GET 的已收到字节累计值；不另起统计连接。服务器以已观测的客户端到达速率、发送队列、未确认量、历史疑似停滞选线路；不再单纯依赖源站 H2 Flush 时间。
- **有价值才放量**：服务器限制每条线路尚未确认数据的额度；辅通道初次只承担有限分片，观察到正向接收数据后再提高额度。优秀辅通道可获得更高权重，反复阻塞会降权；所有线路中只剩一条时不会因降权而禁用它，并允许使用完整 3MiB 总窗口。
- **容错**：沿用 v0.5 的独立 GET、ACK、重组、缺失分片跨通道补传；本版补传改成累计 ACK 停滞约 900ms 且存在另一条健康通道才尝试，避免单通道时重复挤占同一 TCP。该机制可能仍有可感知的短暂等待，并不能保证零卡顿。

## 配置

服务端：

```json
"transport": {"type":"sbxhttp", "mode":"cdn", "path":"/sbx-cdn", "down_connections":6}
```

客户端：

```json
"server": "172.64.229.79",
"server_port": 8443,
"tls": {"enabled":true, "server_name":"kr.xxxw.eu.org", "alpn":["h2"]},
"transport": {
  "type":"sbxhttp", "mode":"cdn", "cdn_protocol":"h2",
  "path":"/sbx-cdn", "down_connections":6,
  "cdn_ips": ["172.64.229.97","172.64.229.28","172.64.229.194","172.64.229.30"]
}
```

`cdn_ips` 是候选 IP，不要求全部可用；`server` 仍作为附加后备入口。`down_connections` 是最大 GET 并发槽位而非必须凑满六条。保证 IP 均可通过同一个 SNI、端口和 CDN 路径访问目标。

## Windows 现有 `pqc-new` 应用 v0.6（只编译一次）

将 `SBXHTTP_CDN_MultiIP_v0.6_相对v0.5.patch` 下载至 `D:\桌面`：

```powershell
cd D:\dev\sing-box
git switch pqc-new
git status --short
$patch = 'D:\桌面\SBXHTTP_CDN_MultiIP_v0.6_相对v0.5.patch'
git apply --check $patch
if ($LASTEXITCODE -ne 0) { throw '补丁不匹配，请先排查未提交的工作区修改' }
git apply $patch
git -c core.autocrlf=input add -- transport/sbxhttp/multitcp.go transport/sbxhttp/multitcp_resilient.go transport/sbxhttp/multitcp_adaptive.go transport/sbxhttp/multitcp_adaptive_test.go transport/sbxhttp/multitcp_adaptive_integration_test.go SBXHTTP-v0.6-README.md
git diff --cached --check
git diff --cached --stat
git commit -m 'SB-XHTTP CDN Multi-IP v0.6 adaptive additive scheduler'
git push origin pqc-new
```

提醒：不要 `git add -A`，不要 `git reset --hard`；你的工作区之前存在无关的 CRLF 文件状态。若 v0.5 后已经提交过“止损”补丁，当前补丁可能有上下文冲突，应从这份说明中核对或发出报错，不要重复硬套。

## 验收建议

1. 先用 2 个候选 IP、`down_connections:2` 验证 v0.6 没有明显退步；再使用 6 候选 IP 和最大 6 通道测试。
2. 比较单条大文件业务流 60s 以上持续吞吐，而不是仅用 YouTube 的瞬时连接速度；观察服务端的 `primary lane=` 和 `primary failover` debug、客户端 `MultiTCP down 10s` 每条 lane 的实际接收 Mbps。
3. 制造一条、三条、五条 IP 故障，检查幸存会话是否按序继续；单通道保底与故障恢复仍须真实 Cloudflare 实测。
4. 如果多通道下持续吞吐没有超过双通道，仍以双通道作为生产基线。这不是保底带宽的数学保证：同一 Cloudflare PoP 可能共享瓶颈，反馈本身也有滞后与额外的 ACK 开销。

## 验证范围

已完成 Go 格式检查、Go 源码 AST 语法解析，以及独立调度器在本地 Go 1.23.2 下的 `go test -race -count=50`（单通道保底、主通道不被更快辅通道替换、较快辅通道主导调度、未验证辅通道额度、停滞降权）。新增的整包集成测试已提供但**尚未运行**。本地 Go 1.23.2 低于源码要求 Go 1.25.5，尚不能声称全工程编译成功或 Cloudflare 实网提速已经成立。
