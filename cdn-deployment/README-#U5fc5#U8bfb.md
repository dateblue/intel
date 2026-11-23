# SB-XHTTP PQC Slim：CDN + ENC 集中修订候选稿

基线：用户在 2026-11-06 上传的 `SB-XHTTP-pqc-new.rar`。工作树未包含原包的 `.git`、`dist` 编译产物。此次只修改指定传输/配置适配与官方 sniff 回归文件，不覆盖 KR、SG 正在运行的程序。

## 本稿已完成的源码变更

- 原 `transport.type=sbxhttp` 且 **不填写** `transport.mode`：仍沿用旧的 Reality、私有 Vision、Replay-v2 路径。
- 新增 `transport:{"type":"sbxhttp","mode":"cdn","path":"/sbx-cdn"}`：TLS h2，**GET 下载流 + 多个有限的 POST 上传数据包**，不是旧的一条长时间双向 POST。上下行通过 128bit 随机会话 ID 配对；上行 POST 带递增序号，接收端按序重排，缓冲有上限；下行 32KiB 分帧，空帧心跳，避免长时间无下行数据被误认为流结束。每个逻辑 VLESS 会话独占 HTTP/2 Transport，关闭/网络变化时释放资源。
- CDN 模式 **强制 VLESS ENC**（不允许 `none`）、`flow` 为空、禁用私有 Vision、禁用 Replay-v2，避免安全层和恢复协议互相叠加。该 ENC 沿用当前仓库已有的 ML-KEM768 + X25519 / native 1rtt 私有双端实现。请勿将其宣称为已完成与 Xray-core **互通验证**。
- 从同版官方源码还原 sniff 相关 5 个文件（`option/inbound.go`、`option/tun.go`、`adapter/inbound.go`、`protocol/tun/inbound.go`、`route/route.go`），取消之前的 TUN `sniff_override_destination` 伪兼容字段和改变官方行为的代码。原有 WRT Fake-IP、TUN IPv4/IPv6、DNS、路由规则未纳入修改范围。现代版本 sniff 应当通过 `route.rules` 内的 `{"action":"sniff"}` 来使用；不使用 sniff 无须加规则。
- 新增 CDN framing、ID 以及乱序重排单元测试。保留其它私有 XHTTP 连接管理代码，不将没有复现证据的问题随意重写。

## 重要边界（正式发布前必须看）

**本稿是可审查的源码候选，尚不是已通过实机验收的生产版。** 当前执行环境仅有 Go 1.23.2、离线且缺少该仓库 Go Modules，而仓库 `go.mod` 要求 Go 1.25.5 或更高。已执行 `gofmt`、Go AST 语法检查、`scripts/verify_slim_profile.py`：通过。**完整 Go 编译、包级测试、端到端 ENC 握手、Cloudflare 边缘长连接、视频实测尚未通过验证，不能假称已经通过。** 完整测试入口见 `cdn-deployment/scripts/verify-and-build.ps1`。

虽参考了官方 Xray 的 `packet-up + stream-down` 思路，本方案仍是 sing-box ↔ sing-box **私有** `sbxhttp` 协议，不能直接拿 Xray XHTTP 客户端互联。

## KR / WRT CDN 配置

1. Cloudflare DNS：`kr.xxxw.eu.org` **橙云**。SSL/TLS 选 `Full (strict)`。保证支持 `HTTPS 8443`，Cloudflare **HTTP/2 to Origin** 打开。
2. KR 上创建标准 TLS 证书和私钥：`/etc/sing-box/certs/kr-cdn.crt`、`/etc/sing-box/certs/kr-cdn.key`。可以用 Cloudflare Origin CA 证书（签发给 `kr.xxxw.eu.org`），仅用于 Cloudflare 回源 TLS；要保护私钥。实际证书必须先准备好，不能单靠 JSON 开启 TLS。
3. KR 确保 `8443/TCP` 没有其它程序占用，OCI 安全列表和系统防火墙允许 Cloudflare 访问 8443/TCP。与已有 2083/TCP、2053/TCP、443/TCP 相互独立。
4. 在 Cloudflare 创建匹配 **域名 kr.xxxw.eu.org 且 URL Path 前缀 `/sbx-cdn/`** 的配置规则：**Response Body Buffering = None**，保证 GET 响应分块直传客户端。上行采用短 POST，无需无止境的请求体流式传输；请求缓存、缓存规则不要影响这个路径。该设置的可用性取决于 Zone 的代理能力。
5. Windows 把本目录 `cdn-deployment` 放到 Git 仓库根目录后，执行 `cdn-deployment/scripts/prepare-cdn-config.ps1`。它调用源码自带 `sb-vlessenc-keygen` 生成一对匹配密钥，写入 `cdn-deployment/generated/` 下的 KR/WRT JSON。**服务端 `decryption` 属于私钥，不要提交 GitHub。** UUID 已在两份模板中一致填写。
6. 按下面的验证命令先运行测试并编译。**全部通过后**，把生成的服务端 JSON 对象加入现有 KR `inbounds`；客户端对象加入 WRT `outbounds`，单独新建 `KR-SBXHTTP-CDN` 节点进行测试。不要替换原有 Reality 节点，不要改变默认路由。

## Windows 单次集中测试与编译

```powershell
cd D:\dev\sing-box
powershell -NoProfile -ExecutionPolicy Bypass -File .\cdn-deployment\scripts\verify-and-build.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\cdn-deployment\scripts\prepare-cdn-config.ps1
```

Windows 本地脚本只使用不含 Naive/cronet 的 build tags，先验证代码和编出 smoke-test 的 `dist/sing-box-cdn-smoke-windows-amd64.exe`、`dist/sing-box-cdn-smoke-linux-arm64`、`dist/sing-box-cdn-smoke-linux-amd64`。这些只是测试产物，**不要作为保留 Naive 的生产版本直接替换**。Naive 出站 CGO、Linux musl 正式发行二进制和签名 Android APK 统一使用现有 `.github/workflows/build-slim.yml` 在 GitHub Actions 远程编译。WRT 架构用 `uname -m` 核实。

## 部署前后验证

```sh
# KR: 先备份原 JSON，合并 CDN 入站后执行（路径按实际填写）
sing-box check -c /etc/sing-box/config.json
ss -lntp | grep ':8443'

# WRT: 合并新 CDN 出站但暂不设为默认，检查配置
sing-box check -c /etc/sing-box/config.json
```

上述 `check` 必须使用你**新编译的** sing-box 二进制路径，而不是系统里还在运行的旧二进制，否则它不认识 `mode:cdn`。验证 `check` 后再在独立试验环境使用；不要同时更换 KR/WRT 的生产二进制。联调需测试小流量 TLS+ENC 握手、同节点下载/上传、大流量 4K、长时间空闲、网络中断后的行为和 Cloudflare 限制。该 CDN 模式 **不承诺无缝恢复已死的 TCP 业务连接**。

## 回退

- 撤销配置中新增 `kr-sbxhttp-cdn-in` 和 `KR-SBXHTTP-CDN`，保留旧 2083 Reality 服务即可。
- 新模式只在指定 `transport.mode="cdn"` 时启用，不依赖改动 Caddy、BBRv3、Brutal、路由或其它现有协议。
- 本包源码包含从上传原始树生成的 `SB-XHTTP-CDN-ENC-sniff.patch`。回滚源码可使用 Git 分支或 `git apply -R`（需匹配同一基线）。
