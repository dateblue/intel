# SB-XHTTP × sing-box 1.15.0-alpha.14 去重整合说明

## 基线

官方 sing-box 标签：`v1.15.0-alpha.14`（提交 `126ce6c21`，从用户上传仓库 `.git` 的该标签导出）。采用官方 **完整源码** 作为基础，而不是将旧分支大范围复制覆盖。

原始自研基线：用户上传的 `sing-box(2).zip`，分支 `pqc-new`，最后提交 `96c20d1d`，并包含其工作区未提交的自研修改。

## 什么是官方已有的

- 官方自带 VLESS（依赖 `github.com/sagernet/sing-vmess`）、Reality、Vision / `xtls-rprx-vision` 流处理，以及 uTLS / TLS 混合后量子密钥交换能力。
- 官方 alpha14 已带来的 HTTP/2/H3、QUIC v0.66.0、FakeIP、DNS、TUN、auto-redirect、基础依赖等更新，统一从官方树保留。
- **没有把旧版 Reality/uTLS 随机指纹改写或 `reality.key_share` 额外开关整体搬过来**，避免覆盖官方证书验证、ECH 错误处理以及与原生 PQC 重叠的实现。

## 我们仍需要保留的私有功能

- `transport/sbxhttp/`：独立于 Xray XHTTP 的私有 SB-XHTTP（直连 H2、Replay-v2、独立会话、CDN H2/H3 单通道、特殊 Vision 适配）。**不与 Xray XHTTP 协议兼容**。
- `protocol/vless/encryption/` 和 `cmd/sb-vlessenc-keygen/`：私有 VLESS ENC，是 VLESS 应用层加密，和官方 TLS/PQC **不是同一个协议层**。
- `protocol/vless/{inbound,outbound}.go` 中只保留私有传输入口、ENC 握手、配置约束、断网后的会话重置等必要增量；保留官方 KTLS 与域名解析器处理逻辑。入站 `References()` 已修正为 alpha14 API `([]string, []string)`。
- `common/tls/reality_client.go` 仅有两个针对私有 H2/Reality 的小兼容补充：uTLS ClientHello 的 ALPN 与所选 H2 一致，以及 `MlkemEcdhe` 的 Reality 授权密钥回退。后者是协议兼容补充，**不是第二套 PQC 算法**；保留新版官方证书验证及其他握手处理。
- 私有 CDN/H3 必要的 uTLS-to-`crypto/tls` 转换辅助代码；注意其设计不意味着 HTTP/3 使用了 uTLS 指纹。
- 精简注册及现有 GitHub Actions 构建流程；没有添加 XMUX 或多 TCP / 多 IP 聚合。

## 行为变化与风险

1. 移除私有 `reality.key_share` 配置字段，改用 alpha14 官方 `tls.curve_preferences`、`utls` 能力。**如果当前运行配置有 `reality.key_share`，部署前必须删除此字段**，否则可能无法通过配置解析。并非所有随机指纹必然带有混合 PQC 份额；若服务端有强制要求，应检查真实 ClientHello 与兼容性。
2. `vision_inner_tls` 是我们的自定义传输适配，不等同于 sing-box 官方 Vision 的重新实现。现有代码仍拒绝 `vision_inner_tls` + `reliable=true` 的组合，请不要改变两端现有配对配置。
3. CDN 模式必须两端使用相应 VLESS ENC 配置；模板中原有的 UUID 已替换成 `__VLESS_UUID__`，`prepare-cdn-config.ps1` 会为一对新配置生成同一个随机 UUID。不会变动你正在运行的节点；若已有部署，勿使用模板生成的新凭据覆盖线上配置。
4. 官方 alpha14 的通用 HTTP/2 共享会话池保留在 `transport/http/`；我们的私有 SB-XHTTP 暂未接入它。不能声称共享连接池收益已经体现在 SB-XHTTP。直连独立 H2 会话仍保留，防止改变 Replay-v2 和 Vision 行为。
5. 由于本执行环境只有 Go 1.23.2，**未能使用 Go 1.27 对整个工程做类型检查、编译和实网测试**。已通过 1,271 个 Go 文件的语法解析、`gofmt` 检查、私有传输注册和关键配置静态守卫。源码为**远程编译候选**，不是已通过构建的生产发行版。

## 远程编译流程

保留 Windows 项目目录 `D:\dev\sing-box\.git`，将压缩包的 `sing-box\` 其余源码覆盖到目录中。旧版代码移除时请不要删除 `.git`。

```powershell
cd D:\dev\sing-box
python scripts/verify_slim_profile.py
git -c core.autocrlf=input add -A
git diff --cached --check
git diff --cached --stat
git commit -m "Rebase private SB-XHTTP and ENC on official alpha14 cleanly"
git push origin pqc-new
```

现有 `.github/workflows/build-slim.yml` 会通过 GitHub Actions 使用 Go 1.27.3 编译 Windows、Linux、Android 目标。先审查 Actions 的首条编译错误；**不要直接替换 KR/SG/N1 的生产程序**。主要验证顺序：Linux amd64 编译与测试 → Reality/H2 + Vision、Replay-v2 → ENC 和 CDN H2/H3 → Windows/N1/ARM64 → 晚高峰吞吐及重连测试。

## 去重审核边界

官方树中除有限的私有适配路径以外的通用源码未被旧版覆盖。并未逐行证明所有运行场景已兼容，也未声称 alpha14 的所有新机制会自动作用于我们的独立 H2 实现。
