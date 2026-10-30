# pqc-new / PQC Slim — 源码交付说明

基线：用户上传的 `sing-box.rar`，Git 分支 `pqc-new`，基线提交 `229106cd`（sing-box 1.15.0-alpha.13 系列）。这是完整源码，不包含编译产物。

## 保留的功能

- **VLESS + Reality**：入站、出站；保留现有 Reality + uTLS 的 **X25519MLKEM768（PQC 混合密钥交换）** 补丁。
- **Naive**：入站、出站；出站编译需 `with_naive_outbound`，已在工作流与 libbox 默认标签中启用。
- **Hysteria2**：入站、出站。
- **TUIC v5**：入站、出站。
- **SOCKS**：入站、出站。
- **完整 TUN**：TUN 入站及相关路由、DNS、Fake-IP、UDP、规则、自动路由/接口等配置保留；Redirect/TProxy 入站保留。
- **sniff override**：`sniff_override_destination` 配置及 `adapter`、`route`、`protocol/tun` 链路保留。
- 核心路由基础设施：Direct、Block、Bridge、Selector、URLTest、DNS、Fake-IP、Clash API。
- TLS/certificate 基础设施、QUIC/HTTP3 DNS、Naive QUIC 支持（相关入站依赖）。

## 精简的功能

- 完整删除新增的 XHTTP 客户端源码、配置、选项、构建标签；还原涉及 `v2ray` 枚举和构造器的 XHTTP 补丁。
- `include/registry.go` 直接固定五类代理的入站/出站注册，不再在 Actions 临时修改。
- 删除 Hysteria v1、HTTP/Mixed、Shadowsocks、Snell、VMess、Trojan、ShadowTLS、AnyTLS、SSH/Tor 等代理注册，以及 WireGuard/Tailscale/OpenVPN/OpenConnect/MASQUE 等 endpoint 注册。
- 删除非必要业务服务注册；保留 TUN/服务端工作依赖。旧协议的**未被注册的源码文件**仍在源码仓库中，但不被核心注册入口引用，也不会仅因为目录存在就链接进编译产物。这是为了避免在尚未使用 Go 1.27.3 编译验证前误删交叉依赖，导致源码不可用。
- Android libbox 默认标签移除 WireGuard/Tailscale、USBIP、OpenVPN、OpenConnect。

## GitHub Actions

`.github/workflows/build-slim.yml` 保留 Android arm64 APK、Windows amd64、Linux amd64/arm64-musl 构建、原有 Android 签名校验和 Cronet/Naive 相关过程。CI 使用 `go1.27.3`，并先运行 `scripts/verify_slim_profile.py`。推送到 `pqc-new` 会触发；也可手动触发。

**注意：** 原签名 Secret 仍需存在；构建时需能联网下载 Go 模块、Cronet 及工具链。本交付只通过源码静态检查，**未通过 Go 1.27.3 编译或 Android 打包验证**。

## 本地校验（Windows PowerShell）

建议先备份现有工作区；从本压缩包取出新源码覆盖你的仓库工作树（保留自己的 `.git`），**或者**优先使用交付的 `.patch` 文件（必须从与上传压缩包同一基线提交开始）。

```powershell
cd D:\dev\sing-box
git switch pqc-new
git status --short
# 备份或提交你已有的未提交修改后，再执行
# 将下面的路径改为实际下载位置：
git apply --check --ignore-space-change "D:\Downloads\sing-box-pqc-slim.patch"
git apply --ignore-space-change "D:\Downloads\sing-box-pqc-slim.patch"

python scripts\verify_slim_profile.py
gofmt -l include\registry.go include\quic.go cmd\internal\build_libbox\main.go
go version

# 检查核心配置可解析、编译成功
go build -trimpath -tags "with_quic,with_utls,with_clash_api,with_naive_outbound,badlinkname,tfogo_checklinkname0" -o dist\sing-box-pqc-slim.exe .\cmd\sing-box
.\dist\sing-box-pqc-slim.exe version
.\dist\sing-box-pqc-slim.exe check -c .\你的配置.json
```

Linux/macOS 可把 `-o dist/...exe` 换成实际文件名。**只有通过对应系统/架构的编译、配置检查和真实节点连接测试，才能确认全部功能可用**。建议分别实测上述五种入站/出站、Reality+PQC、TUN 接管 UDP/DNS/Fake-IP 与 sniff override。

确认后再提交：

```powershell
git diff --stat  # 先确认没有其他意外修改（Windows 的 CRLF 可能影响显示）
git add -A
git commit -m "slim: remove XHTTP; keep PQC sniff override VLESS Reality Naive HY2 TUIC SOCKS TUN"
git push origin pqc-new
```

此补丁不修改 Git 仓库的远程 URL，也不会直接向你的 GitHub 推送代码。
