# SB-XHTTP × sing-box 1.15.0-alpha.14：统一整合版

## 基础与原则

- 官方基线：`SagerNet/sing-box` 标签 `v1.15.0-alpha.14`，提交 `126ce6c21`。源代码从用户原压缩包随附的 Git 对象中 `git archive` 提取；**不是**仅修改旧版的版本号。
- 自定义基线：用户上传的 `sing-box(2).zip`，`pqc-new` 分支 `96c20d1d` 及用户工作区的未提交修改；应用时以此前恢复 alpha13 基线 `229106cda` 的自定义增量为参考。
- 采用 **一个源码版本**，不再分保守版/实验版。面向用户的 Go 1.27 远程编译环境；`go.mod` 保留官方声明的最低 Go 1.26.11。
- 功能目标：**SB-XHTTP + Reality + Vision 直连为主**，VLESS ENC + CDN 单通道作为备用；Replay-v2、HY2、TUIC、Naive、TUN、FakeIP、Clash API 等与原部署相关能力保持兼容。SB-XHTTP 本身不增加 XMUX、多 TCP 或多 IP 聚合。

## 官方 alpha14 纳入情况

此版以官方 alpha14 完整工作树为基础，因此保留它的核心代码更新，包括：

- HTTP/2、HTTP/3 通用客户端连接管理重构（`33a8e9ba2`）：并发建连协调、会话/Stream 使用计数、空闲连接清理和 QUIC 请求失败恢复处理。**注意：这些实现位于官方 `transport/http`，不会自动替代私有 SB-XHTTP 的独立 HTTP/2 会话；没有宣称它们给 SB-XHTTP 带来了 XMUX 或跨会话性能收益。**
- HTTP 出站握手有界超时（`5004b40e4`），以及私有 SB-XHTTP Replay-v2 重连接时只约束响应头等待、建立连接后正常长流不被定时器误杀。
- quic-go 0.66.0 定制依赖（`a8156d220`）和 Go 最低版本 1.26.11/基础依赖（`bc1075f25`），同时包含官方 HTTP/3、HY2、TUIC 及其它 QUIC 修复。
- 上游 DNS Sniffer、FakeIP 缓存一致性、TUN/auto-redirect、网络地址变化后的 UDP 清理、拨号器与错误处理、协议输入校验及通用资源释放修复。

## 自研功能保留/手动适配

- `transport/sbxhttp/` 的源文件从用户工作区保留：Replay-v2 ACK/有限重放、独立 H2 传输、CDN 单通道 H2/H3、可选 inner TLS、PING 与超时保护；维持现有线上帧格式，不改协议版本。
- `protocol/vless/encryption/` 和 ENC 出入站加密握手来自用户源码；`option/vless.go` 中自定义加密配置保持可用。
- 手工适配新版 `protocol/vless/{inbound,outbound}.go`：入站 `References()` 返回值变化；TLS 客户端的 DomainResolver 传递及 nil-config 判断；接口重置和私有加密握手仍保留。
- 将私有 `sbxhttp` 重新注册到 `constant/v2ray.go`、`option/v2ray_transport.go`、`transport/v2ray/transport.go`，在两端继续拒绝使用 SB-XHTTP + multiplex，不恢复官方/历史的 Xray XHTTP/XMUX 代码。
- 额外保留早于本轮开发分支的 PQC 定制：uTLS random/randomized 的混合密钥份额选择、Reality `key_share`（default/hybrid/classical）、必要的 ALPN 重建和 hybrid-only ECDHE 处理。保留 alpha14 证书验证时间/根证书处理，不回退到旧 Reality 实现。
- 采用官方 alpha14 的最新 DNS/FakeIP/TUN/路由代码；用户此前去掉的 `sniff_override_destination` 兼容补丁不重新引入。
- 原自编译 GitHub Action 工作流保留，Go 版本更新为 1.27.4；请注意 Action 默认仍会克隆指定 Git 仓库，**仅解压本包不等于 Action 已改用本包源码**，需先推送到自己的构建分支。

## 本地检查与限制

- `scripts/verify_slim_profile.py`：已检查私有传输进出站注册、精简标签及 Reality/PQC 自定义文件。
- Go 标准解析器已检查全部 Go 源文件无语法错误。`scripts/build_alpha14_remote.sh` 是用于用户远程环境的**实际编译与单元测试**脚本，本环境 Go 1.23 无法运行新版主模块的完整编译。
- **本包没有通过 Go 1.27 完整构建，更没有通过 KR/SG 实际网络回归测试；不能宣称已可直接替换线上程序。** 需要使用用户远程环境验证工具链和升级的依赖接口。
- 源码保留原定制的线协议，官方通用 HTTP 连接池不等于自动获得新的 SB-XHTTP 跨会话连接复用。若要实现该特性，应在双端添加明确的兼容协商并独立测试，而不能在这次直接开启。

## 远程编译检查

解压后在 `sing-box` 源码根目录：

```bash
go version
python3 scripts/verify_slim_profile.py
bash scripts/build_alpha14_remote.sh
```

以上脚本默认不启用 Naive/Cronet，只用于纯 Go 依赖的 smoke build。要用已有 Cronet 环境编译完整 profile，可运行：

```bash
WITH_NAIVE=1 bash scripts/build_alpha14_remote.sh
```

编译成功仍需新旧客户端/服务端对应配置进行测试：Reality + Vision、VLESS ENC（TCP/UDP）、Replay-v2 长连接重连、单通道 CDN H2/H3、HY2/TUIC/Gecko/端口跳跃、N1 TUN/FakeIP/DNS 及连接关闭后的 FD/goroutine 清理，并比较晚高峰吞吐和 CPU。**不要直接覆盖线上二进制。**
