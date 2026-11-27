# SB-XHTTP alpha14 Recovery R1（源代码说明）

基线：sing-box 1.15.0-alpha.14 去重整合版，保留官方 Reality/uTLS/PQC/HTTP2/H3、现有自研 SB-XHTTP、Vision 内层 TLS、VLESS ENC、Replay-v2 及单通道 CDN。

本轮实际改动仅针对独立直连 H2 的故障探测策略。原有代码已经实现 Context 取消、物理连接关闭、H2 PING、超时保护和独立会话管理，**没有重复实现这些机制**。

## 新增参数

`transport.recovery_profile`：

- 不填写或 `balanced`：原配置完全不变。客户端 H2 `ReadIdleTimeout=45s`、`PingTimeout=20s`、`WriteByteTimeout=75s`，服务端 H2 45s/20s。
- `fast`：仅普通直连 `mode=""` 且 `reliable=false`。客户端 H2 30s/15s/60s，服务端 H2 30s/15s。**这是可选快速探测，不是会话无损恢复。**
- `fast` 与 CDN 或 Replay-v2 搭配时明确报错，防止覆盖已有协议的恢复逻辑。

两个端点可以独立选择探测档位；参数不出现在传输报文里，不需要变更密钥、UUID、path、Vision flow 或 Reality 配置。

建议第一轮先只在 KR 客户端启用 `"recovery_profile": "fast"`，服务端仍按原参数运行，观察正常长连接会否误断。若稳定，再考虑在 KR 服务端启用同档位。SG-CDN 不做此项修改。

## 已做的源码保护

- 默认配置仍使用原有 PING/超时参数；不改变独立 H2/TCP 的设计。
- 不增加共享连接池、XMUX、MultiTCP、多 IP 并发或 Replay-v2。
- 正常静默会话不会因为这个选项而设置 HTTP 应用层空闲截止；触发的是协议层 PING 探测。
- 不改变客户端/服务端 SB-XHTTP wire format。
- 没有改动官方 alpha14 的共用 HTTP/2/H3、DNS、TUN 或 QUIC 源码。

## 验证状态

已做 Go parser 全源码语法检查、`scripts/verify_slim_profile.py` 静态检查，以及独立策略测试（Go 1.23 race）；实际 sing-box 工程完整编译、依赖/API 检查、端到端 Reality+Vision 回归仍需使用用户的远程 Go 1.27 编译环境完成。

编译后执行：

```bash
go test -tags 'with_quic,with_utls,with_clash_api,badlinkname,tfogo_checklinkname0' ./transport/sbxhttp
./sing-box check -c /opt/sb/config.json
```

测试快检请在不中断生产节点时进行；若快档发生误断，只需删掉 `recovery_profile` 恢复默认策略，无需回滚协议。
