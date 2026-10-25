# 接入本地微信（wechat-use）

该适配器复用 [wechat-use](https://github.com/leeguooooo/wechat-use) 已登录的 macOS 微信，通过 **native HTTP Bridge + SSE** 收发文字消息。无需再次扫码，也不使用微信官方 iLink 机器人通道。微信运行端需要 macOS Apple Silicon 和 wechat-use 支持的独立微信副本；AstrBot 可在能访问该 Bridge 的主机上运行。

## 准备

1. 按 wechat-use 的官方流程完成安装、激活及专用微信登录。
2. 检查 `wechat-use doctor --json` 和 `http://127.0.0.1:18400/health`；daemon、账号及接收状态必须就绪。HTTP 200 本身不代表就绪。
3. 复用已运行的默认 native Bridge。未运行时才启动 `wechat-bridge`，**不要添加 `--shape hermes`**，不要启动第二个占用同一端口的 Bridge。
4. 用 `wechat-use sessions --json` 查询需要接入的精确群 ID。填写工具专用微信当前已登录账号的 wxid，不是昵称。

## 在管理面板创建机器人

在 **机器人 → 创建机器人** 中选择 **WeChat (wechat-use)**：

| 字段 | 填写内容 |
| --- | --- |
| Bridge URL | 默认 `http://127.0.0.1:18400` |
| Bridge Bearer Token | 仅 Bridge 设置了 `WECHAT_BRIDGE_BEARER` 时填写 |
| 当前微信账号 wxid | 已登录账号的精确 wxid，必填 |
| 允许回复的群 ID | 精确的 `xxx@chatroom` 列表；留空禁止所有群 |
| 群聊仅在被 @时回复 | 默认开启，读取 Bridge 的结构化 `is_mentioned` |
| 接收和回复私聊 | 默认关闭 |

打开启用开关并保存。然后在机器人使用的 **配置文件 → AI** 中选择模型、人格和知识库。Magpie 可作为 OpenAI 兼容提供商接入：Base URL `http://127.0.0.1:3425/v1`，模型填写网关中的完整模型名称。模型 API 密钥与 Bridge Token 是不同配置。

AstrBot 的访问控制、唤醒词和群聊会话隔离继续生效。只创建一个处理目标群的 wechat-use 机器人，并暂停旧的自动回复脚本，避免两个机器人同时回复。**本功能不会自动暂停任何已有机器人。**

## 消息与发送边界

- 当前版本只接收和发送文字。不支持图片、语音、文件、发送 @或引用回复；请关闭回复中的引用和 @装饰。不支持的消息组件会被明确拒绝，不会只发送其中一部分。
- SSE 使用明确的当前时间作为启动边界；启动和重启不补发旧消息。重连保留游标并通过本地持久化消息 ID 去重，过期超过 5 分钟的消息不处理。
- 消息入队前先保存 claim。进程崩溃可能导致该条消息没有回复，适配器不会为了补偿而重复发送。
- `submitted_unconfirmed` / `status_unknown` 只表示已提交待确认或结果未知，不表示送达；超时不会自动重试。发送记录只保存在 AstrBot `data/wechat_use/` 下的自有 SQLite 文件，**不打开微信原库**。
- 每次提交记录本地请求 ID；Bridge 返回的原始 request ID 单独保留。需要核对时沿用原回执，不换 ID 重发。
- 健康检查不包含完整账号 ID；必须确保设置中的 wxid 与 Bridge 账号一致。检测到其他账号的自发消息时会停止该批处理。
- Bridge 应保持本机回环访问。Docker 的 `127.0.0.1` 指容器本身；跨主机接入请使用受保护的网络及 Bearer Token，不公开暴露 Bridge。

## 验证

```bash
uv sync
.venv/bin/python -m pytest tests/test_wechat_use_adapter.py -q
```

测试使用本地模拟 Bridge 验证收发、重连、持久化去重、权限边界和未知发送状态，不向微信联系人发送测试消息。需要真实群聊验收时，确认目标群及旧机器人已暂停后，再由群成员发送一条新的 @消息。
