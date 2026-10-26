# 接入本地微信（wechat-use）

该适配器复用 [wechat-use](https://github.com/leeguooooo/wechat-use) 已登录的 macOS 微信，通过 **native HTTP Bridge + SSE** 接收消息，并通过 Bridge / 本机 CLI 发送文字、图片、文件和群提及。无需再次扫码，也不使用微信官方 iLink 机器人通道。微信运行端需要 macOS Apple Silicon 和 wechat-use 支持的独立微信副本。

## 能力范围

| 功能 | 接入方式与边界 |
| --- | --- |
| 文字、群 @、私聊 | 原生事件转换；群白名单、结构化 @判断、自发消息过滤、持久化去重 |
| 接收图片 / 表情图片 | 在范围检查和 claim 后通过 `/image` 按精确消息 ID 取图；事件结束后清理临时图片。依赖上游能够验证该图片 |
| 引用旧图片 / 语音 | 新的有效提问可以解析同一会话最近 100 条记录内所引用的精确消息；不解析整个历史的媒体 |
| 接收语音 | 可选本地 Whisper 转写、解码为 WAV 交给 AstrBot STT，或关闭媒体读取。上游音频读取被拒绝时保留不可用状态 |
| 发送图片、文件 | 同机 CLI `send --image/--file`；发送前冻结附件副本；单文件限制 100 MiB |
| 发送群成员提及 | 同机 CLI `send --mention`，传精确 wxid；是否提交由当前 runtime 决定。@全体未开放 |
| 引用回复 | 接收转换为 `Reply`；原生引用发送未开放，可明确降级为 `[引用 …]` 文字，也可配置为拒绝 |
| 链接、小程序、合并转发 | 保留结构化消息与可读文字；链接、转发和位置发送使用有标注的文字，不伪装为原生卡片 |
| 文件 / 视频接收 | 保留文件名等可用元数据；上游没有通用附件下载接口时不伪造下载地址，不读取消息提供的任意本地路径 |
| 音频 / 视频发送 | 上游未开放原生语音气泡和视频消息发送；以文件附件发送，回执附带 `audio_as_file` / `video_as_file` 标记 |
| 撤回、系统、转账 / 红包通知 | 保留可用通知信息，作为 `OtherMessage` 交给插件，不自动当作私聊提问，不执行金融操作 |
| 查询与核对 | 会话、联系人、未读、解析候选、历史、搜索、统计、导出、群成员和原回执核对；保留原覆盖状态，不把不可用结果解释为空 |
| 模型、人格、推理参数、知识库 | 使用 AstrBot 原生配置；适配器输出标准消息组件，图片进入视觉链路，WAV 进入 STT 链路 |

朋友圈、收藏、加好友、原生引用发送、原生语音发送等未开放接口不通过绕过校验补齐。Windows 实验 CLI 没有本适配器需要的 HTTP Bridge。

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
| 本机 CLI 路径 | 默认 `auto`；自动发现 `wechat-use`，也可填绝对路径 |
| 语音消息处理 | `wechat_use`：已有 Whisper；`astrbot`：AstrBot STT 提供商；`off`：只保留通知 |
| 引用回复降级为标注文字 | 默认开启；关闭时明确拒绝原生引用发送请求 |

打开启用开关并保存。然后在机器人使用的 **配置文件 → AI** 中选择模型、人格和知识库。Magpie 可作为 OpenAI 兼容提供商接入：Base URL `http://127.0.0.1:3425/v1`，模型填写网关中的完整模型名称。模型 API 密钥与 Bridge Token 是不同配置。

AstrBot 的访问控制、唤醒词和群聊会话隔离继续生效。只创建一个处理目标群的 wechat-use 机器人，并暂停旧的自动回复脚本，避免两个机器人同时回复。**本功能不会自动暂停任何已有机器人。**

## 消息与发送边界

- 图片读取走经过验证的 `/image` 接口，不把 AES 密钥、加密 CDN URL 或 XML 当成模型图片输入。只接收已命中范围和触发策略的消息；群里未被结构化 @的媒体不会因昵称匹配而触发读取。图片取图限制 32 MiB。
- 混合消息先校验全部组件和本机 CLI 账号绑定，再按顺序提交。明确拒绝时中止；结果未知时停止剩余部分；已提交待确认的部分不重发。所有部分回执都保留原请求 ID。
- 语音、视频发送会明确降级为文件附件；其他未支持的组件在任何提交前被拒绝。引用的 `[引用 …]` 文字不是微信原生引用气泡。
- SSE 使用明确的当前时间作为启动边界；启动和重启不补发旧消息。重连保留游标并通过本地持久化消息 ID 去重，过期超过 5 分钟的消息不处理。
- 消息入队前先保存 claim。进程崩溃可能导致该条消息没有回复，适配器不会为了补偿而重复发送。
- `submitted_unconfirmed` / `status_unknown` 只表示已提交待确认或结果未知，不表示送达；超时不会自动重试。发送记录只保存在 AstrBot `data/wechat_use/` 下的自有 SQLite 文件，**不打开微信原库**。
- 每次提交记录本地请求 ID；Bridge 返回的原始 request ID 单独保留。需要核对时沿用原回执，不换 ID 重发。
- 已提交附件的冻结副本保留在 `data/wechat_use/outbox/`，避免原临时文件消失后后台上传失败。不要在未知任务仍在进行时手动清理这些副本。
- 健康检查不包含完整账号 ID；必须确保设置中的 wxid 与 Bridge 账号一致。检测到其他账号的自发消息时会停止该批处理。
- Bridge 应保持本机回环访问。Docker 的 `127.0.0.1` 指容器本身；跨主机接入请使用受保护的网络及 Bearer Token，不公开暴露 Bridge。
- 本机 CLI 会核验工具专用微信配置中的账号，不接受进程、bundle 或账号覆盖参数。远程 Bridge 仍可收发文字和取图片；需要 CLI 的操作必须部署在微信所在 Mac 上。

## 插件中的查询接口

通过 `event.adapter.get_client()` 取得 `WechatUseClient`，可调用：

```python
client = event.adapter.get_client()
history = await client.get_history(event.get_session_id(), limit=50)
contacts = await client.get_contacts(query="姓名")
sessions = await client.get_sessions()
unread = await client.get_unread()
candidates = await client.resolve_recipient("名称")  # 返回候选，不自动选人
results = await client.search_messages("关键词", event.get_session_id())
stats = await client.get_stats(event.get_session_id())
export = await client.export_history(event.get_session_id(), format="json")
group = await event.get_group()
receipt = await event.adapter.reconcile_submission(original_adapter_request_id)
```

历史、搜索、统计和导出限制在配置范围内。群成员、统计等命令是否可用以当前 wechat-use runtime 为准；`event.get_group()` 查询不可用时保留已有群信息并设置 `wechat_use_group_members_unavailable`，不伪造空成员列表。

若本机语音接口返回 `voice_data not found` / `source_database_access_paused`，表示上游没有为该语音提供可用的受验证读取路径。可使用事件中已有的转写；没有转写时会显示媒体不可用。切换 STT 提供商不能修复底层取音频失败，也不要打开或直接读取微信原库。

接收端不可用时，管理面板的错误详情及 AstrBot 日志会区分后台、账号和读取故障。`stage=profile_fetch` 表示上游适配资料获取阶段失败，`stage=profile_verify` 表示资料验证阶段失败；阶段值只从固定列表中选取，不写入上游任意文本、账号或凭据。底层详细诊断仍以 `wechat-use` 官方命令和本机 `~/.wx-rs/wechatd.log` 为准。恢复后适配器自动重连，不补发旧消息。

发送日志和适配器自有记录保留固定的拒绝原因码（如 `native_send_source_unavailable`）。CLI 的嵌套 `receipt` 也会核验账号、收件人和原请求，再更新提交状态；明确的 `not_submitted` 不再误标成未知，也不会自动补发。

发送返回不明确时，适配器最多用原请求 ID 自动读取一次回执（5 秒超时）。核对失败仍保留未知状态，不重发，也不启动新的发送任务。

## 验证

```bash
uv sync
.venv/bin/python -m pytest tests/test_wechat_use_adapter.py -q
```

测试使用本地模拟 Bridge、合成媒体和临时 CLI 验证多媒体、真实 SILK 解码、组件转换、重连、持久化去重、权限边界、参数隔离、超时和未知发送状态，不向微信联系人发送测试消息。可另做 CLI `--dry-run` 检查和只读 Bridge 检查。需要真实群聊验收时，确认目标群及旧机器人已暂停后，再由群成员发送一条新的 @消息。
