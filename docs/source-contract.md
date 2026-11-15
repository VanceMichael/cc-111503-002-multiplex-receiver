# 来源接口与资料语义

使用 RFC6455 WebSocket，应用消息为 UTF-8 JSON。subscribe(requestId,topics) 返回 bound(requestId,channel,subscriptionToken)；仅 bound 建立归属。offer 含 channel、subscriptionToken、deliveryId、payload。unbound(token) 后该 token 不再接受新 offer；channel 可重用。pause/resume 按 token 生效，心跳为 WebSocket ping/pong，不是交付数据。ack 必须含 token 和 deliveryId，已收同内容可以重 ack，同 deliveryId 不同 payload 隔离。新的连接必须重新 subscribe；服务端会重投未 ack 交付，但 token 归属只在本连接有效。

## 收件器扩展接口

现有 `src/intake.mjs` 导出 `preview(source, store)`，只做预览。收件器入口为新增的 `collect(source, store)`，由调用方负责创建和关闭来源及 Inbox。`npm run acceptance` 调用这一入口。返回的 `records` 是可正式交付资料，不把未确认片段放进该集合；特殊业务的 pending 和 diagnostics 独立返回。原始证据通过 Inbox 保存，正式记录应含证据引用。接口不规定内部架构，允许增加模块和持久表，但不能移除原预览或更改来源语义。

## 可观察边界

- 两个通道交错投递且一个暂停
- bound 之前的 offer 不得登记为已归属
- 同一 channel 先后绑定不同 token
- 同交付重投与内容冲突
- 持久化失败不得发 ack、连接关闭不得留下后台重连

## 公开的结果字段

`acceptance/collection.test.mjs` 给出当前集成方使用的结果字段及最小示例，说明的是输出契约而非内部处理步骤。除这些示例外，以上来源语义也属于功能范围，不能靠场景名称分支返回固定答案。`status` 仅在对应资料闭合且可靠取得时为 complete。
