# 多渠道 Swap 服务设计

同链与跨链兑换的后端服务。用户发起 ETH → USDC，先并发拿到各渠道报价，选一笔后在钱包签名或向订单地址转账，之后可查询是否到账。

目标：接入多渠道且易扩展（20+）；询价快，首个有效报价不被慢渠道拖累；执行可靠，重复确认不重复下单；跨链能区分源链确认与目标链到账。

## 架构

```
         ┌── Quote Service ───┐
Client ──┤   Execute Service  ├── ChannelRegistry ── [ Adapter x N ] ── 各渠道上游
         └── Tracking Service─┘          │
                                 统一模型 / Redis / Kafka
```

分三个服务，共用一套渠道注册表与统一模型：

- **Quote Service**：筛选渠道、并发询价、流式返回（SSE）。
- **Execute Service**：按选中报价构建交易或创建订单，处理幂等。
- **Tracking Service**：统一状态机，Webhook + 轮询。

渠道差异全部收敛到 `ChannelAdapter`。平台只认统一模型，不感知具体渠道。接入新渠道 = 实现一个 Adapter 并注册，其余不动。

## 统一模型与渠道接口

金额统一用「最小单位整数字符串 + decimals」，避免浮点精度问题（ETH=18，USDC=6）。

```ts
interface Amount { token: string; decimals: number; amount: string } // "1000000000000000000" = 1 ETH

interface ChannelAdapter {
  capabilities(): ChannelCapability;                          // 支持的网络/币对/类型/是否支持幂等令牌
  quote(req: QuoteRequest, signal: AbortSignal): Promise<Quote>;
  execute(q: Quote, ctx: ExecuteCtx): Promise<ExecuteResult>; // DEX/桥返回未签名交易，订单式返回收款地址
  getStatus(ref: ChannelRef): Promise<SwapStatus>;
}
```

三类渠道实现同一接口：


| 类型     | execute 产物 | 用户动作         |
| ------ | ---------- | ------------ |
| 同链 DEX | 未签名交易      | 钱包签名广播       |
| 跨链桥    | 源链未签名交易    | 钱包签名广播，状态分两腿 |
| 先下单再转入 | 订单 + 收款地址  | 向地址转账        |


完整类型见 `[pseudocode/types.ts](../pseudocode/types.ts)`，三类 Adapter 见 `[pseudocode/adapters.ts](../pseudocode/adapters.ts)`。

## 询价

`ChannelRegistry.eligible(req)` 按网络、币对、启用状态、能力筛出候选渠道，并发调用各自的 `quote()`，每个渠道独立软超时。报价到一个推一个（SSE），到本轮截止推 `round_complete` 列出每个渠道的结果。慢渠道超时不影响已到的报价，客户端据 `round_complete` 结束本轮。

```ts
for (const ch of channels) {
  askOneChannel(ch, req).then(
    (q)   => inbox.push({ event: "quote", data: q }),
    (err) => summary.byChannel[ch.id] = err.reason, // timeout | error
  );
}
delay(ROUND_DEADLINE_MS).then(() => inbox.push(DONE));

while ((item = await inbox.take()) !== DONE) yield item;
yield { event: "round_complete", data: summary };
```

每个渠道调用外包一层熔断 + 舱壁：熔断打开的渠道直接快速失败，舱壁给每个渠道独立并发配额，防止单渠道打满连接池。见 `[pseudocode/aggregator.ts](../pseudocode/aggregator.ts)`。

报价带 `quoteId`，服务端存 `quoteId → {channel, 入参, 报价快照, expiresAt}`（Redis 短 TTL）。客户端只拿到不透明的 `quoteId`，执行时服务端查回，价格无法被篡改。

## 执行

客户端带选中报价的 `quoteId` 调 `/v1/execute`。服务端查回报价，判过期，按 `quoteId` 定位到同一个渠道的 Adapter：

- DEX/桥：`execute()` 返回未签名交易，无服务端副作用，幂等由钱包 nonce 保证。
- 订单式：创建订单有副作用，走幂等。



### 幂等

幂等键由 `quoteId + user` 派生，重试复用。创建订单前先原子占位 `insertIfAbsent(key, PENDING)`；重复确认会命中已有记录，直接返回同一订单，不会二次创建。

关键点是超时：创建调用超时 = 结果未知，不能重试创建（可能已经建了）。标为 `UNKNOWN`，用 `clientOrderRef` 向上游反查这单是否存在——查到就认领，查无才允许重建。

```ts
const inserted = await idemp.insertIfAbsent({ key, state: "PENDING" });
if (!inserted) {
  const rec = await idemp.get(key);
  if (rec.state === "CREATED") return rec.result;      // 返回同一订单
  if (rec.state === "PENDING") return PROCESSING;
  return reconcile(key, adapter);                      // UNKNOWN
}
try {
  const res = await adapter.execute(quote, { clientOrderRef: key, ... });
  await idemp.update(key, { state: "CREATED", result: res });
  return res;
} catch (e) {
  if (isTimeout(e)) { await idemp.update(key, { state: "UNKNOWN" }); return reconcile(key, adapter); }
  await idemp.remove(key); throw e;                    // 明确失败才释放，允许重试
}
```

见 `[pseudocode/execute.ts](../pseudocode/execute.ts)`。

## 状态

统一状态机，对外五态：`PENDING / SOURCE_CONFIRMED / COMPLETED / FAILED / REFUNDED`，细态放 `detail`。

跨链是两条腿，`SwapStatus` 始终带 `sourceTx` 和 `destTx`：

- `SOURCE_CONFIRMED`：源链已确认，`sourceTx` 可查，`destTx` 为 null。
- `COMPLETED`：目标链已到账，`destTx` 可查。

状态来源 Webhook 优先、轮询兜底。源已确认但超过 ETA 若干倍仍无 `destTx` 时升级告警。见 `[pseudocode/status.ts](../pseudocode/status.ts)`。

## 示例：两个渠道报价，A 快 B 晚

A 约 0.2s 返回，B 晚 3s。聚合器并发询价，A 先推给客户端；到 2.5s 本轮截止，B 未回，`round_complete` 标 B 为 timeout（开启宽限期则 B 在 3s 以 `late_quote` 补上）。已展示的 A 不受影响。

用户随后选中 B：客户端带 B 的 `quoteId` 调执行，由执行服务定位到 B 的 Adapter 构建交易或创建订单——不是聚合器做。

- B 询价超时：B 标 timeout，不影响 A，客户端可重试。
- B 下单超时且用户再次确认：复用同一幂等键，命中占位则返回同一订单或走对账，不会重复下单。

见 `[pseudocode/worked_example.ts](../pseudocode/worked_example.ts)`。

## 性能

目标：500 QPS 询价下，只要有渠道能报价，95% 的请求 2s 内出首个有效报价（TTFQ p95 < 2s）。

首个有效报价只取决于最快返回的那个渠道，慢渠道被超时和熔断隔离，不占预算。服务 I/O 密集，靠异步、连接池复用、按 TTFQ 水平扩容承接。

验证：埋点 TTFQ 作为 SLO 度量口径；用注入真实时延分布的 mock 上游压 500 QPS；混沌测试让部分渠道变慢/报错，确认只要还有一个健康渠道 SLO 仍成立，全部渠道失败时明确返回「本轮无报价」而非挂起。

## 技术栈

Node.js / NestJS，模块按 Quote/Execute/Tracking 域拆分；并发扇出用 `Promise.allSettled` + `AbortController`；询价流式 SSE；Quote Store 与幂等用 Redis（短 TTL、Lua 原子操作）；异步编排 BullMQ；状态事件广播 Kafka。路由重计算可下沉 Worker Threads。询价热路径若需更低尾延迟，可用 Go 承载聚合层，接口与模型不变。