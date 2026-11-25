import type { Quote } from "./types";

// 两个渠道 A/B 为同一笔 ETH->USDC 报价，A ~0.2s 返回，B 晚 3s。
// 展示：报价陆续到达、选中 B 后如何执行、B 询价/下单超时与重复确认的处理。

// 时间线（roundDeadline=2.5s，late_quote 宽限 1.5s）：
//   0.0s  并发询价 A、B
//   0.2s  A 返回 -> quote 事件            首个报价，TTFQ≈0.2s
//   2.5s  本轮截止 -> round_complete      B 标 timeout
//   3.0s  B 返回 -> late_quote            宽限期内补上
// 关掉 late_quote 时 B 本轮不展示，客户端停留则下一轮捞回。

async function clientConsume(stream: AsyncIterable<any>) {
  const shown = new Map<string, Quote>(); // channelId -> 当前报价

  for await (const ev of stream) {
    switch (ev.event) {
      case "quote":
      case "late_quote":
        shown.set(ev.data.channelId, ev.data); // 保留已有，新到的替换
        rerankAndRender(shown); // 按 usdTotal / toAmount 重排
        break;
      case "round_complete":
        stopSpinner(ev.data); // 停转圈，展示谁超时；已有报价不清空
        break;
    }
  }
}

// 用户选中 B 后：带 B 的 quoteId 调执行，由执行服务定位到 B 的 Adapter。
async function onUserPick(pickedQuote: Quote) {
  const idempotencyKey = deriveKey(pickedQuote.quoteId, currentUser()); // 稳定，重试复用

  const resp = await POST("/v1/execute", {
    quoteId: pickedQuote.quoteId,
    idempotencyKey,
    recipient: currentUser().destAddress,
  });

  if (resp.code === "QUOTE_EXPIRED") return reQuote();
  if (resp.kind === "unsigned_tx") return walletSignAndBroadcast(resp.tx); // B 是桥
  if (resp.kind === "order") return showDepositAddress(resp.order); // B 是订单式
}

// 下单超时后用户再次确认：复用同一 idempotencyKey，服务端去重+对账，不会重复下单。
async function onUserConfirmAgain(pickedQuote: Quote) {
  const idempotencyKey = deriveKey(pickedQuote.quoteId, currentUser());

  const resp = await POST("/v1/execute", {
    quoteId: pickedQuote.quoteId, idempotencyKey, recipient: currentUser().destAddress,
  });

  switch (resp.code) {
    case "PROCESSING":
    case "PROCESSING_RECONCILING": return showPendingUI();
    case "NOT_CREATED_RETRY": return onUserConfirmAgain(pickedQuote); // 确认未建，可安全重建
    default: return showDepositAddress(resp.order); // 唯一的订单
  }
}

declare function rerankAndRender(shown: Map<string, Quote>): void;
declare function stopSpinner(summary: unknown): void;
declare function deriveKey(quoteId: string, user: any): string;
declare function currentUser(): any;
declare function POST(path: string, body: any): Promise<any>;
declare function walletSignAndBroadcast(tx: any): Promise<void>;
declare function showDepositAddress(order: any): void;
declare function showPendingUI(): void;
declare function reQuote(): void;

export { clientConsume, onUserPick, onUserConfirmAgain };
