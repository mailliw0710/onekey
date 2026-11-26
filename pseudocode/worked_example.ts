import type { Quote } from "./types";

// 两个渠道 A/B 为同一笔 ETH->USDC 报价，A ~0.2s 返回，B 晚 3s。
// 展示：报价陆续到达、选中 B 后如何执行、B 询价/下单超时与重复确认的处理。

// 时间线（A softTimeout=1.5s，B 是桥 softTimeout=4s，本轮截止=max(各渠道)+余量≈4.3s）：
//   0.0s  并发询价 A、B
//   0.2s  A 返回 -> quote 事件            首个报价立即展示，TTFQ≈0.2s（与 B 的超时无关）
//   3.0s  B 返回 -> quote 事件            仍在 B 自己的 4s 预算内，正常展示、可选
//   4.3s  本轮截止 -> round_complete
// 要点：A 的首屏靠流式保证，不靠压 B 的超时；B 超时按它自己的实测延迟配，所以 3s 回来照样能选。

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
  if (resp.kind === "unsigned_tx") {
    const txHash = await walletSignAndBroadcast(resp.tx); // B 是桥：钱包签名广播
    // 快路径：回传 hash 绑定到 swapId。但这只是加速——即使 App 现在就被关掉、永不回传，
    // 服务端的链上 watcher 也会按用户地址兜底绑定（见 status.ts watchBroadcasts）。
    void reportBroadcast(resp.swapId, txHash).catch(() => {});
    return;
  }
  if (resp.kind === "order") return showDepositAddress(resp.order); // B 是订单式
}

// 重开 App：按用户账户拉 swap 列表，不依赖本地状态
//（swapId 在 execute 时就已落库、挂在账户下，每笔含最新 SwapStatus 的源腿/目标腿）。
async function onAppReopen() {
  const swaps = await GET(`/v1/swaps?account=${currentUser().id}`);
  renderSwapList(swaps);
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
declare function GET(path: string): Promise<any>;
declare function walletSignAndBroadcast(tx: any): Promise<string>; // 返回 txHash
declare function reportBroadcast(swapId: string, txHash: string): Promise<void>;
declare function renderSwapList(swaps: any[]): void;
declare function showDepositAddress(order: any): void;
declare function showPendingUI(): void;
declare function reQuote(): void;

export { clientConsume, onUserPick, onUserConfirmAgain, onAppReopen };
