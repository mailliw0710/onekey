import type { ChainId, SwapStatus } from "./types";
import type { ChannelRegistry } from "./registry";

// 状态跟踪：Webhook 优先，轮询兜底。跨链分别暴露源腿/目标腿。

interface SwapRecord {
  swapId: string;
  channelId: string;
  account: string; // 归属用户，重开按它列表
  // 广播类渠道（DEX/桥）在 execute 时就记录以下绑定信息，供 watcher 兜底找 tx：
  userAddress: string;
  chainId: ChainId;
  expectedTo?: string; // 构建交易时的目标合约（桥路由）地址
  nonce?: number; // 可选：用 (address,nonce) 更精确匹配
  providerRef?: string; // tx hash / 订单号；广播类渠道拿到 hash 前为空
  lastStatus?: SwapStatus;
}

interface Deps {
  registry: ChannelRegistry;
  db: {
    getSwap(swapId: string): Promise<SwapRecord | null>;
    getSwapsByAccount(account: string): Promise<SwapRecord[]>;
    saveStatus(s: SwapStatus): Promise<void>;
    setProviderRef(swapId: string, providerRef: string): Promise<void>; // 幂等
  };
}

async function getSwapStatus(swapId: string, deps: Deps): Promise<SwapStatus | { code: "NOT_FOUND" }> {
  const rec = await deps.db.getSwap(swapId);
  if (!rec) return { code: "NOT_FOUND" };

  if (isFresh(rec.lastStatus)) return rec.lastStatus!;
  // 还没拿到 hash（未广播，或 watcher 尚未绑定）：返回等待态，不去查上游
  if (!rec.providerRef)
    return { swapId, channelId: rec.channelId, code: "PENDING", detail: "AWAITING_BROADCAST", updatedAt: Date.now() };

  const adapter = deps.registry.get(rec.channelId)!;
  const fresh = await adapter.getStatus({ channelId: rec.channelId, providerRef: rec.providerRef });
  await deps.db.saveStatus(fresh);
  return fresh;
}

// 重开 App：按账户列出该用户的 swap（swapId 在 execute 时已落库、挂在账户下）
async function listSwapsByAccount(account: string, deps: Deps): Promise<SwapRecord[]> {
  return deps.db.getSwapsByAccount(account);
}

// 绑定 tx hash 到 swapId。客户端回传（快路径）和链上 watcher（兜底）都走这里，幂等。
async function bindTxHash(swapId: string, txHash: string, deps: Deps): Promise<void> {
  await deps.db.setProviderRef(swapId, txHash);
}

// 兜底：对“已 execute、要用户自己签名广播、但还没拿到 hash”的 swap，
// 盯住用户地址的出账，匹配到发往 expectedTo 的交易就绑定 hash。
// 这样 App 一广播就关、永不回传，也能继续跟踪。
async function watchBroadcasts(deps: Deps): Promise<void> {
  for (;;) {
    const unbound = await listUnbound(); // kind=unsigned_tx 且 providerRef 为空
    for (const rec of unbound) {
      const tx = await findOutgoingTx(rec.userAddress, rec.chainId, rec.expectedTo, rec.nonce);
      if (tx) await bindTxHash(rec.swapId, tx.hash, deps); // 绑定后进入正常轮询/Webhook
    }
    await sleep(pollIntervalFor(unbound));
  }
}

async function onChannelWebhook(channelId: string, payload: unknown, deps: Deps): Promise<void> {
  const adapter = deps.registry.get(channelId)!;
  if (!adapter.parseWebhook) return;
  const status = adapter.parseWebhook(payload);
  await deps.db.saveStatus(status);
}

// 对未完成的跨链/订单式 swap 定期拉取；源已确认但久不到账则升级告警
async function pollLoop(deps: Deps): Promise<void> {
  for (;;) {
    const pending = await listUnfinished(); // code in (PENDING, SOURCE_CONFIRMED)
    for (const rec of pending) {
      if (!rec.providerRef) continue; // 还没绑定 hash 的交给 watchBroadcasts
      const adapter = deps.registry.get(rec.channelId)!;
      const s = await adapter.getStatus({ channelId: rec.channelId, providerRef: rec.providerRef });
      await deps.db.saveStatus(s);
      if (s.code === "SOURCE_CONFIRMED" && overdue(rec, s)) await raiseDelayedAlert(rec, s);
    }
    await sleep(pollIntervalFor(pending));
  }
}

declare function isFresh(s?: SwapStatus): boolean;
declare function listUnfinished(): Promise<SwapRecord[]>;
declare function listUnbound(): Promise<SwapRecord[]>; // kind=unsigned_tx 且无 providerRef
declare function findOutgoingTx(addr: string, chainId: ChainId, to: string | undefined, nonce: number | undefined): Promise<{ hash: string } | null>;
declare function overdue(rec: SwapRecord, s: SwapStatus): boolean; // now - sourceConfirmedAt > k * etaSeconds
declare function raiseDelayedAlert(rec: SwapRecord, s: SwapStatus): Promise<void>;
declare function sleep(ms: number): Promise<void>;
declare function pollIntervalFor(pending: SwapRecord[]): number;

export { getSwapStatus, listSwapsByAccount, bindTxHash, watchBroadcasts, onChannelWebhook, pollLoop };
