import type { SwapStatus } from "./types";
import type { ChannelRegistry } from "./registry";

// 状态跟踪：Webhook 优先，轮询兜底。跨链分别暴露源腿/目标腿。

interface SwapRecord { swapId: string; channelId: string; providerRef: string; lastStatus?: SwapStatus; }

interface Deps {
  registry: ChannelRegistry;
  db: { getSwap(swapId: string): Promise<SwapRecord | null>; saveStatus(s: SwapStatus): Promise<void> };
}

async function getSwapStatus(swapId: string, deps: Deps): Promise<SwapStatus | { code: "NOT_FOUND" }> {
  const rec = await deps.db.getSwap(swapId);
  if (!rec) return { code: "NOT_FOUND" };

  if (isFresh(rec.lastStatus)) return rec.lastStatus!;
  const adapter = deps.registry.get(rec.channelId)!;
  const fresh = await adapter.getStatus({ channelId: rec.channelId, providerRef: rec.providerRef });
  await deps.db.saveStatus(fresh);
  return fresh;
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
declare function overdue(rec: SwapRecord, s: SwapStatus): boolean; // now - sourceConfirmedAt > k * etaSeconds
declare function raiseDelayedAlert(rec: SwapRecord, s: SwapStatus): Promise<void>;
declare function sleep(ms: number): Promise<void>;
declare function pollIntervalFor(pending: SwapRecord[]): number;

export { getSwapStatus, onChannelWebhook, pollLoop };
