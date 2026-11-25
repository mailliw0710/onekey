import type { ChannelAdapter, Quote, QuoteRequest } from "./types";
import type { ChannelRegistry } from "./registry";

// 并发询价：先到先推，单渠道独立超时，到本轮截止推 round_complete。

type QuoteEvent =
  | { event: "quote"; data: Quote }
  | { event: "round_complete"; data: RoundSummary }
  | { event: "late_quote"; data: Quote };

interface RoundSummary {
  roundId: string;
  byChannel: Record<string, "answered" | "timeout" | "error" | "skipped_open_circuit">;
  answeredCount: number;
}

interface AggregatorDeps {
  registry: ChannelRegistry;
  breaker: CircuitBreaker;
  bulkhead: Bulkhead;
  metrics: Metrics;
}

const ROUND_DEADLINE_MS = 2500; // 大于 2s SLO，给首个报价留余量
const LATE_GRACE_MS = 1500;
const ENABLE_LATE_QUOTES = true; // 关掉则迟到报价本轮不展示，由客户端下一轮捞回

async function* streamQuotes(
  req: QuoteRequest,
  deps: AggregatorDeps,
): AsyncGenerator<QuoteEvent> {
  const roundId = newId();
  const t0 = now();
  let firstQuoteEmitted = false;

  const channels = deps.registry.eligible(req);
  const summary: RoundSummary = { roundId, byChannel: {}, answeredCount: 0 };
  if (channels.length === 0) {
    yield { event: "round_complete", data: summary };
    return;
  }

  const inbox = new AsyncQueue<QuoteEvent | DoneMarker>();
  let pending = channels.length;
  const markDone = () => { if (--pending === 0) inbox.push(DONE); };

  for (const ch of channels) {
    void askOneChannel(ch, req, deps, roundId).then(
      (q) => {
        summary.byChannel[ch.id] = "answered";
        summary.answeredCount++;
        inbox.push({ event: "quote", data: q });
        markDone();
      },
      (err: QuoteError) => {
        summary.byChannel[ch.id] = err.reason;
        deps.metrics.incChannel(ch.id, err.reason);
        markDone();
      },
    );
  }

  void delay(ROUND_DEADLINE_MS).then(() => inbox.push(DONE));

  while (true) {
    const item = await inbox.take();
    if (item === DONE) break;
    if (item.event === "quote" && !firstQuoteEmitted) {
      firstQuoteEmitted = true;
      deps.metrics.observeTTFQ(now() - t0); // TTFQ，SLO 的度量口径
    }
    yield item;
  }

  for (const ch of channels)
    if (!summary.byChannel[ch.id]) summary.byChannel[ch.id] = "timeout";
  yield { event: "round_complete", data: summary };

  if (ENABLE_LATE_QUOTES) {
    const graceEnd = now() + LATE_GRACE_MS;
    while (now() < graceEnd) {
      const item = await inbox.takeUntil(graceEnd);
      if (item && item !== DONE && item.event === "quote")
        yield { event: "late_quote", data: item.data };
      else break;
    }
  }
}

// 熔断 -> 舱壁限流 -> 带超时调用 -> 归一化
async function askOneChannel(
  ch: ChannelAdapter,
  req: QuoteRequest,
  deps: AggregatorDeps,
  roundId: string,
): Promise<Quote> {
  if (deps.breaker.isOpen(ch.id))
    throw { reason: "skipped_open_circuit" } as QuoteError;

  return deps.bulkhead.run(ch.id, async () => {
    const soft = ch.capabilities().softTimeoutMs ?? 1800;
    const ac = new AbortController();
    const timer = delay(soft).then(() => ac.abort());
    try {
      const q = await ch.quote(req, ac.signal);
      validateAndStamp(q, ch.id, roundId); // 校验金额、生成 quoteId、写 Quote Store
      deps.breaker.onSuccess(ch.id);
      return q;
    } catch (e) {
      deps.breaker.onFailure(ch.id);
      throw { reason: ac.signal.aborted ? "timeout" : "error", cause: e } as QuoteError;
    } finally {
      void timer;
    }
  });
}

interface QuoteError { reason: "timeout" | "error" | "skipped_open_circuit"; cause?: unknown; }
interface CircuitBreaker { isOpen(id: string): boolean; onSuccess(id: string): void; onFailure(id: string): void; }
interface Bulkhead { run<T>(id: string, fn: () => Promise<T>): Promise<T>; } // 每渠道独立信号量
interface Metrics { observeTTFQ(ms: number): void; incChannel(id: string, reason: string): void; }
const DONE = Symbol("done"); type DoneMarker = typeof DONE;
declare function validateAndStamp(q: Quote, channelId: string, roundId: string): void;
declare function newId(): string;
declare function now(): number;
declare function delay(ms: number): Promise<void>;
declare class AsyncQueue<T> { push(x: T): void; take(): Promise<T>; takeUntil(deadline: number): Promise<T | null>; }

export { streamQuotes };
export type { QuoteEvent, RoundSummary };
