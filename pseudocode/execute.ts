import type { ExecuteResult, Quote } from "./types";
import type { ChannelRegistry } from "./registry";
import type { DepositOrderAdapter } from "./adapters";

// 执行选中的报价。DEX/桥返回未签名交易；订单式创建订单，走幂等避免重复下单。

interface ExecuteRequest { quoteId: string; idempotencyKey: string; recipient: string; userAddress: string; }

type IntentState = "PENDING" | "CREATED" | "UNKNOWN";
interface IntentRecord { key: string; state: IntentState; channelId: string; providerRef?: string; result?: ExecuteResult; }

interface Deps {
  registry: ChannelRegistry;
  quoteStore: { get(quoteId: string): Promise<Quote | null> };
  idemp: IdempotencyStore;
}

async function execute(req: ExecuteRequest, deps: Deps): Promise<ExecuteOutcome> {
  const quote = await deps.quoteStore.get(req.quoteId);
  if (!quote) return { http: 404, body: { code: "QUOTE_NOT_FOUND", quoteId: req.quoteId } };
  if (Date.now() > quote.expiresAt)
    return { http: 409, body: { code: "QUOTE_EXPIRED", quoteId: req.quoteId, hint: "re-quote" } };

  const adapter = deps.registry.get(quote.channelId)!;
  const kind = adapter.capabilities().executeKind;

  // DEX/桥：无服务端副作用，幂等交给钱包 nonce，直接构建交易
  if (kind === "unsigned_tx") {
    const res = await adapter.execute(quote, {
      idempotencyKey: req.idempotencyKey, clientOrderRef: req.idempotencyKey, recipient: req.recipient,
    });
    return { http: 200, body: res };
  }

  // 订单式：有副作用，走幂等
  return createOrderIdempotent(req, quote, adapter as unknown as DepositOrderAdapter, deps);
}

async function createOrderIdempotent(
  req: ExecuteRequest, quote: Quote, adapter: DepositOrderAdapter, deps: Deps,
): Promise<ExecuteOutcome> {
  const key = req.idempotencyKey;

  // 原子占位，重复确认会命中已有记录
  const inserted = await deps.idemp.insertIfAbsent({ key, state: "PENDING", channelId: quote.channelId });

  if (!inserted) {
    const rec = await deps.idemp.get(key);
    if (rec.state === "CREATED") return { http: 200, body: rec.result }; // 返回同一订单
    if (rec.state === "PENDING") return { http: 202, body: { code: "PROCESSING", key } };
    return reconcile(key, adapter, deps); // UNKNOWN
  }

  try {
    const res = await adapter.execute(quote, {
      idempotencyKey: key, clientOrderRef: key, recipient: req.recipient,
    });
    await deps.idemp.update(key, { state: "CREATED", providerRef: orderRefOf(res), result: res });
    return { http: 200, body: res };
  } catch (e) {
    if (isTimeout(e)) {
      // 超时=结果未知，不重试创建，交给对账
      await deps.idemp.update(key, { state: "UNKNOWN" });
      return reconcile(key, adapter, deps);
    }
    await deps.idemp.remove(key); // 明确失败，释放占位允许重试
    return { http: 502, body: { code: "CHANNEL_ERROR" } };
  }
}

// 用 clientOrderRef 向上游确认订单到底建没建
async function reconcile(
  key: string, adapter: DepositOrderAdapter, deps: Deps,
): Promise<ExecuteOutcome> {
  if (adapter.capabilities().supportsClientOrderRef) {
    const found = await adapter.findByClientRef(key);
    if (found) {
      const res = await adoptOrder(adapter, found.id);
      await deps.idemp.update(key, { state: "CREATED", providerRef: found.id, result: res });
      return { http: 200, body: res };
    }
    await deps.idemp.remove(key);
    return { http: 409, body: { code: "NOT_CREATED_RETRY", key } };
  }
  // 上游无幂等令牌：入对账队列（按金额+时间窗+地址匹配），期间返回处理中，不盲目重建
  await deps.idemp.enqueueReconcile(key);
  return { http: 202, body: { code: "PROCESSING_RECONCILING", key } };
}

interface ExecuteOutcome { http: number; body: unknown; }
interface IdempotencyStore {
  insertIfAbsent(rec: IntentRecord): Promise<boolean>; // true=首发，false=已存在
  get(key: string): Promise<IntentRecord>;
  update(key: string, patch: Partial<IntentRecord>): Promise<void>;
  remove(key: string): Promise<void>;
  enqueueReconcile(key: string): Promise<void>;
}
declare function isTimeout(e: unknown): boolean;
declare function orderRefOf(r: ExecuteResult): string;
declare function adoptOrder(a: DepositOrderAdapter, orderId: string): Promise<ExecuteResult>;

export { execute };
