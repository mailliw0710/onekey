import type {
  ChannelAdapter, ChannelCapability, ChannelRef, ExecuteCtx, ExecuteResult,
  Quote, QuoteRequest, SwapStatus,
} from "./types";

// 三类渠道适配：
//   DexAdapter          同链 DEX，返回未签名交易
//   BridgeAdapter       跨链桥，返回源链未签名交易，状态分源/目标两腿
//   DepositOrderAdapter 先下单拿收款地址，用户再转入（有服务端副作用，需幂等）

class DexAdapter implements ChannelAdapter {
  constructor(readonly id: string, private cfg: ProviderCfg) {}

  capabilities(): ChannelCapability {
    return {
      id: this.id, kinds: ["same_chain"], enabled: this.cfg.enabled,
      executeKind: "unsigned_tx", supportsClientOrderRef: false,
      defaultSlippageBps: 50, softTimeoutMs: 1500,
      supportsChain: (c) => this.cfg.chains.includes(c),
      supportsPair: (f, t) => f.chainId === t.chainId,
    };
  }

  async quote(req: QuoteRequest, signal: AbortSignal): Promise<Quote> {
    const r = await httpGet(this.cfg.url + "/quote", {
      sellToken: req.from.address, buyToken: req.to.address,
      sellAmount: req.amount.amount, slippageBps: req.slippageBps ?? 50,
    }, signal);
    return normalizeQuote(this.id, "same_chain", req, {
      toAmount: r.buyAmount, minReceived: r.guaranteedBuyAmount,
      gas: r.estimatedGas, protocolFee: r.feeAmount, etaSeconds: 15, raw: r,
    });
  }

  async execute(q: Quote, _ctx: ExecuteCtx): Promise<ExecuteResult> {
    const raw = q.raw as any;
    const tx = await httpGet(this.cfg.url + "/swap-tx", { quoteRef: raw.id });
    return {
      kind: "unsigned_tx",
      swapId: newSwapId(this.id),
      tx: { chainId: q.fromChainId, to: tx.to, data: tx.data, value: tx.value, gas: tx.gas },
    };
  }

  async getStatus(ref: ChannelRef): Promise<SwapStatus> {
    const rc = await getTxReceipt(ref.providerRef);
    return toStatus(this.id, ref, rc);
  }
}

class BridgeAdapter implements ChannelAdapter {
  constructor(readonly id: string, private cfg: ProviderCfg) {}

  capabilities(): ChannelCapability {
    return {
      id: this.id, kinds: ["cross_chain", "same_chain"], // 如 LI.FI 两者皆可
      enabled: this.cfg.enabled, executeKind: "unsigned_tx",
      supportsClientOrderRef: false, defaultSlippageBps: 50, softTimeoutMs: 1800,
      supportsChain: (c) => this.cfg.chains.includes(c),
      supportsPair: () => true,
    };
  }

  async quote(req: QuoteRequest, signal: AbortSignal): Promise<Quote> {
    const r = await httpPost(this.cfg.url + "/routes", {
      fromChain: req.from.chainId, toChain: req.to.chainId,
      fromToken: req.from.address, toToken: req.to.address,
      fromAmount: req.amount.amount, toAddress: req.recipient ?? req.userAddress,
    }, signal);
    const best = r.routes[0];
    return normalizeQuote(this.id, "cross_chain", req, {
      toAmount: best.toAmount, minReceived: best.toAmountMin,
      gas: best.gasCost, protocolFee: best.feeCost, etaSeconds: best.estimatedDurationSec,
      raw: best,
    });
  }

  async execute(q: Quote, _ctx: ExecuteCtx): Promise<ExecuteResult> {
    const best = q.raw as any;
    const tx = await httpPost(this.cfg.url + "/build", { routeId: best.id });
    return {
      kind: "unsigned_tx",
      swapId: newSwapId(this.id),
      tx: { chainId: q.fromChainId, to: tx.to, data: tx.data, value: tx.value },
    };
  }

  async getStatus(ref: ChannelRef): Promise<SwapStatus> {
    const s = await httpGet(this.cfg.url + "/status", { txHash: ref.providerRef });
    return {
      swapId: ref.providerRef, channelId: this.id,
      code: s.status === "DONE" ? "COMPLETED"
          : s.sending?.confirmed ? "SOURCE_CONFIRMED" : "PENDING",
      detail: s.substatus,
      sourceTx: s.sending ? { chainId: s.sending.chainId, hash: s.sending.txHash, confirmations: s.sending.confirmations } : null,
      destTx: s.receiving?.txHash ? { chainId: s.receiving.chainId, hash: s.receiving.txHash } : null,
      updatedAt: Date.now(),
    };
  }
}

class DepositOrderAdapter implements ChannelAdapter {
  constructor(readonly id: string, private cfg: ProviderCfg) {}

  capabilities(): ChannelCapability {
    return {
      id: this.id, kinds: ["cross_chain", "same_chain"],
      enabled: this.cfg.enabled, executeKind: "order",
      supportsClientOrderRef: this.cfg.supportsClientRef,
      defaultSlippageBps: 0, softTimeoutMs: 2000,
      supportsChain: (c) => this.cfg.chains.includes(c),
      supportsPair: () => true,
    };
  }

  async quote(req: QuoteRequest, signal: AbortSignal): Promise<Quote> {
    const r = await httpGet(this.cfg.url + "/estimate", {
      from: req.from.symbol, to: req.to.symbol, amount: req.amount.amount,
    }, signal);
    const type = req.from.chainId === req.to.chainId ? "same_chain" : "cross_chain";
    return normalizeQuote(this.id, type, req, {
      toAmount: r.estimatedAmount, minReceived: r.minAmount,
      gas: "0", protocolFee: r.providerFee, etaSeconds: r.etaSeconds, raw: r,
    });
  }

  async execute(q: Quote, ctx: ExecuteCtx): Promise<ExecuteResult> {
    const r = await httpPost(this.cfg.url + "/order", {
      from: q.from.token, to: q.to.token, amount: q.from.amount,
      payoutAddress: ctx.recipient,
      clientOrderRef: this.cfg.supportsClientRef ? ctx.clientOrderRef : undefined,
    });
    return {
      kind: "order",
      swapId: newSwapId(this.id),
      order: {
        orderId: r.id, depositAddress: r.payinAddress,
        depositAmount: { token: q.from.token, decimals: q.from.decimals, amount: r.amountExpectedFrom },
        depositMemo: r.payinExtraId, expiresAt: r.expiresAt,
      },
    };
  }

  // 按幂等令牌反查订单，用于对账判断是否已创建
  async findByClientRef(clientOrderRef: string): Promise<{ id: string } | null> {
    if (!this.cfg.supportsClientRef) return null;
    const r = await httpGet(this.cfg.url + "/orders", { clientOrderRef });
    return r.items[0] ?? null;
  }

  async getStatus(ref: ChannelRef): Promise<SwapStatus> {
    const s = await httpGet(this.cfg.url + "/order", { id: ref.providerRef });
    const map: Record<string, SwapStatus["code"]> = {
      waiting: "PENDING", confirming: "SOURCE_CONFIRMED", exchanging: "SOURCE_CONFIRMED",
      sending: "SOURCE_CONFIRMED", finished: "COMPLETED", failed: "FAILED", refunded: "REFUNDED",
    };
    return {
      swapId: ref.providerRef, channelId: this.id,
      code: map[s.status] ?? "PENDING", detail: s.status,
      sourceTx: s.payinHash ? { chainId: s.fromChain, hash: s.payinHash } : null,
      destTx: s.payoutHash ? { chainId: s.toChain, hash: s.payoutHash } : null,
      updatedAt: Date.now(),
    };
  }
}

interface ProviderCfg { url: string; enabled: boolean; chains: (number | string)[]; supportsClientRef: boolean; }
declare function httpGet(url: string, params?: any, signal?: AbortSignal): Promise<any>;
declare function httpPost(url: string, body?: any, signal?: AbortSignal): Promise<any>;
declare function getTxReceipt(hash: string): Promise<any>;
declare function toStatus(channelId: string, ref: ChannelRef, receipt: any): SwapStatus;
// normalizeQuote 用 req.from.chainId / req.to.chainId 填充 fromChainId / toChainId
declare function normalizeQuote(channelId: string, type: any, req: QuoteRequest, x: any): Quote;
declare function newSwapId(channelId: string): string;

export { DexAdapter, BridgeAdapter, DepositOrderAdapter };
