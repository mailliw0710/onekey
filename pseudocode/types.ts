// 统一数据模型 + 渠道适配接口。
// 金额统一用最小单位整数字符串 + decimals，避免浮点精度问题。

type ChainId = number | string; // EVM 用 number，Solana 等用 string

interface Asset {
  chainId: ChainId;
  symbol: string;
  address: string | "native";
  decimals: number;
}

interface Amount {
  token: string;
  decimals: number;
  amount: string; // 最小单位整数，如 "1000000000000000000" = 1 ETH
}

interface QuoteRequest {
  from: Asset;
  to: Asset;
  amount: Amount;
  userAddress: string;
  recipient?: string; // 跨链/订单式收款地址，默认同 userAddress
  slippageBps?: number;
}

type SwapType = "same_chain" | "cross_chain";

interface Fee {
  network?: Amount; // 源链 gas
  protocol?: Amount; // 渠道费
  service?: Amount & { rateBps: number }; // 平台服务费
  usdTotal: string; // 折 USD，用于排序比较
}

interface Quote {
  quoteId: string;
  channelId: string;
  type: SwapType;
  fromChainId: ChainId;
  toChainId: ChainId;
  from: Amount;
  to: Amount;
  minReceived: Amount;
  slippageBps: number;
  fees: Fee;
  etaSeconds: number;
  expiresAt: number; // epoch ms
  raw: unknown; // 渠道私有回执，execute 时原样回传
}

type ExecuteResult =
  | { kind: "unsigned_tx"; swapId: string; tx: UnsignedTx }
  | { kind: "order"; swapId: string; order: DepositOrder };

interface UnsignedTx {
  chainId: ChainId;
  to: string;
  data: string;
  value: string;
  gas?: string;
}

interface DepositOrder {
  orderId: string;
  depositAddress: string;
  depositAmount: Amount;
  depositMemo?: string;
  expiresAt: number;
}

type SwapStatusCode =
  | "PENDING"
  | "SOURCE_CONFIRMED" // 跨链：源链已确认，目标链未到账
  | "COMPLETED" // 同链完成 / 目标链已到账 / 订单已打款
  | "FAILED"
  | "REFUNDED";

interface SwapStatus {
  swapId: string;
  channelId: string;
  code: SwapStatusCode;
  detail: string; // 渠道细态
  sourceTx?: { chainId: ChainId; hash: string; confirmations?: number } | null;
  destTx?: { chainId: ChainId; hash: string } | null;
  updatedAt: number;
}

interface ChannelCapability {
  id: string;
  kinds: SwapType[]; // 同一渠道可同时支持同链与跨链
  supportsChain(chainId: ChainId): boolean;
  supportsPair(from: Asset, to: Asset): boolean;
  enabled: boolean;
  executeKind: "unsigned_tx" | "order";
  supportsClientOrderRef: boolean; // 是否支持幂等令牌，决定对账策略
  defaultSlippageBps: number;
  softTimeoutMs: number;
}

interface ExecuteCtx {
  idempotencyKey: string;
  clientOrderRef: string;
  recipient: string;
}

interface ChannelRef {
  channelId: string;
  providerRef: string;
}

interface ChannelAdapter {
  readonly id: string;
  capabilities(): ChannelCapability;
  quote(req: QuoteRequest, signal: AbortSignal): Promise<Quote>;
  execute(q: Quote, ctx: ExecuteCtx): Promise<ExecuteResult>;
  getStatus(ref: ChannelRef): Promise<SwapStatus>;
  parseWebhook?(payload: unknown): SwapStatus;
}

export type {
  ChainId, Asset, Amount, QuoteRequest, SwapType, Fee, Quote,
  ExecuteResult, UnsignedTx, DepositOrder, SwapStatusCode, SwapStatus,
  ChannelCapability, ExecuteCtx, ChannelRef, ChannelAdapter,
};
