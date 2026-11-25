import type { ChannelAdapter, QuoteRequest, SwapType } from "./types";

// 渠道注册表。接新渠道只需 register 一个 Adapter，聚合/执行/查状态都不改。
class ChannelRegistry {
  private adapters = new Map<string, ChannelAdapter>();

  register(adapter: ChannelAdapter): void {
    this.adapters.set(adapter.id, adapter);
  }

  get(channelId: string): ChannelAdapter | undefined {
    return this.adapters.get(channelId);
  }

  // 按网络 / 币对 / 启用状态 / 能力筛出能报价的渠道
  eligible(req: QuoteRequest): ChannelAdapter[] {
    const wantType: SwapType =
      req.from.chainId === req.to.chainId ? "same_chain" : "cross_chain";

    return [...this.adapters.values()].filter((a) => {
      const cap = a.capabilities();
      return (
        cap.enabled &&
        cap.kinds.includes(wantType) &&
        cap.supportsChain(req.from.chainId) &&
        cap.supportsChain(req.to.chainId) &&
        cap.supportsPair(req.from, req.to)
      );
    });
  }
}

// 启动装配示意：
//   registry.register(new DexAdapter("1inch", cfg1inch));
//   registry.register(new BridgeAdapter("lifi", cfgLifi));
//   registry.register(new DepositOrderAdapter("changelly", cfgCy));

export { ChannelRegistry };
