declare namespace Deno {
  const args: string[];

  function readTextFile(path: string | URL): Promise<string>;
  function writeTextFile(path: string | URL, data: string): Promise<void>;

  namespace errors {
    class NotFound extends Error {}
  }
}

declare module "npm:yahoo-finance2" {
  export default class YahooFinance {
    constructor(options?: unknown);

    quote(symbol: string): Promise<unknown>;
    quote(symbols: string[]): Promise<unknown>;
    quoteSummary(symbol: string, options: { modules: string[] }): Promise<Record<string, unknown>>;
    historical(
      symbol: string,
      options: { period1: Date; period2: Date; interval: string },
    ): Promise<unknown[]>;
  }
}