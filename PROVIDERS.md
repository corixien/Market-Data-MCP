# Data providers

Verified live on the deployed server 2026-10-05. Rate limits are the providers' published free-tier numbers (not enforced in code); re-check before relying on them.

## In use

| Provider | Access | Secret (Replit) | Delay | Free-tier limits | Data used for |
|---|---|---|---|---|---|
| CoinGecko | REST `api.coingecko.com/api/v3/simple/price`, header `x-cg-demo-api-key` | `COINGECKO_API_KEY` (optional) | about 1 min | Keyless about 30 calls/min; demo key 30-100 calls/min, 10k calls/month | Crypto quotes (`COINGECKO_CRYPTO_IDS` in `main.py`): price, 24h chg |
| Finnhub | REST `finnhub.io/api/v1/quote`, `token` param | `FINNHUB_API_KEY` | 0 (live) | 60 calls/min, no daily cap, US only | Dow 30, Nasdaq 100, DIA/QQQ/SPY, index proxies: price, chg, prev close, day high/low |
| Alpaca | Websocket `wss://stream.data.alpaca.markets/v2/iex` (trades) + REST snapshot for prev close | `ALPACA_API_KEY`, `ALPACA_API_SECRET` | 0 (live, IEX feed) | 1 websocket connection, 30 symbols (code evicts LRU), REST 200 calls/min, REST data 15 min late | Other US tickers (`US_EQUITY_RE`): price, chg, prev close. No day high/low/volume |
| yfinance | Python lib (Yahoo, unofficial, no key) | none | declared 15 min; observed 1-3 min | No official limit; IP throttling possible under heavy use | Everything else and fallback: history, indicators, fundamentals, options, news, non-US, forex, futures, indices |

Routing (`_available_providers` in `main.py`): least declared delay first. Crypto -> CoinGecko; else Finnhub (listed symbols) then Alpaca (US tickers); yfinance always last. Failed providers are listed in `fallback_from`. Quotes cache 30 s, so repeated calls cost nothing.

Burst warning: `get_quote` with up to 50 Finnhub symbols fires up to 50 calls at once (8 threads), close to the 60/min cap. Beyond that Finnhub fails and yfinance answers.

All history, indicators, fundamentals, options and news come from yfinance only.

## Deka funds, Deka ETFs, bonds, commodity ETCs

Tested with yfinance 2026-10-05:
- Deka ETFs on Xetra: work. Example `EL4A.DE` (Deka DAX UCITS ETF), 1 min bars. `yf.Search("<ISIN>")` resolves an ISIN to a ticker (`DE000ETFL011` -> `EL4A.DE`).
- Commodity ETCs: work. `4GLD.DE` (Xetra-Gold, 1 min bars), `IGLN.L`. The full 215-ETC list is not tested. Resolve each by ISIN with `yf.Search`, then check `.DE` first (live Xetra), then `.L`.
- Active Deka mutual funds (for example DEKAFONDS `DE0008474503` -> `OG7T.F`): priced once per day (NAV). No intraday data exists at any provider. Yahoo gives it with delay via `.F`, `.MU`, `.HM` listings.
- Bonds: no free source for individual bond prices. Use bond ETFs (same path as ETFs) or yields (`^TNX`).
- Free live Xetra data does not exist. `live.deutsche-boerse.com` is web only; no free API. Yahoo is the lowest-delay free option.

## Free providers considered for adding

| Provider | Free limits | Verdict |
|---|---|---|
| Twelve Data | 800 credits/day, 8/min, US stocks/ETFs/forex/crypto realtime; Europe is paid; ISIN `symbol_search` free | Optional: ISIN lookup and forex backup |
| FMP | 250 calls/day, US/CA/EU, mostly end of day | Optional: backup for Europe EOD |
| EODHD | 20 calls/day | Too small |
| Tiingo | 20k requests/day, US equities | Redundant with Alpaca |
| Alpha Vantage | 25 calls/day, 5/min | Too small |
| Stooq | CSV now needs an API key (since 2026-03) | Skip |

Recommendation: add none now. Biggest gain for Deka/ETC coverage is an ISIN -> ticker resolver tool in this server on top of `yf.Search` (not built; needs a go-ahead).

Sources: finnhub.io docs, docs.alpaca.markets (market data), coingecko.com/en/api/pricing, twelvedata.com/pricing, eodhd.com/pricing.
