# Project Resume

> Instruction for Claude: read this whole file first. Then read every file under "Must read before any change". Do not modify anything until you have fully understood the project. Keep this file updated as you work (see "Maintenance rules").

Last updated: 2026-10-06

## 1. What this project is
Read-only market-data MCP server (Python 3.12, FastMCP 4). Exposes trading/analysis tools over Streamable HTTP at `/mcp`. Backed by yfinance, with live quote providers (CoinGecko, Finnhub, Alpaca websocket) and yfinance fallback. Hosted on Prefect Horizon (free, deploys from GitHub `main`, entrypoint `main.py:mcp`, OAuth enforced); previously Replit/Cloud Run (`.replit`, still works via `python main.py`). Consumed by Claude (the `mbs-agent`/`trading-agent` skills use it as the "yfinance-market-data" MCP server). No account/broker access.

## 2. Must read before any change
1. `.agents/memory/MEMORY.md` - index of design memories; read each linked file (they record why things are as they are).
2. `.agents/memory/compact-output-and-setup-tuning.md` - backtest results behind `SETUP_PARAMS`; score is not a probability; shorts opt-in.
3. `.agents/memory/multi-source-quote-routing.md` - provider priority and response fields.
4. `.agents/memory/mcp-market-data-compatibility.md` - tool names and arg order must stay stable.
5. `.agents/memory/python-publishing-runtime.md` - deploy/runtime constraints.
6. `PROVIDERS.md` - provider table: access, secrets, delay, limits, coverage, Deka/ETC findings.
7. `main.py` - server, tool registration, quote routing, setup/scan logic (about 1700 lines).
8. `market_data.py` - history fetch, cache, indicators.
9. `alpaca_stream.py` - Alpaca websocket trade stream.

## 3. How to navigate
- `main.py` - entry point (`uvicorn.run(app)`, port from `PORT`, default 5000). `@market_tool` decorator registers tools (compact JSON text output).
- `market_data.py` - validation, TTL cache, `get_history`, `download_batch`, `compact_history`, indicators (`sma/ema/rsi/atr`, `indicator_snapshot`, `bull_score`, `swing_levels`, `stats`).
- `alpaca_stream.py` - singleton `stream`; daemon thread, lazy per-symbol subscribe, latest trade in memory; `configured()` checks env keys.
- `attached_assets/` - pasted Replit prompts/logs; reference only.
- `.agents/memory/` - agent memory notes (tracked in git).
- Routes: `/` text homepage, `/healthz` health JSON, `/mcp` MCP transport (DNS-rebinding protection intentionally disabled for Replit proxy).
- Tools in `main.py`: `get_price`, `get_quote`, `get_historical_data`, `get_batch_historical_data`, `get_analysis`, `get_trade_setup`, `scan_watchlist`, `market_snapshot`, `compare`, `get_fundamentals_brief`, `get_events`, `get_options_brief`, `get_news_brief`, `position_calc`, `position_size`.
- Lookup:
  - Quote provider routing -> `_available_providers`, `_quote_base`, `PROVIDER_DELAYS` (main.py)
  - Trade setup logic/params -> `SETUP_STYLES`, `SETUP_PARAMS`, `_build_setup`
  - Scan fields/universes -> `SCAN_FIELDS`, `UNIVERSES`, `scan_watchlist`
  - Indicators -> `market_data.indicator_snapshot`
  - interval/period legacy-order handling -> `_history_args`
- Data flow: tool call -> `market_tool` wrapper (error handling, compact JSON) -> provider/yfinance fetch (cached in `market_data`) -> indicator calc -> dict with `as_of`/`delayed`/source.

## 4. Commands
Run from project root.
- Install: `pip install -r requirements.txt` (yfinance, fastmcp 4, pandas, websockets).
- Check Horizon compatibility: `fastmcp inspect main.py:mcp`.
- Run: `python main.py` (use `python`, not `python3`, for Cloud Run).
- Health: `curl localhost:5000/healthz`.
- No tests, linter, or CI in repo.
- Env vars (set in the Horizon server settings or Replit secrets, never in repo): `ALPACA_API_KEY`, `ALPACA_API_SECRET`, `ALPACA_STREAM_URL` (optional), `FINNHUB_API_KEY`, `COINGECKO_API_KEY`, `PORT`.
- Deploy: push to `main`; Horizon redeploys automatically. Set the secrets in Horizon, entrypoint `main.py:mcp`. Retest `/mcp` after each deploy.

## 5. Invariants and gotchas
- Server object must stay a module-level `FastMCP` named `mcp` in `main.py` (Horizon requirement). Tool errors raise `ToolError` (isError true). Binance is not a provider; `BNB` in `COINGECKO_CRYPTO_IDS` is only a CoinGecko coin id.
- Keep original tool names and `/mcp` HTTP transport; accept both (interval, period) and legacy (period, interval) order.
- `_history_args` swaps by span comparison, not set membership ("1d"/"3mo" valid as both).
- Outputs are compact JSON text, no indent, no structured copy (token savings).
- Trade-setup score = rule checklist, not win probability. Do not change `SETUP_PARAMS` without re-running a backtest. Shorts opt-in (`shorts=true`). `style=intraday` returns levels only unless `force=true`.
- Quote provider choice = lowest declared delay; ties CoinGecko, Finnhub, Alpaca; yfinance last/fallback. Preserve `data_source`, `delay_minutes`, `timestamp`. Never log credentials or URLs with tokens.
- `fallback_from` in a quote = providers that failed before yfinance answered. Missing Alpaca in `data_source` means check Deployment secrets + republish.
- Alpaca REST is >=15 min delayed; only websocket (IEX) is live. US equity tickers only.
- Yahoo 1m history limited to about 1 week.
- `.gitignore` covers `.pythonlibs/`, `__pycache__/`, `*.pyc`, `.pytest_cache/`.

## Providers (summary, details in PROVIDERS.md)
| Provider | Delay | Covers |
|---|---|---|
| CoinGecko | ~1 min | crypto |
| Finnhub | 0 | Dow 30, Nasdaq 100, SPY/QQQ/DIA |
| Alpaca (IEX websocket) | 0 | other US tickers |
| yfinance | 1-15 min | all else, all history/indicators |
Deka ETFs and ETCs work through yfinance (`.DE`, `.L`; ISIN via `yf.Search`). Active Deka funds: daily NAV only. No free live Xetra source exists.

## 6. Current state
- Branch / last commit: `main`, `1baf7c7` "Fix fallback_from not reaching yfinance quote responses".
- Working tree: clean after committing this file.
- Goal of the current task: provider test done. All four providers verified on the deployed server (2026-10-05).

### Done
- [x] Compact output, fixed tools, backtested setups (`1de13ff`)
- [x] `skip_downtrend` scan pre-filter (`d403a95`)
- [x] EMA 5/13/50/200, Bollinger, `ema_stack`, `bb_pos` scan field (`17887fd`)
- [x] Alpaca websocket quotes + least-delay routing (`180a19a`; `alpaca_stream.py`, `main.py`)
- [x] Alpaca subscription cap 30 (LRU) + `fallback_from` response field (`9aaf3cd`, `1baf7c7`)
- [x] Migrated MCP layer from `mcp.server.MCPServer` to `FastMCP` for Prefect Horizon (`main.py`, `requirements.txt`)
- [x] `PROVIDERS.md` written (limits, access, Deka/ETC research)
- [x] Provider test: CoinGecko/Finnhub/Alpaca delay 0 or ~1 min, yfinance fallback. Alpaca failed earlier only because Replit Deployment was stale/lacked secrets; fixed by republish.

### In progress
- (none)

### Next steps
1. Await user go-ahead for an ISIN -> ticker resolver tool (`yf.Search`) for Deka ETFs and the 215 ETCs; needs the ETC ISIN list.

### Open questions / blockers
- Alpaca IEX quotes lack day_high/day_low/volume. Decision: leave unfilled (IEX volume is partial, extra yfinance call adds latency).

## 7. Decisions log
- 2026-10-06 - moved hosting to Prefect Horizon; replaced MCPServer with FastMCP (Horizon requires a FastMCP instance).
- 2026-10-05 - no new provider added; ISIN resolver tool proposed, awaiting user go-ahead.
- 2026-10-05 - project-resume.md created and tracked in git (user decision).
- 2026-10-05 - Alpaca quotes stay IEX-only, no yfinance field fill-in.

## 8. Resume prompt
Paste this into a new session:
> Read project-resume.md in this project fully. Read every file it lists under "Must read before any change". Then continue from "Current state". Do not change anything before you have understood the project. Keep project-resume.md updated.
