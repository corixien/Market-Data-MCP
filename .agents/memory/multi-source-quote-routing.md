---
name: Multi-source quote routing
description: Source priority and response semantics for routed quote requests.
---

For latest-price requests, pick the available provider with the least declared delay (ties: CoinGecko, Finnhub, Alpaca), yfinance last. Alpaca = websocket IEX trades via `alpaca_stream.py` (REST is >=15 min delayed), needs `ALPACA_API_KEY`/`ALPACA_API_SECRET`, US equity tickers only. Provider notes: use CoinGecko for recognized crypto symbols, Finnhub for the maintained Dow Jones 30/Nasdaq 100 universe, and yfinance-mcp for everything else or whenever a preferred provider fails. Keep the original ticker in responses.

**Why:** The external providers improve freshness for their supported markets without changing yfinance’s broader symbol and historical-data coverage.

**How to apply:** Use CoinGecko’s public simple-price endpoint with the Replit-stored API key header for crypto quotes. Preserve `data_source`, `delay_minutes`, and `timestamp` on successful price responses, and log the selected or fallback source without logging credentials or request URLs containing tokens.
See `PROVIDERS.md` for per-provider access, delay, limits and coverage. Quote responses carry `fallback_from` when a preferred provider failed. Alpaca IEX quotes intentionally omit day high/low/volume.
