---
name: Multi-source quote routing
description: Source priority and response semantics for routed quote requests.
---

For latest-price requests, use Binance for recognized crypto symbols, Finnhub for the maintained Dow Jones 30/Nasdaq 100 universe, and yfinance-mcp for everything else or whenever a preferred provider fails. Keep the original ticker in responses.

**Why:** The external providers improve freshness for their supported markets without changing yfinance’s broader symbol and historical-data coverage.

**How to apply:** Preserve `data_source`, `delay_minutes`, and `timestamp` on successful price responses, and log the selected or fallback source without logging credentials or request URLs containing tokens.