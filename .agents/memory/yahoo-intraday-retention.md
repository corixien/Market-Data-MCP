---
name: Yahoo intraday retention
description: Provider limits that affect yfinance intraday history requests.
---

Yahoo Finance provides 1m, 2m, 5m, 15m, 30m, 60m/1h, and 90m bars. The retention window is interval-dependent: 1m is roughly 7–8 days, 2m through 90m are roughly 60 days, and hourly data is available for a substantially longer window.

**Why:** A valid yfinance request can return no data when the requested period exceeds the provider’s intraday retention window; this is not evidence that the MCP transport is broken.

**How to apply:** Keep interval and period separate in client guidance. For 1m data use `period=1d` or `5d`; use larger bar intervals when requesting older intraday history.