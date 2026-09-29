---
name: MCP market-data compatibility
description: Compatibility rules for the read-only yfinance MCP interface.
---

Keep the original market-data tool names and HTTP `/mcp` transport when extending the server. Historical-data calls use the existing interval/period schema, but positional callers may also supply period/interval order.

**Why:** Existing Claude Code configurations may already call the original tools, while generated prompts can use the opposite positional order.

**How to apply:** Prefer named MCP arguments in documentation and schemas; only detect and swap the unambiguous positional period/interval combination.