---
name: Compact output and setup tuning
description: Why tools return compact JSON text and why get_trade_setup parameters are what they are.
---

All tools are registered through `market_tool`, which returns compact JSON text (no indent, no structured content). The SDK default is `indent=2` plus a duplicate structured copy, roughly 2x the tokens for table-shaped results.

`get_trade_setup` parameters (`SETUP_PARAMS` in main.py) come from a 10y, 47-ticker daily backtest with a 20-bar hold: stops under ~2 ATR were hit first in ~60% of trades, shorts lost money (profit factor ~0.6, also with SPY-down filters), extended entries (ext_atr>2, RSI>70) underperformed, and the 0-100 score had no out-of-sample predictive power. The score is therefore a rule-alignment checklist, not a probability; shorts are opt-in (`shorts=true`).

**Why:** Re-tuning toward "buy the dip" looks better in that sample only because it holds today's surviving large caps in a bull market.

**How to apply:** Do not advertise the score as a win probability. Re-run a backtest before changing SETUP_PARAMS.

Further backtests: 15m (60d) and 1h (2y) trend setups had no edge (PF 0.88-0.99 before costs; a 15m mean-reversion variant looked good at +0.09R but was 0.0R on 1h/2y, so not robust), hence `style=intraday` returns levels only unless `force=true`. Weekly `position` setups showed PF 1.5 vs 1.4 for buy-everything. Cross-sectionally, top-quintile scan scores did not beat the bottom quintile over 20 days, so scan ranking is descriptive, not predictive.

`_history_args` swaps interval/period only when the span comparison shows the legacy (period, interval) order: "1d" and "3mo" are valid as both, so a plain set-membership swap breaks `interval="1d", period="3mo"`.
