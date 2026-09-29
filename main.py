"""Read-only market-data MCP server backed by yfinance."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
import logging
import math
import os
import threading
import time
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf
from mcp.server import MCPServer
from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Mount, Route
import uvicorn

from market_data import (
    INTERVALS,
    PERIODS,
    NoData,
    atr,
    compact_history,
    download_batch,
    drop_nulls,
    ema,
    get_history,
    indicator_snapshot,
    now_utc,
    rsi,
    rounded,
    series,
    sma,
    stats,
    ticker_name,
)


PORT = int(os.environ.get("PORT", "5000"))
MAX_BATCH_TICKERS = 50
DEFAULT_QUOTE_FIELDS = [
    "price",
    "chg_pct",
    "prev_close",
    "day_high",
    "day_low",
    "volume",
    "market_state",
]
QUOTE_FIELDS = set(DEFAULT_QUOTE_FIELDS)
SCAN_FIELDS = {
    "price",
    "chg_pct",
    "rsi14",
    "trend",
    "sma50_pos",
    "sma200_pos",
    "atr_pct",
    "vol_ratio",
    "dist_high_pct",
    "dist_low_pct",
    "ret_5d",
    "ret_1m",
    "ret_3m",
    "ret_ytd",
    "score",
}
SECTOR_ETFS = ["XLK", "XLF", "XLE", "XLV", "XLY", "XLP", "XLI", "XLB", "XLU", "XLRE", "XLC"]
SNAPSHOT_SYMBOLS = ["SPY", "QQQ", "IWM", "^VIX", "DX-Y.NYB", "^TNX", "BTC-USD", "GC=F", "CL=F"]

logger = logging.getLogger("market-mcp")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

mcp = MCPServer(
    name="yfinance-market-data",
    title="yfinance Market Data",
    description="Compact, read-only market data and analysis using exact yfinance ticker symbols.",
    instructions=(
        "Market data only. No account, broker, or position access. "
        "Use exact ticker symbols. Data may be delayed."
    ),
    version="2.0.0",
)

_quote_cache: dict[str, tuple[float, dict[str, Any], str | None]] = {}
_quote_cache_lock = threading.RLock()


def _error(symbol: str, error: Exception | str = "no data") -> dict[str, str]:
    return {"error": f"{symbol}: {str(error) if str(error) != 'no data' else 'no data'}"}


def _safe_call(symbol: str, function):
    try:
        return function()
    except NoData:
        return _error(symbol)
    except Exception as error:
        message = str(error).splitlines()[0][:180] or "no data"
        return _error(symbol, message)


def _result(data: dict[str, Any], as_of: str | None = None, cached: bool = False) -> dict[str, Any]:
    result = drop_nulls(data)
    result["as_of"] = as_of or result.get("as_of") or now_utc()
    result["delayed"] = True
    if cached:
        result["cached"] = True
    return result


def _validate_fields(fields: list[str] | None, allowed: set[str], default: list[str]) -> list[str]:
    fields = fields or default
    if not isinstance(fields, list) or any(field not in allowed for field in fields):
        raise ValueError(f"fields must be a subset of {', '.join(sorted(allowed))}")
    return list(dict.fromkeys(fields))


def _history_args(interval: str, period: str | None) -> tuple[str, str | None]:
    # The current MCP schema is interval, period. Also accept the prompt's
    # positional example ("3mo", "1d") without changing named-argument behavior.
    if interval in PERIODS and period in INTERVALS:
        return period, interval
    return interval, period


def _old_rows(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for timestamp, row in frame.iterrows():
        item = {
            "timestamp": timestamp.isoformat() if hasattr(timestamp, "isoformat") else str(timestamp),
            "open": rounded(row.get("Open")),
            "high": rounded(row.get("High")),
            "low": rounded(row.get("Low")),
            "close": rounded(row.get("Close")),
            "volume": int(row.get("Volume")) if pd.notna(row.get("Volume")) else None,
        }
        rows.append({key: value for key, value in item.items() if value is not None})
    return rows


def _quote_base(symbol: str) -> tuple[dict[str, Any], str | None, bool]:
    now = time.monotonic()
    with _quote_cache_lock:
        cached = _quote_cache.get(symbol)
        if cached and cached[0] > now:
            return cached[1].copy(), cached[2], True

    try:
        frame, _, as_of = get_history(
            symbol, period="1d", interval="1m", prepost=True, auto_adjust=False
        )
        source_interval = "1m"
    except Exception:
        frame, _, as_of = get_history(
            symbol, period="5d", interval="1d", prepost=True, auto_adjust=False
        )
        source_interval = "1d"
    latest = frame.iloc[-1]
    close = float(latest["Close"])
    previous_close = None
    try:
        daily, _, _ = get_history(symbol, period="5d", interval="1d", prepost=True)
        if len(daily) > 1:
            previous_close = float(daily["Close"].iloc[-2])
    except Exception:
        pass
    if previous_close is None:
        previous_close = close
    fast: dict[str, Any] = {}
    try:
        info = yf.Ticker(symbol).fast_info
        for key in ("currency", "exchange", "timezone", "marketCap"):
            value = info.get(key)
            if value is not None:
                fast[key] = value
    except Exception:
        pass
    base = {
        "ticker": symbol,
        "price": rounded(close),
        "chg_pct": rounded((close / previous_close - 1) * 100) if previous_close else 0,
        "prev_close": rounded(previous_close),
        "day_high": rounded(frame["High"].max()),
        "day_low": rounded(frame["Low"].min()),
        "volume": int(frame["Volume"].sum()) if frame["Volume"].notna().any() else None,
        "source_interval": source_interval,
        "market_state": _market_state(symbol),
        **fast,
    }
    base = drop_nulls(base)
    with _quote_cache_lock:
        _quote_cache[symbol] = (now + 30, base.copy(), as_of)
    return base, as_of, False


def _market_state(symbol: str) -> str:
    if symbol.endswith("-USD") or symbol.endswith("=X"):
        return "open"
    try:
        now = datetime.now(ZoneInfo("America/New_York"))
        return "open" if now.weekday() < 5 and 9.5 <= now.hour + now.minute / 60 < 16 else "closed"
    except Exception:
        return "unknown"


def _metric(frame: pd.DataFrame, name: str) -> Any:
    snapshot = indicator_snapshot(frame)
    if name in snapshot:
        return snapshot[name]
    close = series(frame)
    if name == "ret_5d":
        return rounded((close.iloc[-1] / close.iloc[-6] - 1) * 100) if len(close) > 5 else None
    if name == "ret_1m":
        return rounded((close.iloc[-1] / close.iloc[-22] - 1) * 100) if len(close) > 21 else None
    if name == "ret_3m":
        return rounded((close.iloc[-1] / close.iloc[-64] - 1) * 100) if len(close) > 63 else None
    if name == "ret_ytd":
        year_start = close[close.index.year == close.index[-1].year]
        return rounded((close.iloc[-1] / year_start.iloc[0] - 1) * 100) if len(year_start) else None
    if name == "score":
        score = 50
        score += 15 if snapshot.get("trend") == "up" else -15 if snapshot.get("trend") == "down" else 0
        score += 10 if snapshot.get("sma50_pos") == "above" else -10
        return max(0, min(100, score))
    return None


def _analysis_for(symbol: str, period: str, interval: str):
    frame, cached, as_of = get_history(symbol, period=period, interval=interval, auto_adjust=False)
    return frame, indicator_snapshot(frame), as_of, cached


@mcp.tool()
def get_quote(
    ticker: str | None = None,
    symbols: list[str] | None = None,
    fields: list[str] | None = None,
) -> dict[str, Any]:
    """Get a compact latest quote, or quotes for symbols in one request."""
    requested = symbols or ([ticker] if ticker else [])
    if not requested:
        return {"error": "ticker: no data"}
    requested = [ticker_name(item) for item in requested]
    selected = _validate_fields(fields, QUOTE_FIELDS, DEFAULT_QUOTE_FIELDS)
    if len(requested) == 1:
        symbol = requested[0]
        result = _safe_call(symbol, lambda: _quote_base(symbol))
        if "error" in result:
            return result
        base, as_of, cached = result
        return _result({"ticker": symbol, **{field: base.get(field) for field in selected}}, as_of, cached)

    data: dict[str, Any] = {}
    errors: dict[str, str] = {}
    newest = None
    for symbol in requested:
        result = _safe_call(symbol, lambda symbol=symbol: _quote_base(symbol))
        if "error" in result:
            errors[symbol] = result["error"]
        else:
            base, as_of, _ = result
            newest = max(filter(None, [newest, as_of]), default=newest)
            data[symbol] = {field: base.get(field) for field in selected}
    output: dict[str, Any] = {"data": data}
    if errors:
        output["errors"] = errors
    return _result(output, newest)


@mcp.tool()
def get_historical_data(
    ticker: str,
    interval: str = "1d",
    period: str | None = "1mo",
    fields: list[str] | None = None,
    limit: int | None = None,
    compact: bool = True,
    summary_only: bool = False,
    resample: str | None = None,
    candles: int | None = None,
    start: str | None = None,
    end: str | None = None,
    prepost: bool = False,
    auto_adjust: bool = False,
) -> dict[str, Any]:
    """Get compact OHLCV history. Use period before interval only for positional legacy calls."""
    interval, period = _history_args(interval, period)
    limit = limit if limit is not None else candles

    def fetch():
        frame, was_cached, as_of = get_history(
            ticker,
            period=period,
            interval=interval,
            start=start,
            end=end,
            prepost=prepost,
            auto_adjust=auto_adjust,
        )
        if compact:
            data = compact_history(frame, fields, limit, resample, summary_only)
        else:
            data = {"candles": _old_rows(frame.tail(limit) if limit else frame)}
        data["ticker"] = ticker_name(ticker)
        return _result(data, as_of, was_cached)

    return _safe_call(ticker, fetch)


@mcp.tool()
def get_batch_historical_data(
    tickers: list[str],
    interval: str = "1d",
    period: str | None = "1mo",
    fields: list[str] | None = None,
    limit: int | None = None,
    compact: bool = True,
    summary_only: bool = False,
    resample: str | None = None,
    candles: int | None = None,
    start: str | None = None,
    end: str | None = None,
    prepost: bool = False,
    auto_adjust: bool = False,
) -> dict[str, Any]:
    """Get compact OHLCV history for multiple exact yfinance tickers."""
    if not tickers:
        return {"error": "tickers: no data"}
    interval, period = _history_args(interval, period)
    limit = limit if limit is not None else candles
    data: dict[str, Any] = {}
    errors: dict[str, str] = {}
    newest = None
    for symbol in tickers[:MAX_BATCH_TICKERS]:
        try:
            frame, cached, as_of = get_history(
                symbol,
                period=period,
                interval=interval,
                start=start,
                end=end,
                prepost=prepost,
                auto_adjust=auto_adjust,
            )
            if compact:
                item = compact_history(frame, fields, limit, resample, summary_only)
            else:
                item = {"candles": _old_rows(frame.tail(limit) if limit else frame)}
            item["ticker"] = ticker_name(symbol)
            if cached:
                item["cached"] = True
            data[ticker_name(symbol)] = item
            newest = max(filter(None, [newest, as_of]), default=newest)
        except Exception:
            errors[ticker_name(symbol)] = f"{ticker_name(symbol)}: no data"
    output: dict[str, Any] = {"data": data}
    if errors:
        output["errors"] = errors
    return _result(output, newest)


@mcp.tool()
def get_analysis(symbol: str, period: str = "1y", interval: str = "1d") -> dict[str, Any]:
    """One-call compact technical digest; replaces multiple indicator calls."""
    result = _safe_call(symbol, lambda: _analysis_for(symbol, period, interval))
    if isinstance(result, dict) and "error" in result:
        return result
    frame, analysis, as_of, cached = result
    analysis["ticker"] = ticker_name(symbol)
    return _result(analysis, as_of, cached)


@mcp.tool()
def get_trade_setup(symbol: str, style: str = "swing") -> dict[str, Any]:
    """Return a deterministic fresh-entry setup; it knows nothing about existing positions."""
    configs = {"intraday": ("5d", "15m"), "swing": ("6mo", "1d"), "position": ("2y", "1wk")}
    if style not in configs:
        return {"error": f"{symbol}: style must be intraday, swing, or position"}
    period, interval = configs[style]
    result = _safe_call(symbol, lambda: _analysis_for(symbol, period, interval))
    if isinstance(result, dict) and "error" in result:
        return result
    frame, analysis, as_of, cached = result
    price = analysis.get("price")
    atr_value = analysis.get("atr14") or price * 0.02
    supports = analysis.get("support", [])
    resistances = analysis.get("resistance", [])
    trend = analysis.get("trend")
    rsi_value = analysis.get("rsi14", 50)
    long_signal = trend == "up" and analysis.get("sma50_pos") == "above" and rsi_value < 70
    short_signal = trend == "down" and analysis.get("sma50_pos") == "below" and rsi_value > 30
    bias = "long" if long_signal else "short" if short_signal else "none"
    reasons = [f"trend {trend}", f"RSI {rsi_value}"]
    if analysis.get("sma200_pos"):
        reasons.append(f"price {analysis['sma200_pos']} 200 SMA")
    if analysis.get("vol_ratio"):
        reasons.append(f"vol {analysis['vol_ratio']}x")
    if bias == "long":
        stop = (supports[0] if supports else price - atr_value) - 0.5 * atr_value
        target = next((level for level in resistances if level > price), price + 2 * (price - stop))
    elif bias == "short":
        stop = (resistances[0] if resistances else price + atr_value) + 0.5 * atr_value
        target = next((level for level in reversed(supports) if level < price), price - 2 * (stop - price))
    else:
        stop, target = price - atr_value, price + atr_value
    risk = abs(price - stop)
    reward = abs(target - price)
    score = 50 if bias != "none" else 30
    score += 15 if trend in {"up", "down"} else 0
    score += 10 if analysis.get("macd", {}).get("state") in {"bull", "cross_up"} and bias == "long" else 0
    score += 10 if analysis.get("macd", {}).get("state") in {"bear", "cross_down"} and bias == "short" else 0
    score = min(100, max(0, score))
    if bias == "none":
        reasons.append("trend and momentum do not align")
    return _result(
        {
            "ticker": ticker_name(symbol),
            "style": style,
            "bias": bias,
            "entry": rounded(price),
            "stop": rounded(stop),
            "target": rounded(target),
            "rr": rounded(reward / risk) if risk else 0,
            "invalidation": rounded(stop),
            "score": score,
            "reasons": reasons[:4],
        },
        as_of,
        cached,
    )


@mcp.tool()
def scan_watchlist(
    symbols: list[str],
    fields: list[str],
    sort_by: str | None = None,
    desc: bool = True,
    limit: int | None = None,
) -> dict[str, Any]:
    """Scan supplied symbols with one yfinance batch download; replaces many calls."""
    if not symbols:
        return {"error": "symbols: no data"}
    fields = _validate_fields(fields, SCAN_FIELDS, list(SCAN_FIELDS))
    if sort_by and sort_by not in fields:
        raise ValueError("sort_by must be one of the requested fields")
    frames, cached, as_of = download_batch(symbols, period="1y", interval="1d")
    rows: list[list[Any]] = []
    errors: dict[str, str] = {}
    for symbol in symbols:
        frame = frames.get(symbol)
        if frame is None or frame.empty:
            errors[symbol] = f"{symbol}: no data"
            continue
        row = [symbol] + [_metric(frame, field) for field in fields]
        rows.append(row)
    cols = ["symbol"] + fields
    if sort_by:
        index = cols.index(sort_by)
        rows.sort(key=lambda row: row[index] is None and 1 or row[index], reverse=desc)
    if limit:
        rows = rows[:limit]
    output: dict[str, Any] = {"cols": cols, "rows": rows}
    if errors:
        output["errors"] = errors
    return _result(output, as_of, cached)


@mcp.tool()
def market_snapshot() -> dict[str, Any]:
    """One-call market and sector snapshot; replaces multiple market-data calls."""
    symbols = SNAPSHOT_SYMBOLS + SECTOR_ETFS
    frames, cached, as_of = download_batch(symbols, period="3mo", interval="1d")
    items = []
    for symbol in SNAPSHOT_SYMBOLS:
        frame = frames.get(symbol)
        if frame is not None and not frame.empty:
            analysis = indicator_snapshot(frame)
            close = series(frame)
            items.append(
                {
                    "symbol": symbol,
                    "price": analysis.get("price"),
                    "chg_pct": analysis.get("chg_pct"),
                    "ret_5d": rounded((close.iloc[-1] / close.iloc[-6] - 1) * 100)
                    if len(close) > 5
                    else None,
                    "trend": analysis.get("trend"),
                }
            )
    sectors = [item for item in (market_snapshot_row(frames.get(symbol), symbol) for symbol in SECTOR_ETFS) if item]
    sectors.sort(key=lambda item: item.get("chg_pct", -math.inf), reverse=True)
    vix = next((item for item in items if item["symbol"] == "^VIX"), {})
    spy = next((item for item in items if item["symbol"] == "SPY"), {})
    regime = "neutral"
    if spy.get("trend") == "up" and (vix.get("price", 99) < 20 or vix.get("chg_pct", 0) < 0):
        regime = "risk_on"
    elif spy.get("trend") == "down" and (vix.get("price", 0) > 20 or vix.get("chg_pct", 0) > 0):
        regime = "risk_off"
    return _result({"items": items, "sectors": sectors, "regime": regime}, as_of, cached)


def market_snapshot_row(frame: pd.DataFrame | None, symbol: str) -> dict[str, Any] | None:
    if frame is None or frame.empty:
        return None
    analysis = indicator_snapshot(frame)
    return {
        "symbol": symbol,
        "price": analysis.get("price"),
        "chg_pct": analysis.get("chg_pct"),
        "ret_5d": _metric(frame, "ret_5d"),
        "trend": analysis.get("trend"),
    }


@mcp.tool()
def compare(
    symbols: list[str],
    period: str = "6mo",
    benchmark: str = "SPY",
) -> dict[str, Any]:
    """Compare returns, beta, correlation, drawdown, and volatility compactly."""
    symbols = list(dict.fromkeys(symbols))
    requested = symbols + ([] if benchmark in symbols else [benchmark])
    frames, cached, as_of = download_batch(requested, period=period, interval="1d")
    benchmark_frame = frames.get(benchmark)
    if benchmark_frame is None:
        return _error(benchmark)
    benchmark_returns = series(benchmark_frame).pct_change()
    rows = []
    valid_symbols = []
    for symbol in symbols:
        frame = frames.get(symbol)
        if frame is None or frame.empty:
            continue
        aligned = pd.concat([series(frame).pct_change(), benchmark_returns], axis=1).dropna()
        aligned.columns = ["asset", "benchmark"]
        beta = aligned["asset"].cov(aligned["benchmark"]) / aligned["benchmark"].var()
        corr = aligned["asset"].corr(aligned["benchmark"])
        rows.append(
            [
                symbol,
                stats(frame).get("return_pct"),
                rounded(beta),
                rounded(corr),
                stats(frame).get("max_drawdown_pct"),
                stats(frame).get("volatility_ann_pct"),
            ]
        )
        valid_symbols.append(symbol)
    matrix = pd.DataFrame(
        {symbol: series(frames[symbol]).pct_change() for symbol in valid_symbols}
    ).corr()
    return _result(
        {
            "cols": ["symbol", "return_pct", "beta", "correlation", "max_drawdown_pct", "volatility_ann_pct"],
            "rows": rows,
            "correlation": {"symbols": valid_symbols, "matrix": matrix.round(2).fillna(0).values.tolist()},
        },
        as_of,
        cached,
    )


@mcp.tool()
def get_fundamentals_brief(symbol: str) -> dict[str, Any]:
    """Return a compact fundamentals brief for one exact yfinance symbol."""
    def fetch():
        info = yf.Ticker(symbol).info
        mapping = {
            "market_cap": "marketCap",
            "pe": "trailingPE",
            "fwd_pe": "forwardPE",
            "eps_growth": "earningsGrowth",
            "rev_growth": "revenueGrowth",
            "profit_margin": "profitMargins",
            "debt_to_equity": "debtToEquity",
            "div_yield": "dividendYield",
            "analyst_target": "targetMeanPrice",
            "analyst_rating": "recommendationKey",
            "next_earnings_date": "earningsTimestamp",
            "sector": "sector",
        }
        output = {}
        for target, source in mapping.items():
            value = info.get(source)
            if source in {"earningsGrowth", "revenueGrowth", "profitMargins", "dividendYield"} and value is not None:
                value = round(float(value) * 100, 2)
            elif source == "earningsTimestamp" and value:
                value = datetime.fromtimestamp(value, timezone.utc).isoformat()
            output[target] = rounded(value) if isinstance(value, (int, float)) else value
        return _result({"symbol": ticker_name(symbol), **output})
    return _safe_call(symbol, fetch)


@mcp.tool()
def get_events(symbols: list[str], days: int = 14) -> dict[str, Any]:
    """Return upcoming earnings and ex-dividend events within the requested window."""
    end = datetime.now(timezone.utc) + timedelta(days=max(1, min(days, 365)))
    data: dict[str, Any] = {}
    errors: dict[str, str] = {}
    for symbol in symbols:
        try:
            client = yf.Ticker(symbol)
            events = []
            calendar = client.calendar
            if isinstance(calendar, pd.DataFrame):
                calendar = calendar.to_dict()
            earnings = calendar.get("Earnings Date") if isinstance(calendar, dict) else None
            if isinstance(earnings, list):
                earnings = earnings[0] if earnings else None
            if earnings is not None:
                stamp = pd.Timestamp(earnings).tz_localize("UTC") if pd.Timestamp(earnings).tzinfo is None else pd.Timestamp(earnings).tz_convert("UTC")
                if datetime.now(timezone.utc) <= stamp.to_pydatetime() <= end:
                    events.append({"type": "earnings", "date": stamp.date().isoformat()})
            dividends = client.dividends
            if not dividends.empty:
                for stamp, value in dividends.tail(20).items():
                    stamp = pd.Timestamp(stamp).tz_convert("UTC") if pd.Timestamp(stamp).tzinfo else pd.Timestamp(stamp).tz_localize("UTC")
                    if datetime.now(timezone.utc) <= stamp.to_pydatetime() <= end:
                        events.append({"type": "dividend", "date": stamp.date().isoformat(), "amount": rounded(value, 4)})
            data[ticker_name(symbol)] = events
        except Exception:
            errors[ticker_name(symbol)] = f"{ticker_name(symbol)}: no data"
    output: dict[str, Any] = {"data": data}
    if errors:
        output["errors"] = errors
    return _result(output)


@mcp.tool()
def get_options_brief(symbol: str) -> dict[str, Any]:
    """Return a compact nearest-monthly options summary with approximation flags."""
    def fetch():
        client = yf.Ticker(symbol)
        expiries = list(client.options)
        if not expiries:
            return _error(symbol)
        expiry = next((item for item in expiries if pd.Timestamp(item).month != pd.Timestamp(expiries[0]).month), expiries[0])
        chain = client.option_chain(expiry)
        quote = _quote_base(symbol)[0]
        price = quote.get("price")
        if price is None:
            return _error(symbol)
        calls, puts = chain.calls, chain.puts
        if calls.empty or puts.empty:
            return _error(symbol)
        call = calls.iloc[(calls["strike"] - price).abs().argsort()[:1]].iloc[0]
        put = puts.iloc[(puts["strike"] - price).abs().argsort()[:1]].iloc[0]
        iv_values = [float(call["impliedVolatility"]), float(put["impliedVolatility"])]
        iv = sum(iv_values) / 2
        call_oi = float(calls["openInterest"].fillna(0).sum())
        put_oi = float(puts["openInterest"].fillna(0).sum())
        dte = max(1, (pd.Timestamp(expiry).date() - date.today()).days)
        realized = series(get_history(symbol, period="1y", interval="1d")[0]).pct_change().dropna() * math.sqrt(252)
        rank = (realized < realized.tail(1).iloc[0]).mean() * 100 if not realized.empty else None
        return _result(
            {
                "symbol": ticker_name(symbol),
                "expiry": expiry,
                "iv_atm": round(iv * 100, 2),
                "iv_rank_approx": rounded(rank),
                "iv_rank_is_approx": True,
                "put_call_oi_ratio": rounded(put_oi / call_oi) if call_oi else None,
                "expected_move_pct": rounded(iv * math.sqrt(dte / 365) * 100),
            }
        )
    return _safe_call(symbol, fetch)


@mcp.tool()
def get_news_brief(symbol: str, n: int = 3) -> dict[str, Any]:
    """Return only recent headline, publisher, and published time."""
    def fetch():
        items = []
        for item in yf.Ticker(symbol).news[: max(1, min(n, 10))]:
            content = item.get("content", item)
            published = content.get("pubDate") or content.get("providerPublishTime")
            if isinstance(published, (int, float)):
                published = datetime.fromtimestamp(published, timezone.utc).isoformat()
            items.append(
                drop_nulls(
                    {
                        "headline": content.get("title"),
                        "publisher": (content.get("provider") or {}).get("displayName")
                        if isinstance(content.get("provider"), dict)
                        else content.get("publisher"),
                        "published": published,
                    }
                )
            )
        return _result({"symbol": ticker_name(symbol), "items": items})
    return _safe_call(symbol, fetch)


@mcp.tool()
def position_calc(
    positions: list[dict[str, Any]],
    account_size: float | None = None,
) -> dict[str, Any]:
    """Stateless. Positions must be supplied by the caller. No account access."""
    if not positions:
        return {"error": "positions: no data"}
    symbols = [ticker_name(item.get("symbol", "")) for item in positions]
    frames, _, as_of = download_batch(symbols, period="5d", interval="1d", log=False)
    prices = {symbol: float(series(frame).iloc[-1]) for symbol, frame in frames.items() if not frame.empty}
    values = []
    total_value = 0.0
    total_pnl = 0.0
    for item in positions:
        symbol = ticker_name(item.get("symbol", ""))
        price = prices.get(symbol)
        if price is None:
            continue
        qty = float(item.get("qty", 0))
        avg_cost = float(item.get("avg_cost", 0))
        value = price * qty
        pnl = (price - avg_cost) * qty
        total_value += value
        total_pnl += pnl
        row = {
            "symbol": symbol,
            "price": rounded(price),
            "value": rounded(value),
            "pnl": rounded(pnl),
            "pnl_pct": rounded((price / avg_cost - 1) * 100) if avg_cost else None,
            "weight_pct": rounded(value / account_size * 100) if account_size else None,
        }
        stop = item.get("stop")
        if stop is not None:
            row["risk_to_stop_pct"] = rounded(abs(price - float(stop)) / price * 100)
            row["risk_to_stop_amount"] = rounded(abs(price - float(stop)) * qty)
        values.append(drop_nulls(row))
    if account_size is None and total_value:
        for row in values:
            row["weight_pct"] = rounded(row["value"] / total_value * 100)
    return _result({"positions": values, "total_value": rounded(total_value), "pnl": rounded(total_pnl)}, as_of)


@mcp.tool()
def position_size(
    symbol: str,
    stop: float,
    entry: float | None = None,
    risk_amount: float | None = None,
    account_size: float | None = None,
    risk_pct: float | None = None,
) -> dict[str, Any]:
    """Pure math for a fresh entry; no positions are read or stored."""
    def calculate():
        live = entry
        as_of = None
        if live is None:
            quote, as_of, _ = _quote_base(ticker_name(symbol))
            live = quote.get("price")
        if live is None or live == stop:
            raise ValueError("entry and stop must produce a non-zero risk per unit")
        amount = risk_amount
        if amount is None and account_size is not None and risk_pct is not None:
            amount = account_size * risk_pct / 100
        if amount is None or amount <= 0:
            raise ValueError("provide risk_amount or account_size and risk_pct")
        per_unit = abs(float(live) - float(stop))
        fractional = ticker_name(symbol).endswith("-USD")
        raw_qty = amount / per_unit
        qty = raw_qty if fractional else math.floor(raw_qty)
        return _result(
            {
                "symbol": ticker_name(symbol),
                "entry": rounded(live),
                "stop": rounded(stop),
                "qty": rounded(qty, 6) if fractional else int(qty),
                "position_value": rounded(qty * live),
                "risk_amount": rounded(amount),
            },
            as_of,
        )
    return _safe_call(symbol, calculate)


async def homepage(_: Request) -> PlainTextResponse:
    return PlainTextResponse("yfinance market-data MCP server. Connect an MCP client to /mcp.\n")


async def healthcheck(_: Request) -> JSONResponse:
    return JSONResponse({"status": "ok", "mcp_endpoint": "/mcp"})


transport_security = TransportSecuritySettings(enable_dns_rebinding_protection=False)
mcp_http_app = mcp.streamable_http_app(
    streamable_http_path="/mcp",
    json_response=True,
    stateless_http=True,
    transport_security=transport_security,
    host="0.0.0.0",
)


@asynccontextmanager
async def lifespan(_: Starlette):
    async with mcp.session_manager.run():
        yield


app = Starlette(
    routes=[Route("/", homepage), Route("/healthz", healthcheck), Mount("/", app=mcp_http_app)],
    lifespan=lifespan,
)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")