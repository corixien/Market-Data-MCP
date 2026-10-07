"""Read-only market-data MCP server backed by yfinance."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
import email.utils
import functools
import html
import inspect
import json
import logging
import math
import os
import re
import threading
import time
import xml.etree.ElementTree as ET
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request as UrlRequest, urlopen
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.tools import ToolResult
from mcp.types import TextContent
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse

from alpaca_stream import configured as alpaca_configured, stream as alpaca_stream
from market_data import (
    INTERVALS,
    PERIODS,
    NoData,
    atr,
    bull_score,
    compact_history,
    download_batch,
    drop_nulls,
    ema,
    get_history,
    indicator_snapshot,
    now_utc,
    px,
    rsi,
    rounded,
    series,
    sma,
    stats,
    swing_levels,
    ticker_name,
)


PORT = int(os.environ.get("PORT", "5000"))
MAX_BATCH_TICKERS = 50
COINGECKO_API_URL = "https://api.coingecko.com/api/v3/simple/price"
FINNHUB_API_URL = "https://finnhub.io/api/v1/quote"
ALPACA_SNAPSHOT_URL = "https://data.alpaca.markets/v2/stocks/{symbol}/snapshot"
FINNHUB_COMPANY_NEWS_URL = "https://finnhub.io/api/v1/company-news"
FINNHUB_MARKET_NEWS_URL = "https://finnhub.io/api/v1/news"
NEWSAPI_EVERYTHING_URL = "https://newsapi.org/v2/everything"
NEWSAPI_HEADLINES_URL = "https://newsapi.org/v2/top-headlines"
FRED_API_URL = "https://api.stlouisfed.org/fred"
SEC_TICKERS_URL = "https://www.sec.gov/include/ticker.txt"
SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
SEC_ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{document}"
FINNHUB_RECOMMENDATION_URL = "https://finnhub.io/api/v1/stock/recommendation"
FINNHUB_EARNINGS_URL = "https://finnhub.io/api/v1/stock/earnings"
NEWS_CACHE_TTL = 600
MACRO_CACHE_TTL = 3600
YFINANCE_DELAY_MINUTES = 15
# Declared delay per provider (minutes). Lowest delay is tried first, ties keep list order.
PROVIDER_DELAYS = {"coingecko": 0, "finnhub": 0, "alpaca": 0, "yfinance-mcp": YFINANCE_DELAY_MINUTES}
US_EQUITY_RE = re.compile(r"^[A-Z]{1,5}([.-][A-Z])?$")
DEFAULT_QUOTE_FIELDS = [
    "price",
    "chg_pct",
    "prev_close",
    "day_high",
    "day_low",
    "volume",
    "market_state",
    "data_source",
    "delay_minutes",
    "timestamp",
    "fallback_from",
]
QUOTE_FIELDS = set(DEFAULT_QUOTE_FIELDS)
COINGECKO_CRYPTO_IDS = {
    "ADA": "cardano",
    "AVAX": "avalanche-2",
    "BCH": "bitcoin-cash",
    "BNB": "binancecoin",
    "BTC": "bitcoin",
    "DOGE": "dogecoin",
    "DOT": "polkadot",
    "ETH": "ethereum",
    "LINK": "chainlink",
    "LTC": "litecoin",
    "MATIC": "matic-network",
    "NEAR": "near",
    "PEPE": "pepe",
    "SHIB": "shiba-inu",
    "SOL": "solana",
    "TRX": "tron",
    "UNI": "uniswap",
    "USDC": "usd-coin",
    "USDT": "tether",
    "XLM": "stellar",
    "XMR": "monero",
    "XRP": "ripple",
}
DOW_JONES_30 = {
    "AAPL",
    "AMGN",
    "AMZN",
    "AXP",
    "BA",
    "CAT",
    "CRM",
    "CSCO",
    "CVX",
    "DIS",
    "GS",
    "HD",
    "HON",
    "IBM",
    "JNJ",
    "JPM",
    "KO",
    "MCD",
    "MMM",
    "MRK",
    "MSFT",
    "NKE",
    "NVDA",
    "PG",
    "SHW",
    "TRV",
    "UNH",
    "V",
    "VZ",
    "WMT",
}
NASDAQ_100 = {
    "AAPL", "ABNB", "ADI", "ADP", "ADSK", "AEP", "AMAT", "AMD", "AMGN",
    "AMZN", "ANSS", "APP", "ARM", "ASML", "AVGO", "AXON", "AZN", "BIIB",
    "BKNG", "BKR", "CCEP", "CDNS", "CEG", "CHTR", "CMCSA", "COST", "CPRT",
    "CRWD", "CSCO", "CSGP", "CSX", "CTAS", "CTSH", "DASH", "DDOG", "DXCM",
    "EA", "EXC", "FANG", "FAST", "FER", "FI", "FOX", "FOXA", "GDDY",
    "GFS", "GILD", "GOOG", "GOOGL", "HON", "IDXX", "ILMN", "INTC", "INTU",
    "ISRG", "KDP", "KHC", "KLAC", "LIN", "LRCX", "MAR", "MCHP", "MDLZ",
    "MELI", "META", "MNST", "MRVL", "MSFT", "MU", "NFLX", "NDAQ", "NICE",
    "NTES", "NVDA", "NXPI", "ODFL", "ON", "ORLY", "PANW", "PAYX", "PCAR",
    "PDD", "PEP", "PLTR", "PYPL", "QCOM", "REGN", "ROP", "ROST", "SBUX",
    "SNPS", "TEAM", "TMUS", "TRI", "TSLA", "TTD", "TTWO", "TXN", "VRSK",
    "VRTX", "WBD", "WDAY", "WDC", "XEL", "ZS",
}
FINNHUB_SYMBOLS = DOW_JONES_30 | NASDAQ_100 | {"DIA", "QQQ", "SPY"}
FINNHUB_INDEX_PROXIES = {
    "^GSPC": ("SPY", "S&P 500"),
    "GSPC": ("SPY", "S&P 500"),
    "SPX": ("SPY", "S&P 500"),
    "SP500": ("SPY", "S&P 500"),
    "S&P500": ("SPY", "S&P 500"),
    "SNP500": ("SPY", "S&P 500"),
    "^DJI": ("DIA", "Dow Jones Industrial Average"),
    "DJI": ("DIA", "Dow Jones Industrial Average"),
    "DJIA": ("DIA", "Dow Jones Industrial Average"),
    "DOW": ("DIA", "Dow Jones Industrial Average"),
    "^IXIC": ("QQQ", "Nasdaq Composite"),
    "IXIC": ("QQQ", "Nasdaq Composite"),
    "NASDAQ": ("QQQ", "Nasdaq Composite"),
}
MAX_SCAN_SYMBOLS = 120
DEFAULT_SCAN_FIELDS = ["price", "chg_pct", "rsi14", "trend", "rs_3m", "score"]
UNIVERSES = {
    "mag7": ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA"],
    "mega": [
        "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "AVGO", "TSLA", "BRK-B", "LLY",
        "JPM", "V", "WMT", "XOM", "UNH", "MA", "COST", "ORCL", "NFLX", "AMD", "HD", "PG",
        "JNJ", "ABBV", "BAC",
    ],
    "dow30": sorted(DOW_JONES_30),
    "ndx100": sorted(NASDAQ_100),
    "sectors": ["XLK", "XLF", "XLE", "XLV", "XLY", "XLP", "XLI", "XLB", "XLU", "XLRE", "XLC"],
    "crypto": [
        "BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "BNB-USD", "DOGE-USD", "ADA-USD",
        "AVAX-USD", "LINK-USD", "DOT-USD",
    ],
    "macro": ["SPY", "QQQ", "IWM", "TLT", "HYG", "GLD", "SLV", "USO", "UUP", "EEM"],
}
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
    "rs_3m",
    "ext_atr",
    "score",
    "bb_pos",
    "ema_stack",
}
SECTOR_ETFS = ["XLK", "XLF", "XLE", "XLV", "XLY", "XLP", "XLI", "XLB", "XLU", "XLRE", "XLC"]
SNAPSHOT_SYMBOLS = ["SPY", "QQQ", "IWM", "^VIX", "DX-Y.NYB", "^TNX", "BTC-USD", "GC=F", "CL=F"]

logger = logging.getLogger("market-mcp")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

mcp = FastMCP(
    name="yfinance-market-data",
    instructions=(
        "Market data only. No account, broker, or position access. Use exact ticker symbols. "
        "Start with market_snapshot (market-wide) or get_analysis (one ticker); use scan_watchlist "
        "to rank many tickers in one call and get_trade_setup for a fresh-entry plan. "
        "Prices: the provider with the least delay for the ticker is used (CoinGecko for recognized crypto, "
        "Finnhub for Dow 30/Nasdaq 100 and common ETFs, Alpaca websocket for other US stocks), "
        "yfinance otherwise or as fallback. Each response has as_of and delayed; delayed=false means live. "
        "News: get_company_news / get_market_news (Finnhub, fresh), search_news / get_top_headlines (NewsAPI, "
        "24 h delayed, 100 calls/day). Macro: get_macro_snapshot first, then get_macro_series, "
        "search_macro_series, get_economic_calendar (FRED). Catalysts: get_filings (SEC 8-K/10-K/10-Q), "
        "get_insider_trades (Form 4), get_analyst_view (consensus, earnings surprises), "
        "get_central_bank_news (Fed, ECB), get_feed_news (DE/US RSS)."
    ),
    version="2.3.0",
)

def market_tool(function):
    """Register function as an MCP tool replying with compact JSON text.

    The SDK default serialises dict results with indent=2 (about twice the tokens on
    tables) and attaches the same data again as structured content.
    """

    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        payload = function(*args, **kwargs)
        failed = isinstance(payload, dict) and list(payload) == ["error"]
        text = json.dumps(payload, separators=(",", ":"), default=str, ensure_ascii=False)
        if failed:
            raise ToolError(text)
        return ToolResult(content=[TextContent(type="text", text=text)])

    wrapper.__signature__ = inspect.signature(function).replace(return_annotation=ToolResult)
    wrapper.__annotations__ = {**function.__annotations__, "return": ToolResult}
    mcp.tool()(wrapper)
    return function


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


def _result(
    data: dict[str, Any],
    as_of: str | None = None,
    cached: bool = False,
    delayed: bool = True,
) -> dict[str, Any]:
    result = drop_nulls(data)
    result["as_of"] = as_of or result.get("as_of") or now_utc()
    result["delayed"] = delayed
    if cached:
        result["cached"] = True
    return result


def _validate_fields(fields: list[str] | None, allowed: set[str], default: list[str]) -> list[str]:
    fields = fields or default
    if not isinstance(fields, list) or any(field not in allowed for field in fields):
        raise ValueError(f"fields must be a subset of {', '.join(sorted(allowed))}")
    return list(dict.fromkeys(fields))


_INTERVAL_MINUTES = {
    "1m": 1, "2m": 2, "5m": 5, "15m": 15, "30m": 30, "60m": 60, "1h": 60, "90m": 90,
    "1d": 1440, "5d": 7200, "1wk": 10080, "1mo": 43200, "3mo": 129600,
}
_PERIOD_MINUTES = {
    "1d": 1440, "5d": 7200, "1mo": 43200, "3mo": 129600, "6mo": 259200, "1y": 525600,
    "2y": 1051200, "5y": 2628000, "10y": 5256000, "ytd": 525600, "max": 10**9,
}


def _history_args(interval: str, period: str | None) -> tuple[str, str | None]:
    # Schema order is interval, period. Legacy positional callers may pass
    # (period, interval); swap only when that is the sole sensible reading.
    # "1d" and "3mo" are valid as both, so compare spans: a bar cannot be longer
    # than the whole period.
    if period is None:
        if interval not in INTERVALS and interval in PERIODS:
            return "1d", interval
        return interval, None
    if interval in INTERVALS and period in PERIODS:
        if _INTERVAL_MINUTES[interval] > _PERIOD_MINUTES[period] and (
            interval in PERIODS and period in INTERVALS
        ):
            return period, interval
        return interval, period
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


def _http_json(
    url: str,
    params: dict[str, Any],
    headers: dict[str, str] | None = None,
    allow_list: bool = False,
) -> Any:
    request = UrlRequest(
        f"{url}?{urlencode(params)}",
        headers={"Accept": "application/json", **(headers or {})},
    )
    try:
        with urlopen(request, timeout=8) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        raise RuntimeError(f"provider HTTP {error.code}") from error
    except (URLError, TimeoutError, json.JSONDecodeError) as error:
        raise RuntimeError("provider request failed") from error
    if not isinstance(payload, dict) and not (allow_list and isinstance(payload, list)):
        raise RuntimeError("provider returned an invalid response")
    return payload


def _timestamp_from_epoch(value: Any, milliseconds: bool = False) -> str:
    seconds = float(value) / (1000 if milliseconds else 1)
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat()


def _coingecko_id(symbol: str) -> str | None:
    upper = symbol.upper().strip()
    normalized = upper.replace("-", "").replace("/", "").replace("_", "")
    for base, coin_id in sorted(COINGECKO_CRYPTO_IDS.items(), key=lambda item: len(item[0]), reverse=True):
        if upper == coin_id.upper():
            return coin_id
        if normalized == base or normalized.startswith(f"{base}USD") or normalized.startswith(f"{base}USDT"):
            return coin_id
        if upper.startswith(f"{base}-") or upper.startswith(f"{base}/"):
            return coin_id
    return None


def _available_providers(symbol: str) -> list[str]:
    """Providers able to quote symbol, least delay first; yfinance is always the last resort."""
    available = []
    if _coingecko_id(symbol):
        available.append("coingecko")
    else:
        if os.environ.get("FINNHUB_API_KEY") and (
            _finnhub_index_proxy(symbol) or symbol.upper().strip() in FINNHUB_SYMBOLS
        ):
            available.append("finnhub")
        if alpaca_configured() and US_EQUITY_RE.match(symbol.upper().strip()):
            available.append("alpaca")
    available.append("yfinance-mcp")
    return sorted(available, key=lambda name: PROVIDER_DELAYS[name])


def _finnhub_index_proxy(symbol: str) -> tuple[str, str] | None:
    normalized = symbol.upper().strip().replace(" ", "").replace("_", "").replace("-", "")
    return FINNHUB_INDEX_PROXIES.get(normalized)


def _coingecko_quote(symbol: str) -> tuple[dict[str, Any], str]:
    coin_id = _coingecko_id(symbol)
    if not coin_id:
        raise ValueError("not a CoinGecko crypto symbol")
    api_key = os.environ.get("COINGECKO_API_KEY")
    headers = {"x-cg-demo-api-key": api_key} if api_key else {}
    payload = _http_json(
        COINGECKO_API_URL,
        {
            "ids": coin_id,
            "vs_currencies": "usd",
            "include_24hr_change": "true",
            "include_last_updated_at": "true",
        },
        headers,
    )
    quote = payload.get(coin_id)
    if not isinstance(quote, dict) or quote.get("usd") is None:
        raise RuntimeError("CoinGecko returned no price")
    timestamp = (
        _timestamp_from_epoch(quote["last_updated_at"])
        if quote.get("last_updated_at")
        else now_utc()
    )
    return (
        drop_nulls(
            {
                "ticker": symbol,
                "price": px(quote.get("usd")),
                "chg_pct": rounded(quote.get("usd_24h_change")),
                "market_state": "open",
                "source_interval": "live",
            }
        ),
        timestamp,
    )


def _finnhub_quote(symbol: str) -> tuple[dict[str, Any], str]:
    api_key = os.environ.get("FINNHUB_API_KEY")
    if not api_key:
        raise RuntimeError("Finnhub API key is not configured")
    proxy = _finnhub_index_proxy(symbol)
    source_symbol = proxy[0] if proxy else symbol.upper()
    payload = _http_json(FINNHUB_API_URL, {"symbol": source_symbol, "token": api_key})
    price = payload.get("c")
    timestamp_value = payload.get("t")
    if not price or not timestamp_value:
        raise RuntimeError("Finnhub returned no price")
    timestamp = _timestamp_from_epoch(timestamp_value)
    proxy_fields = (
        {
            "proxy_symbol": source_symbol,
            "proxy_for": proxy[1],
            "price_is_proxy": True,
        }
        if proxy
        else {}
    )
    return (
        drop_nulls(
            {
                "ticker": symbol,
                "price": px(price),
                "chg_pct": rounded(payload.get("dp")),
                "prev_close": px(payload.get("pc"), price),
                "day_high": px(payload.get("h"), price),
                "day_low": px(payload.get("l"), price),
                "market_state": "open",
                "source_interval": "live",
                **proxy_fields,
            }
        ),
        timestamp,
    )


def _alpaca_quote(symbol: str) -> tuple[dict[str, Any], str]:
    ticker = symbol.upper().strip()
    price, epoch = alpaca_stream.latest_trade(ticker)
    prev_close = None
    try:
        snapshot = _http_json(
            ALPACA_SNAPSHOT_URL.format(symbol=ticker),
            {"feed": "iex"},
            {
                "APCA-API-KEY-ID": os.environ.get("ALPACA_API_KEY", ""),
                "APCA-API-SECRET-KEY": os.environ.get("ALPACA_API_SECRET", ""),
            },
        )
        prev_close = (snapshot.get("prevDailyBar") or {}).get("c")
    except Exception:
        pass
    return (
        drop_nulls(
            {
                "ticker": symbol,
                "price": px(price),
                "chg_pct": rounded((price / prev_close - 1) * 100) if prev_close else None,
                "prev_close": px(prev_close, price) if prev_close else None,
                "market_state": _market_state(symbol),
                "source_interval": "live",
            }
        ),
        _timestamp_from_epoch(epoch),
    )


def _with_source(
    base: dict[str, Any],
    as_of: str | None,
    source: str,
    delay_minutes: int,
    failed: list[str] | None = None,
) -> tuple[dict[str, Any], str]:
    timestamp = as_of or now_utc()
    return (
        drop_nulls(
            {
                **base,
                "data_source": source,
                "delay_minutes": delay_minutes,
                "timestamp": timestamp,
                "fallback_from": ",".join(failed) if failed else None,
            }
        ),
        timestamp,
    )


def _yfinance_quote_base(symbol: str) -> tuple[dict[str, Any], str | None, bool]:
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
        "price": px(close),
        "chg_pct": rounded((close / previous_close - 1) * 100) if previous_close else 0,
        "prev_close": px(previous_close, close),
        "day_high": px(frame["High"].max(), close),
        "day_low": px(frame["Low"].min(), close),
        "volume": int(frame["Volume"].sum()) if frame["Volume"].notna().any() else None,
        "source_interval": source_interval,
        "market_state": _market_state(symbol),
        **fast,
    }
    base = drop_nulls(base)
    with _quote_cache_lock:
        _quote_cache[symbol] = (now + 30, base.copy(), as_of)
    return base, as_of, False


def _quote_base(symbol: str) -> tuple[dict[str, Any], str | None, bool]:
    now = time.monotonic()
    with _quote_cache_lock:
        cached = _quote_cache.get(f"routed:{symbol}")
        if cached and cached[0] > now:
            logger.info(
                "price source used ticker=%s source=%s cached=true",
                symbol,
                cached[1].get("data_source", "yfinance-mcp"),
            )
            return cached[1].copy(), cached[2], True

    quoters = {
        "coingecko": _coingecko_quote,
        "finnhub": _finnhub_quote,
        "alpaca": _alpaca_quote,
    }
    base: dict[str, Any] = {}
    as_of: str | None = None
    failed: list[str] = []
    for provider in _available_providers(symbol):
        if provider == "yfinance-mcp":
            yfinance_base, yfinance_as_of, _ = _yfinance_quote_base(symbol)
            base, as_of = _with_source(
                yfinance_base, yfinance_as_of, provider, PROVIDER_DELAYS[provider], failed
            )
            if failed:
                logger.info(
                    "price source used ticker=%s source=yfinance-mcp fallback_from=%s",
                    symbol,
                    ",".join(failed),
                )
            else:
                logger.info("price source used ticker=%s source=yfinance-mcp", symbol)
            break
        try:
            provider_base, provider_timestamp = quoters[provider](symbol)
            base, as_of = _with_source(
                provider_base, provider_timestamp, provider, PROVIDER_DELAYS[provider]
            )
            logger.info("price source used ticker=%s source=%s", symbol, provider)
            break
        except Exception as error:
            failed.append(provider)
            logger.warning(
                "price source failed ticker=%s source=%s reason=%s",
                symbol,
                provider,
                str(error).splitlines()[0][:160],
            )

    with _quote_cache_lock:
        _quote_cache[f"routed:{symbol}"] = (now + 30, base.copy(), as_of)
    return base, as_of, False


def _market_state(symbol: str) -> str:
    if symbol.endswith("-USD") or symbol.endswith("=X"):
        return "open"
    try:
        now = datetime.now(ZoneInfo("America/New_York"))
        return "open" if now.weekday() < 5 and 9.5 <= now.hour + now.minute / 60 < 16 else "closed"
    except Exception:
        return "unknown"


_aux_cache: dict[tuple, tuple[float, Any]] = {}


def _aux_cached(key: tuple, ttl: float, loader):
    now = time.monotonic()
    with _quote_cache_lock:
        hit = _aux_cache.get(key)
        if hit and hit[0] > now:
            return hit[1]
    value = loader()
    with _quote_cache_lock:
        _aux_cache[key] = (now + ttl, value)
    return value


def _calendar(symbol: str) -> dict[str, date | None]:
    """Next earnings and ex-dividend dates (cached 6h); empty values when unavailable."""

    def load() -> dict[str, date | None]:
        try:
            raw = yf.Ticker(symbol).calendar
        except Exception:
            return {}
        if isinstance(raw, pd.DataFrame):
            raw = raw.to_dict()
        if not isinstance(raw, dict):
            return {}
        today = datetime.now(timezone.utc).date()
        earnings = raw.get("Earnings Date")
        earnings = earnings if isinstance(earnings, list) else [earnings]
        upcoming = []
        for item in earnings:
            try:
                stamp = pd.Timestamp(item)
                stamp = None if pd.isna(stamp) else stamp.date()
            except Exception:
                continue
            if stamp and stamp >= today:
                upcoming.append(stamp)
        ex_div = raw.get("Ex-Dividend Date")
        try:
            ex_div = None if ex_div is None or pd.isna(pd.Timestamp(ex_div)) else pd.Timestamp(ex_div).date()
        except Exception:
            ex_div = None
        return {"earnings": min(upcoming) if upcoming else None, "ex_div": ex_div}

    return _aux_cached(("calendar", symbol), 6 * 3600, load)


def _earn_days(symbol: str) -> int | None:
    if symbol.endswith("-USD") or symbol.startswith("^") or symbol.endswith("=X") or symbol.endswith("=F"):
        return None
    stamp = _calendar(symbol).get("earnings")
    return (stamp - datetime.now(timezone.utc).date()).days if stamp else None


def _bench_ret_3m(benchmark: str = "SPY") -> float | None:
    def load():
        frame, _, _ = get_history(benchmark, period="1y", interval="1d")
        close = series(frame)
        return (float(close.iloc[-1]) / float(close.iloc[-64]) - 1) * 100 if len(close) > 63 else None

    return _aux_cached(("ret3m", benchmark), 900, load)


def _bench_trend(benchmark: str = "SPY") -> str | None:
    def load():
        try:
            return indicator_snapshot(get_history(benchmark, period="1y", interval="1d")[0]).get("trend")
        except Exception:
            return None

    return _aux_cached(("trend", benchmark), 900, load)


def _ret_ytd(frame: pd.DataFrame) -> float | None:
    close = series(frame)
    year_start = close[close.index.year == close.index[-1].year]
    if not len(year_start):
        return None
    prior = close[close.index.year < close.index[-1].year]
    base = float(prior.iloc[-1]) if len(prior) else float(year_start.iloc[0])
    return rounded((float(close.iloc[-1]) / base - 1) * 100)


def _analysis_for(symbol: str, period: str, interval: str):
    frame, cached, as_of = get_history(symbol, period=period, interval=interval, auto_adjust=False)
    return frame, indicator_snapshot(frame, interval), as_of, cached


@market_tool
def get_price(ticker: str) -> dict[str, Any]:
    """Latest price only (routed CoinGecko/Finnhub/Alpaca/yfinance). Use get_quote for day range, volume, batches."""
    symbol = ticker_name(ticker)
    result = _safe_call(symbol, lambda: _quote_base(symbol))
    if isinstance(result, dict) and "error" in result:
        return result
    base, as_of, cached = result
    delay = base.get("delay_minutes", YFINANCE_DELAY_MINUTES)
    return _result(
        {
            "price": base.get("price"),
            "ticker": symbol,
            "data_source": base.get("data_source", "yfinance-mcp"),
            "delay_minutes": delay,
            "timestamp": base.get("timestamp") or as_of or now_utc(),
            **{
                field: base[field]
                for field in ("proxy_symbol", "proxy_for", "price_is_proxy")
                if field in base
            },
        },
        as_of,
        cached,
        delayed=delay > 0,
    )


@market_tool
def get_quote(
    ticker: str | None = None,
    symbols: list[str] | None = None,
    fields: list[str] | None = None,
) -> dict[str, Any]:
    """Live quote: price, chg_pct, prev_close, day_high/low, volume, market_state, source, delay. Pass symbols[] for many tickers in one call; fields[] trims output."""
    requested = symbols or ([ticker] if ticker else [])
    if not requested:
        return {"error": "ticker: no data"}
    requested = list(dict.fromkeys(ticker_name(item) for item in requested))
    selected = _validate_fields(fields, QUOTE_FIELDS, DEFAULT_QUOTE_FIELDS)
    if len(requested) == 1:
        symbol = requested[0]
        result = _safe_call(symbol, lambda: _quote_base(symbol))
        if isinstance(result, dict) and "error" in result:
            return result
        base, as_of, cached = result
        return _result(
            {"ticker": symbol, **{field: base.get(field) for field in selected}},
            as_of,
            cached,
            delayed=base.get("delay_minutes", YFINANCE_DELAY_MINUTES) > 0,
        )

    def one(symbol: str):
        return _safe_call(symbol, lambda: _quote_base(symbol))

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(one, requested[:MAX_BATCH_TICKERS]))
    data: dict[str, Any] = {}
    errors: dict[str, str] = {}
    newest = None
    delayed = False
    for symbol, result in zip(requested, results):
        if isinstance(result, dict) and "error" in result:
            errors[symbol] = result["error"]
        else:
            base, as_of, _ = result
            newest = max(filter(None, [newest, as_of]), default=newest)
            delayed = delayed or base.get("delay_minutes", YFINANCE_DELAY_MINUTES) > 0
            data[symbol] = {field: base.get(field) for field in selected}
    output: dict[str, Any] = {"data": data}
    if errors:
        output["errors"] = errors
    return _result(output, newest, delayed=delayed)


@market_tool
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
    """OHLCV bars as column arrays (t,o,h,l,c,v); daily t=date, intraday t=exchange-local time. Trim with fields[], limit=N last bars, summary_only, resample W/M. Use get_analysis for indicators instead of computing from bars."""
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


@market_tool
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
    """get_historical_data for several tickers in one call (max 50). Prefer scan_watchlist or compare when you only need metrics."""
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
            item.pop("as_of", None)
            item.pop("delayed", None)
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


@market_tool
def get_analysis(symbol: str, period: str = "1y", interval: str = "1d") -> dict[str, Any]:
    """Best first call for one ticker. Server-side technical digest replacing many calls: price, SMAs/EMA20, RSI14 (Wilder), MACD hist/state, ATR14, ext_atr (distance from EMA20 in ATRs), 52w range, returns, vol_ratio (vol_proj=true means projected for the unfinished day), nearest 2 support/resistance, trend up/down/range. Intraday: pass interval=15m period=5d (adds session vwap)."""
    result = _safe_call(symbol, lambda: _analysis_for(symbol, period, interval))
    if isinstance(result, dict) and "error" in result:
        return result
    frame, analysis, as_of, cached = result
    analysis.pop("sma50_pos", None)
    analysis.pop("sma200_pos", None)
    macd = analysis.get("macd", {})
    analysis["macd"] = {"hist": macd.get("hist"), "state": macd.get("state")}
    analysis["ticker"] = ticker_name(symbol)
    return _result(analysis, as_of, cached, delayed=not ticker_name(symbol).endswith("-USD"))


SETUP_STYLES = {
    # style: (primary period, primary interval, higher-timeframe period/interval or None)
    "intraday": ("5d", "15m", ("1y", "1d")),
    "swing": ("2y", "1d", ("2y", "1wk")),
    "position": ("5y", "1wk", None),
}


@market_tool
def get_trade_setup(
    symbol: str, style: str = "swing", shorts: bool = False, force: bool = False
) -> dict[str, Any]:
    """Deterministic fresh-entry plan (style intraday|swing|position): bias long/short/none, entry (entry_type=pullback when price is extended: wait for it), stop, invalidation, target, t2, rr, risk_pct, score 0-100, reasons, earn_days when earnings are within 14d. Uses trend, higher-timeframe trend, relative strength vs SPY, MACD, RSI, volume, clustered S/R, earnings risk. Long-only by default (shorts=true to allow: backtests lost money on shorts). style=intraday returns levels only (vwap, ATR, support/resistance) because 15m/1h trend setups backtested with no edge (PF 0.9-1.0 before costs); force=true returns the plan anyway. score is a rule-alignment checklist, not a win probability. Knows nothing about existing positions. bias none = no edge, returns nearby support/resistance only."""
    if style not in SETUP_STYLES:
        return {"error": f"{symbol}: style must be intraday, swing, or position"}
    period, interval, higher = SETUP_STYLES[style]

    def build():
        frame, analysis, as_of, cached = _analysis_for(symbol, period, interval)
        htf = None
        if higher:
            try:
                htf = _analysis_for(symbol, higher[0], higher[1])[1].get("trend")
            except Exception:
                htf = None
        name = ticker_name(symbol)
        rs = None
        if interval == "1d" and not name.endswith("-USD"):
            bench = _bench_ret_3m()
            if bench is not None and analysis.get("ret_3m") is not None:
                rs = analysis["ret_3m"] - bench
        if style == "intraday" and not force:
            return {
                "ticker": name,
                "style": style,
                "bias": "none",
                "score": 30,
                "reasons": ["intraday trend setups backtested with no edge (PF 0.9-1.0, before costs): levels only"],
                **{k: analysis[k] for k in ("vwap", "ema20", "atr14", "support", "resistance", "trend") if k in analysis},
            }, as_of, cached
        earn = _earn_days(name)
        spy_trend = None if name.endswith("-USD") or name == "SPY" else _bench_trend()
        return _build_setup(name, style, frame, analysis, htf, rs, earn, spy_trend, shorts), as_of, cached

    result = _safe_call(symbol, build)
    if isinstance(result, dict) and "error" in result:
        return result
    setup, as_of, cached = result
    return _result(setup, as_of, cached, delayed=not ticker_name(symbol).endswith("-USD"))


# Tuned on a 10y / 47-ticker daily backtest (20-bar hold): stops under ~2 ATR were hit first
# in ~60% of trades; shorts lost money; extended entries (ext_atr>2, RSI>70) underperformed.
SETUP_PARAMS = {
    "long_threshold": 45,
    "short_threshold": -50,
    "min_stop_atr": 2.0,
    "max_stop_atr": 4.0,
    "buffer_atr": 0.5,
    "target_r": 2.0,
    "min_rr": 1.5,
    "allow_short": False,
}


def _build_setup(
    name: str,
    style: str,
    frame: pd.DataFrame,
    a: dict[str, Any],
    htf: str | None,
    rs: float | None,
    earn: int | None,
    spy_trend: str | None = None,
    allow_short: bool | None = None,
) -> dict[str, Any]:
    allow_short = SETUP_PARAMS["allow_short"] if allow_short is None else allow_short
    price = a["price"]
    atr_value = a.get("atr14") or price * 0.02
    trend = a.get("trend")
    macd_state = a.get("macd", {}).get("state")
    rsi_value = a.get("rsi14", 50)
    ext = a.get("ext_atr") or 0
    earn_near = earn is not None and earn <= 14
    earn_field = {"earn_days": earn} if earn_near else {}

    points = {"up": 30, "down": -30}.get(trend, 0)
    points += {"above": 10, "below": -10}.get(a.get("sma200_pos") or a.get("sma50_pos"), 0)
    points += 10 if macd_state in {"bull", "cross_up"} else -10 if macd_state in {"bear", "cross_down"} else 0
    points += 6 if 50 <= rsi_value < 70 else -6 if 30 < rsi_value < 50 else 0
    points += {"up": 15, "down": -15}.get(htf, 0)
    if rs is not None:
        points += 8 if rs > 2 else -8 if rs < -2 else 0
    bias = (
        "long" if points >= SETUP_PARAMS["long_threshold"]
        else "short" if points <= SETUP_PARAMS["short_threshold"]
        else "none"
    )

    def no_edge(reasons: list[str], score: int) -> dict[str, Any]:
        return {
            "ticker": name,
            "style": style,
            "bias": "none",
            "score": min(score, 39),
            "reasons": reasons[:4],
            "support": a.get("support", []),
            "resistance": a.get("resistance", []),
            **({"ema20": a["ema20"]} if a.get("ema20") else {}),
            **earn_field,
        }

    if bias == "none":
        reasons = [f"trend {trend}" + (f", higher tf {htf}" if htf else "") + ": no alignment",
                   f"RSI {rsi_value}, MACD {macd_state}"]
        if earn_near:
            reasons.insert(0, f"earnings in {earn}d")
        return no_edge(reasons, int(20 + abs(points) * 0.5))

    sign = 1 if bias == "long" else -1
    if bias == "short" and not allow_short:
        return no_edge(
            ["downtrend: no long setup", "short setups disabled (backtest PF 0.6); pass shorts=true to override",
             f"RSI {rsi_value}, MACD {macd_state}"],
            25,
        )
    if bias == "short" and spy_trend == "up":
        return no_edge(["short vs uptrending market: skip", f"trend {trend}, RSI {rsi_value}"], 30)

    def plan(entry: float) -> dict[str, float]:
        supports, resistances = swing_levels(frame, atr_value, ref=entry)
        behind = [p for p, _ in (supports if sign > 0 else resistances) if abs(entry - p) >= 0.25 * atr_value]
        ahead = [p for p, _ in (resistances if sign > 0 else supports) if abs(entry - p) >= 0.25 * atr_value]
        structure = behind[0] if behind else entry - sign * 2 * atr_value
        stop = structure - sign * SETUP_PARAMS["buffer_atr"] * atr_value
        risk = abs(entry - stop)
        far_stop = False
        low, high = SETUP_PARAMS["min_stop_atr"] * atr_value, SETUP_PARAMS["max_stop_atr"] * atr_value
        if risk < low:
            stop, risk = entry - sign * low, low
        elif risk > high:
            stop, risk, far_stop = entry - sign * high, high, True
        two_r = entry + sign * SETUP_PARAMS["target_r"] * risk
        # levels closer than 1R are obstacles, not targets
        far = [p for p in ahead if abs(p - entry) >= risk]
        target = (min(far[0], two_r) if sign > 0 else max(far[0], two_r)) if far else two_r
        beyond = [p for p in far if (p > target if sign > 0 else p < target)]
        return {
            "entry": entry,
            "stop": stop,
            "structure": structure,
            "risk": risk,
            "target": target,
            "t2": beyond[0] if beyond else entry + sign * 3 * risk,
            "rr": abs(target - entry) / risk,
            "far_stop": far_stop,
        }

    flags: list[str] = []
    stretched = ext * sign > 1.5 or (rsi_value >= 75 and sign > 0) or (rsi_value <= 25 and sign < 0)
    chosen, entry_type = plan(price), None
    if stretched or chosen["rr"] < SETUP_PARAMS["min_rr"]:
        pullback = plan(a["ema20"]) if a.get("ema20") else None
        if stretched:
            flags.append(f"extended {ext:+.1f} ATR: wait for EMA20")
        else:
            flags.append(f"rr {chosen['rr']:.1f} at market: wait for EMA20")
        if pullback and pullback["rr"] >= SETUP_PARAMS["min_rr"]:
            chosen, entry_type = pullback, "pullback"
        elif stretched or chosen["rr"] < SETUP_PARAMS["min_rr"]:
            return no_edge(flags + [f"pullback rr {pullback['rr']:.1f}" if pullback else "no pullback level"] + [f"trend {trend}"], 35)
    if chosen["far_stop"]:
        flags.append("structure far: ATR stop")

    # Rule-alignment checklist, not a probability: backtests showed no predictive power beyond
    # the extension/earnings penalties, so spread is kept narrow.
    score = 45 + min(abs(points) - 35, 35) * 0.45
    score += 6 if chosen["rr"] >= 2 else 2
    if ext * sign > 2 or (rsi_value >= 70 and sign > 0) or (rsi_value <= 30 and sign < 0):
        score -= 12
    volume = a.get("vol_ratio")
    if volume is not None:
        up_day = (a.get("chg_pct") or 0) * sign > 0
        score += 3 if volume >= 1.2 and up_day else -3 if volume < 0.6 and not a.get("vol_proj") else 0
    if earn_near:
        score -= 30 if earn <= 3 else 20 if earn <= 7 else 8
        flags.append(f"earnings in {earn}d")
    score = int(max(0, min(100, round(score))))
    if score < 40:
        return no_edge(flags + [f"trend {trend}"], score)

    core = [f"trend {trend}" + (f" (htf {htf})" if htf else "")]
    if rs is not None:
        core.append(f"RS vs SPY {rs:+.0f}% 3m")
    core.append(f"RSI {rsi_value}")
    if volume is not None:
        core.append(f"vol {volume}x" + (" proj" if a.get("vol_proj") else ""))
    return {
        "ticker": name,
        "style": style,
        "bias": bias,
        "entry": px(chosen["entry"], price),
        **({"entry_type": entry_type} if entry_type else {}),
        "stop": px(chosen["stop"], price),
        "invalidation": px(chosen["structure"], price),
        "target": px(chosen["target"], price),
        "t2": px(chosen["t2"], price),
        "rr": rounded(chosen["rr"]),
        "risk_pct": rounded(chosen["risk"] / chosen["entry"] * 100),
        "score": score,
        "reasons": (flags + core)[:4],
        **earn_field,
    }


_COMPARATORS = {
    "<": lambda a, b: a < b,
    "<=": lambda a, b: a <= b,
    ">": lambda a, b: a > b,
    ">=": lambda a, b: a >= b,
    "=": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
}


def _parse_where(conditions: list[str] | None) -> list[tuple[str, str, Any]]:
    parsed = []
    for condition in conditions or []:
        match = re.match(r"^\s*(\w+)\s*(<=|>=|!=|<|>|=)\s*(.+?)\s*$", condition)
        if not match or match.group(1) not in SCAN_FIELDS:
            raise ValueError(f"where item '{condition}': use '<field><op><value>' with field in {', '.join(sorted(SCAN_FIELDS))}")
        raw = match.group(3)
        try:
            value: Any = float(raw)
        except ValueError:
            value = raw
        parsed.append((match.group(1), match.group(2), value))
    return parsed


def _sort_key(value: Any, desc: bool):
    # None always last, for either direction
    if value is None:
        return (1, 0)
    return (0, -value if desc and isinstance(value, (int, float)) else value)


@market_tool
def scan_watchlist(
    symbols: list[str] | None = None,
    fields: list[str] | None = None,
    sort_by: str | None = None,
    desc: bool = True,
    limit: int | None = None,
    universe: str | None = None,
    where: list[str] | None = None,
    skip_downtrend: bool = False,
) -> dict[str, Any]:
    """Rank/screen many tickers in ONE call (daily 1y data). skip_downtrend=true is the strategy pre-filter: drops trend=down symbols before the where[] filters and lists them in `skipped_down` (keeps up/range). symbols[] and/or universe (mag7, mega, dow30, ndx100, sectors, crypto, macro). where[] filters, e.g. ["rsi14<35","trend=up","vol_ratio>1.2"]. sort_by any field (default score desc; score = bullishness 0-100, use desc=false for shorts). Fields: price chg_pct rsi14 trend sma50_pos sma200_pos atr_pct vol_ratio dist_high_pct dist_low_pct ret_5d ret_1m ret_3m ret_ytd rs_3m ext_atr score bb_pos (Bollinger %B: <0 below lower band) ema_stack (bull = EMA5>13>20>50). Use limit to cap rows."""
    requested = list(dict.fromkeys(ticker_name(item) for item in (symbols or [])))
    if universe:
        if universe not in UNIVERSES:
            raise ValueError(f"universe must be one of {', '.join(UNIVERSES)}")
        requested += [item for item in UNIVERSES[universe] if item not in requested]
    if not requested:
        return {"error": "symbols or universe required"}
    fields = _validate_fields(fields, SCAN_FIELDS, DEFAULT_SCAN_FIELDS)
    if sort_by is None and universe:
        sort_by = "score"
    if sort_by and sort_by not in SCAN_FIELDS:
        raise ValueError(f"sort_by must be one of {', '.join(sorted(SCAN_FIELDS))}")
    conditions = _parse_where(where)
    if sort_by and sort_by not in fields:
        fields.append(sort_by)

    benchmark = "SPY"
    download = requested + ([benchmark] if benchmark not in requested else [])
    frames, cached, as_of = download_batch(download, period="1y", interval="1d", max_symbols=MAX_SCAN_SYMBOLS)
    bench_frame = frames.get(benchmark)
    bench_ret = None
    if bench_frame is not None and len(series(bench_frame)) > 63:
        close = series(bench_frame)
        bench_ret = (float(close.iloc[-1]) / float(close.iloc[-64]) - 1) * 100

    rows: list[list[Any]] = []
    missing: list[str] = []
    skipped: list[str] = []
    for symbol in requested:
        frame = frames.get(symbol)
        if frame is None or frame.empty or len(series(frame)) < 30:
            missing.append(symbol)
            continue
        metrics = indicator_snapshot(frame)
        if skip_downtrend and metrics.get("trend") == "down":
            skipped.append(symbol)
            continue
        if bench_ret is not None and metrics.get("ret_3m") is not None:
            metrics["rs_3m"] = rounded(metrics["ret_3m"] - bench_ret)
        metrics["ret_ytd"] = _ret_ytd(frame)
        metrics["score"] = bull_score(metrics, metrics.get("rs_3m"))
        if not all(
            metrics.get(field) is not None and _COMPARATORS[op](metrics[field], value)
            for field, op, value in conditions
            if not isinstance(value, str) or op in {"=", "!="}
        ):
            continue
        rows.append([symbol] + [metrics.get(field) for field in fields])
    cols = ["symbol"] + fields
    if sort_by:
        index = cols.index(sort_by)
        rows.sort(key=lambda row: _sort_key(row[index], desc))
    if limit:
        rows = rows[:limit]
    output: dict[str, Any] = {"cols": cols, "rows": rows}
    if skip_downtrend:
        output["skipped_down"] = skipped
        output["scanned"] = len(requested) - len(missing)
    if missing:
        output["no_data"] = missing[:10] if not universe else len(missing)
    return _result(output, as_of, cached)


@market_tool
def market_snapshot() -> dict[str, Any]:
    """One call for market context, replacing 20 quotes: regime (risk_on/risk_off/neutral) with reasons, breadth, indices/VIX/DXY/10y yield/BTC/gold/oil rows, 11 sector ETFs ranked by 1d change. Use first for any market-wide question or before sizing a trade."""
    symbols = SNAPSHOT_SYMBOLS + SECTOR_ETFS
    frames, cached, as_of = download_batch(symbols, period="1y", interval="1d")
    snaps = {
        symbol: indicator_snapshot(frames[symbol])
        for symbol in symbols
        if frames.get(symbol) is not None and not frames[symbol].empty
    }
    rows = [
        [s, a.get("price"), a.get("chg_pct"), a.get("ret_5d"), a.get("ret_1m"), a.get("trend")]
        for s in SNAPSHOT_SYMBOLS
        if (a := snaps.get(s))
    ]
    sectors = [
        [s, a.get("chg_pct"), a.get("ret_5d"), a.get("ret_1m"), a.get("trend")]
        for s in SECTOR_ETFS
        if (a := snaps.get(s))
    ]
    sectors.sort(key=lambda row: row[1] if row[1] is not None else -math.inf, reverse=True)
    above = [a.get("sma50_pos") == "above" for s, a in snaps.items() if s in SECTOR_ETFS]
    breadth = round(sum(above) / len(above) * 100) if above else None

    spy, vix = snaps.get("SPY", {}), snaps.get("^VIX", {})
    points, why = 0, []
    if spy.get("trend") == "up":
        points += 2
        why.append("SPY uptrend")
    elif spy.get("trend") == "down":
        points -= 2
        why.append("SPY downtrend")
    else:
        why.append("SPY range")
    if spy.get("sma50_pos"):
        points += 1 if spy["sma50_pos"] == "above" else -1
    level = vix.get("price")
    if level is not None:
        points += 1 if level < 17 else -2 if level > 24 else -1 if level > 20 else 0
        why.append(f"VIX {level}")
    if (vix.get("ret_5d") or 0) > 20:
        points -= 1
        why.append("VIX spiking")
    if breadth is not None:
        points += 1 if breadth >= 60 else -1 if breadth <= 40 else 0
        why.append(f"{breadth}% sectors > SMA50")
    regime = "risk_on" if points >= 3 else "risk_off" if points <= -2 else "neutral"
    return _result(
        {
            "regime": regime,
            "why": why,
            "breadth_pct": breadth,
            "cols": ["symbol", "price", "chg_pct", "ret_5d", "ret_1m", "trend"],
            "rows": rows,
            "sector_cols": ["symbol", "chg_pct", "ret_5d", "ret_1m", "trend"],
            "sectors": sectors,
        },
        as_of,
        cached,
    )


@market_tool
def compare(
    symbols: list[str],
    period: str = "6mo",
    benchmark: str = "SPY",
) -> dict[str, Any]:
    """Relative comparison of 2-50 tickers vs a benchmark in one call: return, beta, correlation, max drawdown, annualised volatility, plus correlation matrix. Use for pair/rotation/diversification questions."""
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
        summary = stats(frame)
        rows.append(
            [
                symbol,
                summary.get("return_pct"),
                rounded(beta),
                rounded(corr),
                summary.get("max_drawdown_pct"),
                summary.get("volatility_ann_pct"),
            ]
        )
        valid_symbols.append(symbol)
    output: dict[str, Any] = {
        "benchmark": benchmark,
        "cols": ["symbol", "return_pct", "beta", "correlation", "max_drawdown_pct", "volatility_ann_pct"],
        "rows": rows,
    }
    if len(valid_symbols) > 1:
        matrix = pd.DataFrame(
            {symbol: series(frames[symbol]).pct_change() for symbol in valid_symbols}
        ).corr()
        output["correlation"] = {"symbols": valid_symbols, "matrix": matrix.round(2).fillna(0).values.tolist()}
    missing = [symbol for symbol in symbols if symbol not in valid_symbols]
    if missing:
        output["errors"] = {symbol: f"{symbol}: no data" for symbol in missing}
    return _result(output, as_of, cached)


@market_tool
def get_fundamentals_brief(symbol: str) -> dict[str, Any]:
    """Key fundamentals for one stock: market cap ($B), PE, fwd PE, growth, margin, debt/equity, dividend yield %, beta, short % float, analyst target/upside/rating, next earnings date, sector. ETFs/crypto return few fields."""

    def fetch():
        info = yf.Ticker(symbol).info
        price = info.get("currentPrice") or info.get("regularMarketPrice")
        target = info.get("targetMeanPrice")
        dividend_rate = info.get("dividendRate")
        market_cap = info.get("marketCap")
        short = info.get("shortPercentOfFloat")
        earnings = _calendar(ticker_name(symbol)).get("earnings")
        pct = lambda key: rounded(float(info[key]) * 100) if info.get(key) is not None else None
        output = {
            "mcap_b": rounded(market_cap / 1e9, 1) if market_cap else None,
            "pe": rounded(info.get("trailingPE")),
            "fwd_pe": rounded(info.get("forwardPE")),
            "peg": rounded(info.get("trailingPegRatio")),
            "eps_growth": pct("earningsGrowth"),
            "rev_growth": pct("revenueGrowth"),
            "profit_margin": pct("profitMargins"),
            "debt_to_equity": rounded(info.get("debtToEquity")),
            "div_yield": rounded(dividend_rate / price * 100) if dividend_rate and price else None,
            "beta": rounded(info.get("beta")),
            "short_pct": rounded(float(short) * 100) if short is not None else None,
            "analyst_target": rounded(target),
            "target_upside_pct": rounded((target / price - 1) * 100) if target and price else None,
            "analyst_rating": info.get("recommendationKey"),
            "next_earnings": earnings.isoformat() if earnings else None,
            "sector": info.get("sector"),
        }
        return _result({"symbol": ticker_name(symbol), **output})

    return _safe_call(symbol, fetch)


@market_tool
def get_events(symbols: list[str], days: int = 14) -> dict[str, Any]:
    """Upcoming earnings and ex-dividend dates within `days` for many tickers in one call, with in_days. Empty list = none. Check before any entry: earnings inside the holding period change the trade."""
    today = datetime.now(timezone.utc).date()
    end = today + timedelta(days=max(1, min(days, 365)))
    names = list(dict.fromkeys(ticker_name(symbol) for symbol in symbols))

    def one(name: str):
        calendar = _calendar(name)
        events = []
        for kind, key in (("earnings", "earnings"), ("ex_div", "ex_div")):
            stamp = calendar.get(key)
            if stamp and today <= stamp <= end:
                events.append({"type": kind, "date": stamp.isoformat(), "in_days": (stamp - today).days})
        return events

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(one, names))
    return _result({"data": dict(zip(names, results))})


def _pick_expiry(expiries: list[str]) -> tuple[str, int]:
    today = date.today()
    parsed = [(item, (pd.Timestamp(item).date() - today).days) for item in expiries]
    monthly = [(item, dte) for item, dte in parsed if dte >= 14 and 15 <= pd.Timestamp(item).day <= 21]
    if monthly:
        return monthly[0]
    later = [(item, dte) for item, dte in parsed if dte >= 7]
    return later[0] if later else parsed[-1]


@market_tool
def get_options_brief(symbol: str) -> dict[str, Any]:
    """Options context for one ticker at the ~monthly expiry (>=14 days): ATM implied vol, IV vs 20d realised vol (iv_hv) and its 1y percentile (hv_rank), put/call open-interest and volume ratios, expected move (1 sd from IV and from ATM straddle). IV rank is approximate."""

    def fetch():
        name = ticker_name(symbol)
        client = yf.Ticker(name)
        expiries = list(client.options)
        if not expiries:
            return _error(name)
        expiry, dte = _pick_expiry(expiries)
        chain = client.option_chain(expiry)
        price = _quote_base(name)[0].get("price")
        calls, puts = chain.calls, chain.puts
        if price is None or calls.empty or puts.empty:
            return _error(name)
        call = calls.iloc[(calls["strike"] - price).abs().argsort()[:1]].iloc[0]
        put = puts.iloc[(puts["strike"] - price).abs().argsort()[:1]].iloc[0]
        ivs = [float(x) for x in (call["impliedVolatility"], put["impliedVolatility"]) if x and x > 0.01]
        iv = sum(ivs) / len(ivs) if ivs else None

        def mid(row) -> float | None:
            bid, ask = float(row.get("bid") or 0), float(row.get("ask") or 0)
            if bid > 0 and ask > 0:
                return (bid + ask) / 2
            last = float(row.get("lastPrice") or 0)
            return last or None

        straddle = None
        if mid(call) and mid(put):
            straddle = (mid(call) + mid(put)) / price * 100

        closes = series(get_history(name, period="1y", interval="1d")[0])
        hv = (closes.pct_change().rolling(20).std() * math.sqrt(252) * 100).dropna()
        hv_now = float(hv.iloc[-1]) if not hv.empty else None
        hv_rank = float((hv < hv_now).mean() * 100) if hv_now is not None else None

        def ratio(a: pd.Series, b: pd.Series) -> float | None:
            top, bottom = float(a.fillna(0).sum()), float(b.fillna(0).sum())
            return rounded(top / bottom) if bottom else None

        return _result(
            {
                "symbol": name,
                "expiry": expiry,
                "dte": dte,
                "iv_atm": rounded(iv * 100) if iv else None,
                "hv20": rounded(hv_now),
                "iv_hv": rounded(iv * 100 / hv_now) if iv and hv_now else None,
                "hv_rank": rounded(hv_rank, 0),
                "put_call_oi": ratio(puts["openInterest"], calls["openInterest"]),
                "put_call_vol": ratio(puts["volume"], calls["volume"]),
                "expected_move_pct": rounded(iv * math.sqrt(max(dte, 1) / 365) * 100) if iv else None,
                "straddle_pct": rounded(straddle),
            }
        )

    return _safe_call(symbol, fetch)


@market_tool
def get_news_brief(symbol: str, n: int = 3) -> dict[str, Any]:
    """Latest n headlines (max 10) with publisher, time and age in hours. No bodies or links. Use to explain a sharp move or check catalysts."""

    def fetch():
        items = []
        now = datetime.now(timezone.utc)
        for item in yf.Ticker(symbol).news[: max(1, min(n, 10))]:
            content = item.get("content", item)
            published = content.get("pubDate") or content.get("providerPublishTime")
            if isinstance(published, (int, float)):
                published = datetime.fromtimestamp(published, timezone.utc).isoformat()
            age = None
            try:
                age = round((now - pd.Timestamp(published).tz_convert("UTC").to_pydatetime()).total_seconds() / 3600)
            except Exception:
                pass
            items.append(
                drop_nulls(
                    {
                        "headline": content.get("title"),
                        "publisher": (content.get("provider") or {}).get("displayName")
                        if isinstance(content.get("provider"), dict)
                        else content.get("publisher"),
                        "published": published,
                        "age_h": age,
                    }
                )
            )
        return _result({"symbol": ticker_name(symbol), "items": items})

    return _safe_call(symbol, fetch)


def _require_key(name: str) -> str:
    key = os.environ.get(name)
    if not key:
        raise RuntimeError(f"{name} is not configured")
    return key


def _age_hours_exact(value: Any) -> float:
    """Hours since value (epoch seconds or ISO string); inf when unparseable so it sorts last."""
    try:
        if isinstance(value, (int, float)):
            stamp = datetime.fromtimestamp(value, timezone.utc)
        else:
            stamp = pd.Timestamp(value).tz_convert("UTC").to_pydatetime()
        return max(0.0, (datetime.now(timezone.utc) - stamp).total_seconds() / 3600)
    except Exception:
        return math.inf


def _age_hours(value: Any) -> int | None:
    age = _age_hours_exact(value)
    return None if math.isinf(age) else round(age)


def _news_items(rows: list[dict[str, Any]], n: int, summary_chars: int) -> list[dict[str, Any]]:
    """Dedupe by headline, newest first, compact fields. No links or bodies (token savings)."""
    items, seen = [], set()
    for row in sorted(rows, key=lambda item: _age_hours_exact(item["ts"])):
        headline = (row.get("headline") or "").strip()
        key = headline.lower()[:80]
        if not headline or headline == "[Removed]" or key in seen:
            continue
        seen.add(key)
        summary = (row.get("summary") or "").strip()
        if summary == "[Removed]" or summary.lower().startswith(key[:40]):
            summary = ""
        items.append(
            drop_nulls(
                {
                    "headline": headline,
                    "source": row.get("source"),
                    "age_h": _age_hours(row["ts"]) if row["ts"] else None,
                    "summary": summary[:summary_chars] or None,
                }
            )
        )
        if len(items) >= n:
            break
    return items


@market_tool
def get_company_news(symbol: str, days: int = 7, n: int = 8) -> dict[str, Any]:
    """Finnhub company news for a US/CA ticker: last `days` (max 30), n items (max 20), newest first, with source, age_h and a short summary. Richer and longer-range than get_news_brief. Non-US tickers return no items; use get_news_brief or search_news for those."""

    def fetch():
        key = _require_key("FINNHUB_API_KEY")
        name = symbol.upper().strip()
        today = datetime.now(timezone.utc).date()
        start = today - timedelta(days=max(1, min(days, 30)))

        def load():
            return _http_json(
                FINNHUB_COMPANY_NEWS_URL,
                {"symbol": name, "from": start.isoformat(), "to": today.isoformat(), "token": key},
                allow_list=True,
            )

        payload = _aux_cached(("fh-news", name, start), NEWS_CACHE_TTL, load)
        rows = [
            {"headline": row.get("headline"), "source": row.get("source"), "summary": row.get("summary"), "ts": row.get("datetime")}
            for row in payload
            if isinstance(row, dict)
        ]
        items = _news_items(rows, max(1, min(n, 20)), 200)
        output: dict[str, Any] = {"symbol": name, "days": (today - start).days, "count": len(items), "items": items}
        if not items:
            output["note"] = "no Finnhub news; non-US tickers: use get_news_brief or search_news"
        return _result(output, delayed=False)

    return _safe_call(symbol, fetch)


@market_tool
def get_market_news(category: str = "general", n: int = 10) -> dict[str, Any]:
    """Finnhub market-wide news, newest first. category: general, forex, crypto or merger (M&A). n max 25. Use for the day's macro/market narrative before trading, or crypto/forex/M&A flow."""

    def fetch():
        if category not in ("general", "forex", "crypto", "merger"):
            raise ValueError("category must be general, forex, crypto or merger")
        key = _require_key("FINNHUB_API_KEY")
        payload = _aux_cached(
            ("fh-market-news", category),
            NEWS_CACHE_TTL,
            lambda: _http_json(FINNHUB_MARKET_NEWS_URL, {"category": category, "token": key}, allow_list=True),
        )
        rows = [
            {"headline": row.get("headline"), "source": row.get("source"), "summary": row.get("summary"), "ts": row.get("datetime")}
            for row in payload
            if isinstance(row, dict)
        ]
        items = _news_items(rows, max(1, min(n, 25)), 160)
        return _result({"category": category, "count": len(items), "items": items}, delayed=False)

    return _safe_call(category, fetch)


def _newsapi_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if payload.get("status") != "ok":
        raise RuntimeError(f"NewsAPI {payload.get('code') or 'error'}")
    return [
        {
            "headline": row.get("title"),
            "source": (row.get("source") or {}).get("name"),
            "summary": row.get("description"),
            "ts": row.get("publishedAt"),
        }
        for row in payload.get("articles") or []
        if isinstance(row, dict)
    ]


@market_tool
def search_news(
    query: str,
    days: int = 7,
    sort: str = "publishedAt",
    n: int = 8,
    domains: str | None = None,
    language: str = "en",
) -> dict[str, Any]:
    """NewsAPI keyword search across thousands of publishers: companies, products, people, themes, events ("Nvidia export ban", "OPEC cut", "bank run"). Boolean operators and quotes work in query (AND, OR, NOT, "exact phrase"). days max 30; sort publishedAt | relevancy | popularity; n max 20; domains comma list e.g. "reuters.com,bloomberg.com". Free plan: articles are 24 h delayed and quota is 100 calls/day (cached 10 min), so prefer get_company_news / get_market_news for fresh ticker news."""

    def fetch():
        if sort not in ("publishedAt", "relevancy", "popularity"):
            raise ValueError("sort must be publishedAt, relevancy or popularity")
        key = _require_key("NEWSAPI_API_KEY")
        start = (datetime.now(timezone.utc) - timedelta(days=max(1, min(days, 30)))).date().isoformat()
        params = {"q": query, "from": start, "sortBy": sort, "language": language, "pageSize": 30}
        if domains:
            params["domains"] = domains
        payload = _aux_cached(
            ("newsapi-search", tuple(sorted(params.items()))),
            NEWS_CACHE_TTL,
            lambda: _http_json(NEWSAPI_EVERYTHING_URL, params, headers={"X-Api-Key": key}),
        )
        items = _news_items(_newsapi_rows(payload), max(1, min(n, 20)), 200)
        return _result(
            {"query": query, "total": payload.get("totalResults"), "count": len(items), "items": items},
            delayed=True,
        )

    return _safe_call(query, fetch)


@market_tool
def get_top_headlines(category: str = "business", country: str = "us", query: str | None = None, n: int = 10) -> dict[str, Any]:
    """NewsAPI top headlines now. category: business, technology, general, science, health, sports, entertainment. country: 2-letter code (us, gb, de ...). Optional query filters inside the category. n max 20. Quota 100 calls/day (cached 10 min)."""

    def fetch():
        if category not in ("business", "technology", "general", "science", "health", "sports", "entertainment"):
            raise ValueError("invalid category")
        key = _require_key("NEWSAPI_API_KEY")
        params: dict[str, Any] = {"category": category, "country": country.lower(), "pageSize": 30}
        if query:
            params["q"] = query
        payload = _aux_cached(
            ("newsapi-top", tuple(sorted(params.items()))),
            NEWS_CACHE_TTL,
            lambda: _http_json(NEWSAPI_HEADLINES_URL, params, headers={"X-Api-Key": key}),
        )
        items = _news_items(_newsapi_rows(payload), max(1, min(n, 20)), 160)
        return _result({"category": category, "country": country.lower(), "count": len(items), "items": items}, delayed=True)

    return _safe_call(category, fetch)


FRED_UNITS = {"lin", "chg", "ch1", "pch", "pc1", "pca", "cch", "cca", "log"}
# label -> (FRED series id, units transformation, note)
MACRO_SNAPSHOT = {
    "fed_funds_pct": ("DFF", "lin"),
    "us2y_pct": ("DGS2", "lin"),
    "us10y_pct": ("DGS10", "lin"),
    "curve_10y_2y_pp": ("T10Y2Y", "lin"),
    "real_10y_pct": ("DFII10", "lin"),
    "breakeven_10y_pct": ("T10YIE", "lin"),
    "hy_spread_pp": ("BAMLH0A0HYM2", "lin"),
    "cpi_yoy_pct": ("CPIAUCSL", "pc1"),
    "core_cpi_yoy_pct": ("CPILFESL", "pc1"),
    "core_pce_yoy_pct": ("PCEPILFE", "pc1"),
    "unemployment_pct": ("UNRATE", "lin"),
    "payrolls_chg_k": ("PAYEMS", "chg"),
    "jobless_claims": ("ICSA", "lin"),
    "gdp_qoq_ann_pct": ("A191RL1Q225SBEA", "lin"),
}
KEY_RELEASES = (
    "consumer price index",
    "employment situation",
    "gross domestic product",
    "personal income and outlays",
    "producer price index",
    "advance monthly sales for retail",
    "job openings and labor turnover",
    "unemployment insurance weekly claims",
    "fomc press release",
    "industrial production and capacity",
    "employment cost index",
)


def _fred(path: str, params: dict[str, Any]) -> dict[str, Any]:
    key = _require_key("FRED_API_KEY")
    return _http_json(f"{FRED_API_URL}/{path}", {"api_key": key, "file_type": "json", **params})


def _fred_observations(series_id: str, units: str, limit: int) -> list[tuple[str, float]]:
    """Newest first, missing values ('.') dropped."""
    payload = _aux_cached(
        ("fred-obs", series_id, units, limit),
        MACRO_CACHE_TTL,
        lambda: _fred(
            "series/observations",
            {"series_id": series_id, "units": units, "sort_order": "desc", "limit": limit + 5},
        ),
    )
    rows = []
    for row in payload.get("observations") or []:
        try:
            rows.append((row["date"], float(row["value"])))
        except (KeyError, TypeError, ValueError):
            continue
    return rows[:limit]


@market_tool
def get_macro_snapshot() -> dict[str, Any]:
    """FRED macro dashboard in one call: fed funds, 2y/10y yields, 10y-2y curve, real 10y, 10y breakeven, HY spread, CPI/core CPI/core PCE YoY, unemployment, payrolls change (k), jobless claims, real GDP QoQ annualized. Each: v (latest), prev, date. Use to set the macro regime (rates, inflation, labor, credit stress) before judging index or sector trades."""

    def fetch():
        _require_key("FRED_API_KEY")

        def one(item):
            label, (series_id, units) = item
            try:
                rows = _fred_observations(series_id, units, 2)
            except Exception:
                return label, None
            if not rows:
                return label, None
            data = {"v": rounded(rows[0][1], 3), "date": rows[0][0]}
            if len(rows) > 1:
                data["prev"] = rounded(rows[1][1], 3)
            return label, data

        with ThreadPoolExecutor(max_workers=7) as pool:
            results = dict(pool.map(one, MACRO_SNAPSHOT.items()))
        data = {label: value for label, value in results.items() if value}
        if not data:
            raise RuntimeError("FRED returned no data")
        missing = [label for label, value in results.items() if not value]
        return _result({"data": data, "missing": missing or None}, delayed=False)

    return _safe_call("macro", fetch)


@market_tool
def get_macro_series(series_id: str, n: int = 12, units: str = "lin") -> dict[str, Any]:
    """Any FRED series by id (find ids with search_macro_series): title, frequency, units, and the latest n observations oldest first as [date, value] (n max 120). units transform: lin level, chg change, ch1 change vs year ago, pch % change, pc1 % change vs year ago (YoY), pca annualized % change, cch/cca continuously compounded, log. Examples: CPIAUCSL, UNRATE, DGS10, T10Y2Y, ICSA, M2SL, VIXCLS, DCOILWTICO."""

    def fetch():
        if units not in FRED_UNITS:
            raise ValueError(f"units must be one of {', '.join(sorted(FRED_UNITS))}")
        sid = series_id.upper().strip()
        meta_payload = _aux_cached(
            ("fred-meta", sid), 86400, lambda: _fred("series", {"series_id": sid})
        )
        meta = (meta_payload.get("seriess") or [{}])[0]
        rows = _fred_observations(sid, units, max(1, min(n, 120)))
        if not rows:
            raise RuntimeError("no observations")
        values = [rounded(value, 4) for _, value in reversed(rows)]
        output = {
            "series": sid,
            "title": meta.get("title"),
            "freq": meta.get("frequency_short"),
            "units": meta.get("units_short") if units == "lin" else units,
            "seasonal": meta.get("seasonal_adjustment_short"),
            "latest": {"date": rows[0][0], "value": rounded(rows[0][1], 4)},
            "obs": [[date, value] for (date, _), value in zip(reversed(rows), values)],
        }
        if len(rows) > 1:
            output["chg_vs_prev"] = rounded(rows[0][1] - rows[1][1], 4)
        return _result(output, delayed=False)

    return _safe_call(series_id, fetch)


@market_tool
def search_macro_series(query: str, n: int = 8) -> dict[str, Any]:
    """Find FRED series ids by keyword ("core inflation", "10 year treasury", "initial jobless claims"), most popular first. n max 20. Returns id, title, freq, units, last observation date. Feed the id into get_macro_series."""

    def fetch():
        payload = _aux_cached(
            ("fred-search", query.lower(), n),
            86400,
            lambda: _fred(
                "series/search",
                {
                    "search_text": query,
                    "order_by": "popularity",
                    "sort_order": "desc",
                    "limit": max(1, min(n, 20)),
                },
            ),
        )
        items = [
            drop_nulls(
                {
                    "id": row.get("id"),
                    "title": row.get("title"),
                    "freq": row.get("frequency_short"),
                    "units": row.get("units_short"),
                    "last": row.get("observation_end"),
                }
            )
            for row in payload.get("seriess") or []
        ]
        return _result({"query": query, "count": len(items), "items": items}, delayed=False)

    return _safe_call(query, fetch)


@market_tool
def get_economic_calendar(days: int = 14, all_releases: bool = False) -> dict[str, Any]:
    """Upcoming US macro release dates from FRED within `days` (max 60), with in_days: CPI, jobs report, GDP, PCE, PPI, retail sales, JOLTS, jobless claims, FOMC, industrial production, ECI. all_releases=true lists every FRED release (long). Dates only, no consensus or actuals. Check before holding index/rate-sensitive trades through a release."""

    def fetch():
        today = datetime.now(timezone.utc).date()
        end = today + timedelta(days=max(1, min(days, 60)))
        payload = _aux_cached(
            ("fred-calendar", today, end),
            MACRO_CACHE_TTL,
            lambda: _fred(
                "releases/dates",
                {
                    "realtime_start": today.isoformat(),
                    "realtime_end": end.isoformat(),
                    "include_release_dates_with_no_data": "true",
                    "order_by": "release_date",
                    "sort_order": "asc",
                    "limit": 5000,
                },
            ),
        )
        events = []
        for row in payload.get("release_dates") or []:
            name = row.get("release_name") or ""
            if not all_releases and not any(part in name.lower() for part in KEY_RELEASES):
                continue
            try:
                when = date.fromisoformat(row["date"])
            except (KeyError, ValueError):
                continue
            if today <= when <= end:
                events.append({"date": row["date"], "in_days": (when - today).days, "release": name})
        return _result({"count": len(events), "events": events}, delayed=False)

    return _safe_call("calendar", fetch)


def _http_text(url: str, headers: dict[str, str] | None = None, max_bytes: int = 3_000_000) -> str:
    request = UrlRequest(url, headers=headers or {})
    try:
        with urlopen(request, timeout=10) as response:
            return response.read(max_bytes).decode("utf-8", errors="replace")
    except HTTPError as error:
        raise RuntimeError(f"provider HTTP {error.code}") from error
    except (URLError, TimeoutError) as error:
        raise RuntimeError("provider request failed") from error


def _sec_headers() -> dict[str, str]:
    # SEC rejects anonymous agents; set SEC_USER_AGENT="Name email@example.com" in the host secrets.
    return {"User-Agent": os.environ.get("SEC_USER_AGENT") or "Market-Data-MCP admin@example.com"}


def _sec_cik(symbol: str) -> str:
    def load():
        rows = _http_text(SEC_TICKERS_URL, _sec_headers()).splitlines()
        return dict(row.split("\t", 1) for row in rows if "\t" in row)

    table = _aux_cached(("sec-tickers",), 86400, load)
    cik = table.get(symbol.lower().replace(".", "-").strip())
    if not cik:
        raise ValueError("not in SEC ticker list (US-listed filers only)")
    return cik.strip()


def _sec_recent(cik: str) -> list[dict[str, Any]]:
    def load():
        text = _http_text(SEC_SUBMISSIONS_URL.format(cik=cik.zfill(10)), _sec_headers())
        recent = json.loads(text).get("filings", {}).get("recent", {})
        keys = list(recent)
        return [dict(zip(keys, values)) for values in zip(*(recent[key] for key in keys))]

    return _aux_cached(("sec-recent", cik), NEWS_CACHE_TTL, load)


SEC_8K_ITEMS = {
    "1.01": "material agreement",
    "1.02": "agreement terminated",
    "1.03": "bankruptcy",
    "2.01": "acquisition/disposal done",
    "2.02": "earnings results",
    "2.03": "new debt",
    "2.05": "restructuring costs",
    "2.06": "impairment",
    "3.01": "delisting notice",
    "3.02": "unregistered equity sale",
    "4.01": "auditor change",
    "4.02": "financials non-reliance",
    "5.01": "control change",
    "5.02": "officer/director change",
    "5.07": "shareholder vote",
    "7.01": "Reg FD",
    "8.01": "other events",
}


@market_tool
def get_filings(symbol: str, forms: str = "8-K,10-K,10-Q", days: int = 30, n: int = 10) -> dict[str, Any]:
    """SEC EDGAR filings for a US ticker, newest first: form, date, and for 8-K the item meaning (earnings results, officer change, material agreement, bankruptcy, delisting notice ...). Primary-source catalysts, often before headlines. forms: comma list (8-K, 10-K, 10-Q, S-1, SC 13D, 4 ...). days max 365; n max 25. US-listed filers only."""

    def fetch():
        wanted = {form.strip().upper() for form in forms.split(",") if form.strip()}
        cutoff = (datetime.now(timezone.utc) - timedelta(days=max(1, min(days, 365)))).date().isoformat()
        items = []
        for row in _sec_recent(_sec_cik(symbol)):
            if row.get("filingDate", "") < cutoff:
                break
            form = str(row.get("form", "")).upper()
            if wanted and form not in wanted:
                continue
            codes = [code for code in str(row.get("items") or "").split(",") if code and code != "9.01"]
            items.append(
                drop_nulls(
                    {
                        "form": form,
                        "date": row.get("filingDate"),
                        "items": [f"{code} {SEC_8K_ITEMS.get(code, '')}".strip() for code in codes] or None,
                    }
                )
            )
            if len(items) >= max(1, min(n, 25)):
                break
        return _result({"symbol": ticker_name(symbol), "count": len(items), "filings": items}, delayed=False)

    return _safe_call(symbol, fetch)


def _xml_value(node: ET.Element | None, path: str) -> str | None:
    found = node.find(path) if node is not None else None
    return found.text.strip() if found is not None and found.text else None


def _form4_trades(cik: str, row: dict[str, Any]) -> list[dict[str, Any]]:
    document = str(row["primaryDocument"]).split("/")[-1]
    accession = str(row["accessionNumber"]).replace("-", "")
    url = SEC_ARCHIVE_URL.format(cik=int(cik), accession=accession, document=document)
    root = ET.fromstring(_aux_cached(("sec-form4", url), 86400, lambda: _http_text(url, _sec_headers())))
    owner = root.find("reportingOwner")
    relation = owner.find("reportingOwnerRelationship") if owner is not None else None
    role = _xml_value(relation, "officerTitle") or (
        "director" if _xml_value(relation, "isDirector") in ("1", "true")
        else "10% owner" if _xml_value(relation, "isTenPercentOwner") in ("1", "true") else None
    )
    trades = []
    for txn in root.findall("nonDerivativeTable/nonDerivativeTransaction"):
        shares = _xml_value(txn, "transactionAmounts/transactionShares/value")
        price = _xml_value(txn, "transactionAmounts/transactionPricePerShare/value")
        try:
            shares_f, price_f = float(shares or 0), float(price or 0)
        except ValueError:
            continue
        trades.append(
            drop_nulls(
                {
                    "date": _xml_value(txn, "transactionDate/value"),
                    "who": _xml_value(owner, "reportingOwnerId/rptOwnerName"),
                    "role": role,
                    "code": _xml_value(txn, "transactionCoding/transactionCode"),
                    "shares": int(shares_f),
                    "price": rounded(price_f) if price_f else None,
                    "value": int(shares_f * price_f) if price_f else None,
                }
            )
        )
    return trades


@market_tool
def get_insider_trades(symbol: str, days: int = 90, n: int = 12, open_market_only: bool = True) -> dict[str, Any]:
    """SEC Form 4 insider transactions for a US ticker from the last `days` (max 365), newest first: date, who, role, code, shares, price, value. Codes: P open-market buy, S open-market sale, A grant, M option exercise, F tax withholding. open_market_only=true keeps P/S only (the real conviction signal; grants and tax sales are noise). n = Form 4 filings to parse (max 20). Summary gives net open-market buy/sell value. Cluster buying by several insiders is the strongest signal."""

    def fetch():
        cik = _sec_cik(symbol)
        cutoff = (datetime.now(timezone.utc) - timedelta(days=max(1, min(days, 365)))).date().isoformat()
        filings = []
        for row in _sec_recent(cik):
            if row.get("filingDate", "") < cutoff:
                break
            if row.get("form") == "4":
                filings.append(row)
            if len(filings) >= max(1, min(n, 20)):
                break

        def parse(row):
            try:
                return _form4_trades(cik, row)
            except Exception:
                return []

        with ThreadPoolExecutor(max_workers=4) as pool:
            trades = [trade for batch in pool.map(parse, filings) for trade in batch]
        bought = sum(t.get("value", 0) for t in trades if t.get("code") == "P")
        sold = sum(t.get("value", 0) for t in trades if t.get("code") == "S")
        if open_market_only:
            trades = [t for t in trades if t.get("code") in ("P", "S")]
        trades.sort(key=lambda t: t.get("date") or "", reverse=True)
        return _result(
            {
                "symbol": ticker_name(symbol),
                "filings_parsed": len(filings),
                "open_market_bought": bought,
                "open_market_sold": sold,
                "net": bought - sold,
                "trades": trades[:40],
            },
            delayed=False,
        )

    return _safe_call(symbol, fetch)


@market_tool
def get_analyst_view(symbol: str) -> dict[str, Any]:
    """Finnhub analyst recommendation counts (latest month and the month before, with bullish_pct shift) plus the last 4 quarterly earnings vs estimate with surprise_pct. US/CA tickers. Use to judge consensus drift and whether the company habitually beats."""

    def fetch():
        key = _require_key("FINNHUB_API_KEY")
        name = symbol.upper().strip()
        recs = _aux_cached(
            ("fh-rec", name), 3600, lambda: _http_json(FINNHUB_RECOMMENDATION_URL, {"symbol": name, "token": key}, allow_list=True)
        )
        earnings = _aux_cached(
            ("fh-earn", name), 3600, lambda: _http_json(FINNHUB_EARNINGS_URL, {"symbol": name, "token": key}, allow_list=True)
        )

        def summarise(row):
            counts = {k: int(row.get(k) or 0) for k in ("strongBuy", "buy", "hold", "sell", "strongSell")}
            total = sum(counts.values())
            return {
                "period": row.get("period"),
                "strong_buy": counts["strongBuy"],
                "buy": counts["buy"],
                "hold": counts["hold"],
                "sell": counts["sell"],
                "strong_sell": counts["strongSell"],
                "bullish_pct": rounded((counts["strongBuy"] + counts["buy"]) / total * 100, 1) if total else None,
            }

        rows = sorted((r for r in recs if isinstance(r, dict)), key=lambda r: r.get("period") or "", reverse=True)[:2]
        quarters = [
            drop_nulls(
                {
                    "period": row.get("period"),
                    "actual": rounded(row.get("actual"), 3),
                    "estimate": rounded(row.get("estimate"), 3),
                    "surprise_pct": rounded(row.get("surprisePercent"), 1),
                }
            )
            for row in sorted((r for r in earnings if isinstance(r, dict)), key=lambda r: r.get("period") or "", reverse=True)[:4]
        ]
        if not rows and not quarters:
            raise RuntimeError("no analyst data (US/CA tickers only)")
        output: dict[str, Any] = {"symbol": name, "recs": [drop_nulls(summarise(row)) for row in rows], "earnings": quarters}
        if len(output["recs"]) == 2 and None not in (output["recs"][0].get("bullish_pct"), output["recs"][1].get("bullish_pct")):
            output["bullish_shift_pp"] = rounded(output["recs"][0]["bullish_pct"] - output["recs"][1]["bullish_pct"], 1)
        return _result(output, delayed=False)

    return _safe_call(symbol, fetch)


RSS_FEEDS = {
    "fed_press": "https://www.federalreserve.gov/feeds/press_all.xml",
    "fed_monetary": "https://www.federalreserve.gov/feeds/press_monetary.xml",
    "fed_speeches": "https://www.federalreserve.gov/feeds/speeches.xml",
    "ecb_press": "https://www.ecb.europa.eu/rss/press.xml",
    "tagesschau_wirtschaft": "https://www.tagesschau.de/wirtschaft/index~rss2.xml",
    "handelsblatt_finanzen": "https://www.handelsblatt.com/contentexport/feed/finanzen",
    "marketwatch_top": "https://feeds.marketwatch.com/marketwatch/topstories/",
    "cnbc_business": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10001147",
}
CENTRAL_BANK_FEEDS = {
    ("fed", "press"): "fed_press",
    ("fed", "monetary"): "fed_monetary",
    ("fed", "speeches"): "fed_speeches",
    ("ecb", "press"): "ecb_press",
}


def _rss_rows(feed: str) -> list[dict[str, Any]]:
    def load():
        root = ET.fromstring(_http_text(RSS_FEEDS[feed], {"User-Agent": "Market-Data-MCP admin@example.com"}))
        rows = []
        for item in root.iter("item"):
            stamp = None
            try:
                stamp = email.utils.parsedate_to_datetime(item.findtext("pubDate") or "").astimezone(timezone.utc).isoformat()
            except (TypeError, ValueError):
                pass
            summary = re.sub(r"<[^>]+>", " ", html.unescape(item.findtext("description") or ""))
            rows.append(
                {
                    "headline": html.unescape(item.findtext("title") or ""),
                    "source": feed,
                    "summary": re.sub(r"\s+", " ", summary).strip(),
                    "ts": stamp,
                }
            )
        return rows

    return _aux_cached(("rss", feed), NEWS_CACHE_TTL, load)


def _feed_items(feed: str, n: int, query: str | None, summary_chars: int) -> list[dict[str, Any]]:
    rows = _rss_rows(feed)
    if query:
        needle = query.lower()
        rows = [row for row in rows if needle in f"{row['headline']} {row['summary']}".lower()]
    items = _news_items(rows, max(1, min(n, 25)), summary_chars)
    for item in items:
        item.pop("source", None)
    return items


@market_tool
def get_central_bank_news(bank: str = "fed", kind: str = "press", n: int = 5) -> dict[str, Any]:
    """Latest official central-bank publications, newest first. bank=fed: kind press (all releases), monetary (FOMC statements, rate decisions) or speeches. bank=ecb: kind press (decisions, surveys, statements). n max 15. Headlines with a short summary and age_h; use right after a meeting or before a decision. Rate path and tone move every asset."""

    def fetch():
        feed = CENTRAL_BANK_FEEDS.get((bank.lower(), kind.lower()))
        if not feed:
            raise ValueError("use fed/press, fed/monetary, fed/speeches or ecb/press")
        items = _feed_items(feed, min(n, 15), None, 240)
        return _result({"bank": bank.lower(), "kind": kind.lower(), "count": len(items), "items": items}, delayed=False)

    return _safe_call(bank, fetch)


@market_tool
def get_feed_news(feed: str = "handelsblatt_finanzen", n: int = 10, query: str | None = None) -> dict[str, Any]:
    """Free no-key RSS market news. feed: handelsblatt_finanzen (DE, DAX/markets), tagesschau_wirtschaft (DE economy), marketwatch_top (US markets), cnbc_business (US business), plus central-bank feeds fed_press, fed_monetary, fed_speeches, ecb_press. Optional query keeps items whose headline or summary contains it (case-insensitive, e.g. "DAX", "Zinsen"). n max 25. German feeds return German text; use for DAX/European names where Finnhub and NewsAPI are thin."""

    def fetch():
        if feed not in RSS_FEEDS:
            raise ValueError(f"feed must be one of {', '.join(RSS_FEEDS)}")
        items = _feed_items(feed, n, query, 200)
        return _result({"feed": feed, "count": len(items), "items": items}, delayed=False)

    return _safe_call(feed, fetch)


@market_tool
def position_calc(
    positions: list[dict[str, Any]],
    account_size: float | None = None,
) -> dict[str, Any]:
    """Stateless. Positions must be supplied by the caller. No account access. Input [{symbol, qty, avg_cost, stop?}] (negative qty = short). Returns value, pnl, weight and risk-to-stop per position, totals, top weight, total risk to stops, and pairs with 6mo correlation above 0.8."""
    if not positions:
        return {"error": "positions: no data"}
    symbols = list(dict.fromkeys(ticker_name(item.get("symbol", "")) for item in positions))
    frames, _, as_of = download_batch(symbols, period="6mo", interval="1d", log=False)

    def live(symbol: str) -> float | None:
        try:
            return float(_quote_base(symbol)[0]["price"])
        except Exception:
            frame = frames.get(symbol)
            return float(series(frame).iloc[-1]) if frame is not None and not frame.empty else None

    with ThreadPoolExecutor(max_workers=8) as pool:
        prices = dict(zip(symbols, pool.map(live, symbols)))
    rows = []
    total_value = total_pnl = total_cost = total_risk = 0.0
    for item in positions:
        symbol = ticker_name(item.get("symbol", ""))
        price = prices.get(symbol)
        if price is None:
            continue
        qty = float(item.get("qty", 0))
        avg_cost = float(item.get("avg_cost", 0))
        value = price * qty
        pnl = (price - avg_cost) * qty
        total_value += abs(value)
        total_pnl += pnl
        total_cost += abs(avg_cost * qty)
        row = {
            "symbol": symbol,
            "price": rounded(price),
            "value": rounded(value),
            "pnl": rounded(pnl),
            "pnl_pct": rounded(pnl / abs(avg_cost * qty) * 100) if avg_cost and qty else None,
        }
        stop = item.get("stop")
        if stop is not None:
            loss = max(0.0, (price - float(stop)) * qty)
            total_risk += loss
            row["risk_to_stop_pct"] = rounded(abs(price - float(stop)) / price * 100)
            row["risk_to_stop_amount"] = rounded(loss)
        rows.append(row)
    base = account_size or total_value
    for row in rows:
        row["weight_pct"] = rounded(abs(row["value"]) / base * 100) if base else None
    output: dict[str, Any] = {
        "positions": [drop_nulls(row) for row in rows],
        "total_value": rounded(total_value),
        "pnl": rounded(total_pnl),
        "pnl_pct": rounded(total_pnl / total_cost * 100) if total_cost else None,
    }
    if rows and base:
        top = max(rows, key=lambda row: abs(row["value"]))
        output["top_weight"] = {"symbol": top["symbol"], "pct": top["weight_pct"]}
    if total_risk:
        output["risk_to_stops"] = rounded(total_risk)
        if account_size:
            output["risk_to_stops_pct"] = rounded(total_risk / account_size * 100)
    held = [symbol for symbol in symbols if frames.get(symbol) is not None and not frames[symbol].empty]
    if len(held) > 1:
        matrix = pd.DataFrame({symbol: series(frames[symbol]).pct_change() for symbol in held}).corr()
        pairs = [
            [a, b, rounded(matrix.loc[a, b])]
            for index, a in enumerate(held)
            for b in held[index + 1 :]
            if matrix.loc[a, b] > 0.8
        ]
        if pairs:
            output["corr_pairs"] = pairs
    return _result(output, as_of)


@market_tool
def position_size(
    symbol: str,
    stop: float,
    entry: float | None = None,
    risk_amount: float | None = None,
    account_size: float | None = None,
    risk_pct: float | None = None,
    target: float | None = None,
) -> dict[str, Any]:
    """Pure math for a fresh entry; no positions are read or stored. qty = risk / |entry-stop| (whole shares; fractional for -USD), capped so position value never exceeds account_size. Give risk_amount, or account_size + risk_pct. entry defaults to live price. Optional target adds rr and reward. Works for shorts (stop above entry)."""

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
        capped = False
        if account_size and raw_qty * float(live) > account_size:
            raw_qty, capped = account_size / float(live), True
        qty = raw_qty if fractional else math.floor(raw_qty)
        output = {
            "symbol": ticker_name(symbol),
            "side": "long" if float(stop) < float(live) else "short",
            "entry": px(live),
            "stop": px(stop, live),
            "qty": rounded(qty, 6) if fractional else int(qty),
            "position_value": rounded(qty * live),
            "risk_amount": rounded(qty * per_unit),
            "capped_by_account": True if capped else None,
            "pct_of_account": rounded(qty * live / account_size * 100) if account_size else None,
        }
        if target is not None:
            output["rr"] = rounded(abs(float(target) - float(live)) / per_unit)
            output["reward_amount"] = rounded(qty * abs(float(target) - float(live)))
        return _result(output, as_of)

    return _safe_call(symbol, calculate)


@mcp.custom_route("/", methods=["GET"])
async def homepage(_: Request) -> PlainTextResponse:
    return PlainTextResponse("Private market data MCP server for claude.\n")


@mcp.custom_route("/healthz", methods=["GET"])
async def healthcheck(_: Request) -> JSONResponse:
    return JSONResponse({"status": "ok", "mcp_endpoint": "/mcp"})


if __name__ == "__main__":
    # Prefect Horizon imports `mcp` from this file and ignores this block.
    mcp.run(transport="http", host="0.0.0.0", port=PORT, path="/mcp", stateless_http=True, json_response=True)
