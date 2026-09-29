"""Remote MCP server for live and historical market data from yfinance."""

from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
import logging
import math
import os
from typing import Any

import yfinance as yf
from mcp.server import MCPServer
from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Mount, Route
import uvicorn


PORT = int(os.environ.get("PORT", "5000"))
MAX_CANDLES = 5_000
MAX_BATCH_TICKERS = 50

SUPPORTED_INTERVALS = frozenset(
    {"1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo"}
)
SUPPORTED_PERIODS = frozenset(
    {"1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"}
)

logger = logging.getLogger("market-mcp")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

mcp = MCPServer(
    name="yfinance-market-data",
    title="yfinance Market Data",
    description="Live and historical market data using ticker symbols understood directly by yfinance.",
    instructions=(
        "Use the exact yfinance ticker symbol supplied by the user. "
        "Do not invent aliases or map company names to symbols. "
        "Yahoo Finance data may be delayed depending on the instrument and exchange."
    ),
    version="1.0.0",
)


def _clean_value(value: Any) -> Any:
    """Convert pandas and numpy scalar values into JSON-safe primitives."""
    if value is None:
        return None
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def _validate_ticker(ticker: str) -> str:
    if not isinstance(ticker, str) or not ticker.strip():
        raise ValueError("ticker must be a non-empty yfinance ticker string.")
    return ticker.strip()


def _validate_interval(interval: str) -> str:
    if interval not in SUPPORTED_INTERVALS:
        allowed = ", ".join(sorted(SUPPORTED_INTERVALS))
        raise ValueError(f"interval must be one of: {allowed}.")
    return interval


def _validate_period(period: str | None) -> str | None:
    if period is not None and period not in SUPPORTED_PERIODS:
        allowed = ", ".join(sorted(SUPPORTED_PERIODS))
        raise ValueError(f"period must be one of: {allowed}.")
    return period


def _validate_candles(candles: int | None) -> int | None:
    if candles is None:
        return None
    if not isinstance(candles, int) or isinstance(candles, bool) or candles < 1:
        raise ValueError("candles must be a positive integer.")
    if candles > MAX_CANDLES:
        raise ValueError(f"candles cannot exceed {MAX_CANDLES:,}.")
    return candles


def _history_request(
    ticker: str,
    interval: str,
    period: str | None,
    start: str | None,
    end: str | None,
    prepost: bool,
    auto_adjust: bool,
    candles: int | None,
) -> dict[str, Any]:
    ticker = _validate_ticker(ticker)
    interval = _validate_interval(interval)
    period = _validate_period(period)
    candles = _validate_candles(candles)

    if (start or end) and period is not None:
        raise ValueError("Use either period or start/end dates, not both.")
    if not period and not start and not end:
        period = "1mo"

    kwargs: dict[str, Any] = {
        "interval": interval,
        "prepost": prepost,
        "auto_adjust": auto_adjust,
        "actions": False,
    }
    if start:
        kwargs["start"] = start
    if end:
        kwargs["end"] = end
    if period:
        kwargs["period"] = period

    logger.info(
        "Fetching history ticker=%s interval=%s period=%s start=%s end=%s candles=%s",
        ticker,
        interval,
        period,
        start,
        end,
        candles,
    )
    history = yf.Ticker(ticker).history(**kwargs)
    if history.empty:
        raise ValueError(
            f"No market data returned for {ticker!r}. Check the exact yfinance ticker "
            "and whether the requested interval/date range is supported."
        )
    if candles:
        history = history.tail(candles)

    rows: list[dict[str, Any]] = []
    for timestamp, row in history.iterrows():
        rows.append(
            {
                "timestamp": _clean_value(timestamp),
                "open": _clean_value(row.get("Open")),
                "high": _clean_value(row.get("High")),
                "low": _clean_value(row.get("Low")),
                "close": _clean_value(row.get("Close")),
                "volume": _clean_value(row.get("Volume")),
            }
        )

    return {
        "ticker": ticker,
        "interval": interval,
        "period": period,
        "start": start,
        "end": end,
        "prepost": prepost,
        "auto_adjust": auto_adjust,
        "count": len(rows),
        "candles": rows,
    }


@mcp.tool()
def get_quote(ticker: str) -> dict[str, Any]:
    """Get the latest available quote for one exact yfinance ticker symbol.

    The result uses the most recent 1-minute candle when available, with a
    daily fallback when the market is closed or 1-minute data is unavailable.
    Yahoo Finance quotes can be delayed depending on the instrument/exchange.
    """
    ticker = _validate_ticker(ticker)
    ticker_client = yf.Ticker(ticker)
    history = ticker_client.history(
        period="1d",
        interval="1m",
        prepost=True,
        auto_adjust=False,
        actions=False,
    )
    source_interval = "1m"
    if history.empty:
        history = ticker_client.history(
            period="5d",
            interval="1d",
            prepost=True,
            auto_adjust=False,
            actions=False,
        )
        source_interval = "1d"
    if history.empty:
        raise ValueError(f"No quote data returned for {ticker!r}.")

    timestamp, latest = history.iloc[-1].name, history.iloc[-1]
    result: dict[str, Any] = {
        "ticker": ticker,
        "timestamp": _clean_value(timestamp),
        "source_interval": source_interval,
        "open": _clean_value(latest.get("Open")),
        "high": _clean_value(latest.get("High")),
        "low": _clean_value(latest.get("Low")),
        "price": _clean_value(latest.get("Close")),
        "volume": _clean_value(latest.get("Volume")),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "data_note": "Yahoo Finance data may be delayed depending on the instrument and exchange.",
    }

    try:
        fast_info = ticker_client.fast_info
        for field in (
            "currency",
            "exchange",
            "timezone",
            "previousClose",
            "marketCap",
            "fiftyDayAverage",
            "twoHundredDayAverage",
            "yearHigh",
            "yearLow",
        ):
            result[field] = _clean_value(fast_info.get(field))
    except Exception as error:
        logger.warning("Optional fast_info unavailable for %s: %s", ticker, error)

    return result


@mcp.tool()
def get_historical_data(
    ticker: str,
    interval: str = "1d",
    period: str | None = "1mo",
    candles: int | None = None,
    start: str | None = None,
    end: str | None = None,
    prepost: bool = False,
    auto_adjust: bool = False,
) -> dict[str, Any]:
    """Get OHLCV candles for one exact yfinance ticker.

    Use yfinance interval names such as 1m, 5m, 15m, 1h, 1d, 1wk, or 1mo.
    Use a yfinance period such as 1d, 5d, 1mo, 1y, ytd, or max, or provide
    ISO start/end dates instead. Set candles to return only the latest N rows.
    """
    return _history_request(
        ticker=ticker,
        interval=interval,
        period=period,
        start=start,
        end=end,
        prepost=prepost,
        auto_adjust=auto_adjust,
        candles=candles,
    )


@mcp.tool()
def get_batch_historical_data(
    tickers: list[str],
    interval: str = "1d",
    period: str | None = "1mo",
    candles: int | None = None,
    start: str | None = None,
    end: str | None = None,
    prepost: bool = False,
    auto_adjust: bool = False,
) -> dict[str, Any]:
    """Get the same OHLCV candle request for multiple exact yfinance tickers.

    Pass ticker strings exactly as yfinance expects, such as ["AAPL", "MSFT"]
    or ["BTC-USD", "EURUSD=X"]. Results are returned under data by ticker;
    one failed ticker does not discard successful results and is reported under
    errors. Batch requests are limited to 50 tickers.
    """
    if not isinstance(tickers, list) or not tickers:
        raise ValueError("tickers must be a non-empty list of yfinance ticker strings.")
    if len(tickers) > MAX_BATCH_TICKERS:
        raise ValueError(f"Batch requests cannot exceed {MAX_BATCH_TICKERS} tickers.")

    normalized_tickers = [_validate_ticker(ticker) for ticker in tickers]
    data: dict[str, Any] = {}
    errors: dict[str, str] = {}
    for ticker in normalized_tickers:
        try:
            data[ticker] = _history_request(
                ticker=ticker,
                interval=interval,
                period=period,
                start=start,
                end=end,
                prepost=prepost,
                auto_adjust=auto_adjust,
                candles=candles,
            )
        except Exception as error:
            errors[ticker] = str(error)

    return {
        "interval": interval,
        "period": period,
        "start": start,
        "end": end,
        "prepost": prepost,
        "auto_adjust": auto_adjust,
        "requested_tickers": normalized_tickers,
        "data": data,
        "errors": errors,
    }


async def homepage(_: Request) -> PlainTextResponse:
    return PlainTextResponse(
        "yfinance market-data MCP server. Connect an MCP client to /mcp.\n"
    )


async def healthcheck(_: Request) -> JSONResponse:
    return JSONResponse({"status": "ok", "mcp_endpoint": "/mcp"})


transport_security = TransportSecuritySettings(
    # Replit's public proxy terminates/forwards requests before the app.
    # The server is read-only market data, so the proxy is the host boundary.
    enable_dns_rebinding_protection=False,
)
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
    routes=[
        Route("/", homepage),
        Route("/healthz", healthcheck),
        Mount("/", app=mcp_http_app),
    ],
    lifespan=lifespan,
)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")