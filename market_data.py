"""Shared yfinance fetching, caching, compact formatting, and indicators."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import math
import threading
import time
from typing import Any

import pandas as pd
import yfinance as yf


MAX_CANDLES = 5_000
MAX_BATCH_TICKERS = 50
INTERVALS = frozenset(
    {"1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo"}
)
PERIODS = frozenset({"1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"})


class NoData(ValueError):
    """A short, user-safe market-data error."""


@dataclass
class CacheEntry:
    value: Any
    expires_at: float
    as_of: str | None


_cache: dict[tuple[Any, ...], CacheEntry] = {}
_cache_lock = threading.RLock()


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def rounded(value: Any, digits: int = 2) -> float | int | None:
    value = clean(value)
    if value is None:
        return None
    return round(float(value), digits)


def ticker_name(ticker: str) -> str:
    if not isinstance(ticker, str) or not ticker.strip():
        raise NoData("ticker: no data")
    return ticker.strip()


def validate_interval(interval: str) -> str:
    if interval not in INTERVALS:
        raise ValueError(f"interval must be one of {', '.join(sorted(INTERVALS))}")
    return interval


def validate_period(period: str | None) -> str | None:
    if period is not None and period not in PERIODS:
        raise ValueError(f"period must be one of {', '.join(sorted(PERIODS))}")
    return period


def validate_limit(limit: int | None) -> int | None:
    if limit is None:
        return None
    if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= MAX_CANDLES:
        raise ValueError(f"limit must be an integer from 1 to {MAX_CANDLES:,}")
    return limit


def _as_of(frame: pd.DataFrame) -> str | None:
    if frame.empty:
        return None
    stamp = frame.index[-1]
    if not isinstance(stamp, pd.Timestamp):
        stamp = pd.Timestamp(stamp)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("UTC")
    else:
        stamp = stamp.tz_convert("UTC")
    return stamp.isoformat()


def _cache_read(key: tuple[Any, ...]) -> tuple[Any, str | None] | None:
    with _cache_lock:
        item = _cache.get(key)
        if item is None:
            return None
        if item.expires_at <= time.monotonic():
            _cache.pop(key, None)
            return None
        value = item.value.copy(deep=True) if isinstance(item.value, pd.DataFrame) else item.value.copy()
        return value, item.as_of


def _cache_write(key: tuple[Any, ...], value: Any, ttl: int, as_of: str | None) -> None:
    with _cache_lock:
        _cache[key] = CacheEntry(value, time.monotonic() + ttl, as_of)


def _history_ttl(interval: str) -> int:
    return 60 if interval.endswith("m") or interval in {"60m", "90m", "1h"} else 900


def _normalize_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    frame = frame.copy()
    if isinstance(frame.columns, pd.MultiIndex):
        frame.columns = [column[-1] for column in frame.columns]
    rename = {str(column).lower(): column for column in frame.columns}
    for required in ("open", "high", "low", "close", "volume"):
        if required not in rename:
            frame[required.title()] = float("nan")
    return frame


def get_history(
    ticker: str,
    period: str | None = "1mo",
    interval: str = "1d",
    start: str | None = None,
    end: str | None = None,
    prepost: bool = False,
    auto_adjust: bool = False,
) -> tuple[pd.DataFrame, bool, str | None]:
    ticker = ticker_name(ticker)
    interval = validate_interval(interval)
    period = validate_period(period)
    if (start or end) and period is not None:
        raise ValueError("use period or start/end, not both")
    if not period and not start and not end:
        period = "1mo"
    key = ("history", ticker, period, interval, start, end, prepost, auto_adjust)
    cached = _cache_read(key)
    if cached:
        return cached[0], True, cached[1]

    kwargs: dict[str, Any] = {
        "interval": interval,
        "prepost": prepost,
        "auto_adjust": auto_adjust,
        "actions": False,
    }
    if period:
        kwargs["period"] = period
    if start:
        kwargs["start"] = start
    if end:
        kwargs["end"] = end
    frame = _normalize_frame(yf.Ticker(ticker).history(**kwargs))
    if frame.empty:
        raise NoData(f"{ticker}: no data")
    as_of = _as_of(frame)
    _cache_write(key, frame, _history_ttl(interval), as_of)
    return frame, False, as_of


def _split_download(frame: pd.DataFrame, symbols: list[str]) -> dict[str, pd.DataFrame]:
    result: dict[str, pd.DataFrame] = {}
    if frame.empty:
        return result
    if not isinstance(frame.columns, pd.MultiIndex):
        if len(symbols) == 1:
            result[symbols[0]] = _normalize_frame(frame)
        return result
    levels = [set(frame.columns.get_level_values(i)) for i in range(frame.columns.nlevels)]
    for symbol in symbols:
        sub: pd.DataFrame | None = None
        for level, values in enumerate(levels):
            if symbol in values:
                sub = frame.xs(symbol, axis=1, level=level, drop_level=True)
                break
        if sub is not None:
            result[symbol] = _normalize_frame(sub)
    return result


def download_batch(
    symbols: list[str],
    period: str = "1y",
    interval: str = "1d",
    auto_adjust: bool = False,
    log: bool = True,
) -> tuple[dict[str, pd.DataFrame], bool, str | None]:
    symbols = [ticker_name(symbol) for symbol in symbols]
    if not symbols:
        raise ValueError("symbols must not be empty")
    if len(symbols) > MAX_BATCH_TICKERS:
        raise ValueError(f"at most {MAX_BATCH_TICKERS} symbols are supported")
    period = validate_period(period) or "1y"
    interval = validate_interval(interval)
    key = ("batch", tuple(symbols), period, interval, auto_adjust)
    cached = _cache_read(key)
    if cached:
        frames, as_of = cached
        return frames, True, as_of

    frame = yf.download(
        tickers=symbols,
        period=period,
        interval=interval,
        group_by="ticker",
        auto_adjust=auto_adjust,
        progress=False,
        threads=True,
        actions=False,
    )
    frames = _split_download(frame, symbols)
    if not frames:
        raise NoData(f"{symbols[0]}: no data")
    newest = max((_as_of(item) for item in frames.values() if not item.empty), default=None)
    _cache_write(key, frames, _history_ttl(interval), newest)
    return frames, False, newest


def _resample(frame: pd.DataFrame, rule: str) -> pd.DataFrame:
    if rule not in {"W", "M"}:
        raise ValueError("resample must be W or M")
    return frame.resample(rule).agg(
        {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}
    )


def compact_history(
    frame: pd.DataFrame,
    fields: list[str] | None = None,
    limit: int | None = None,
    resample: str | None = None,
    summary_only: bool = False,
) -> dict[str, Any]:
    fields = fields or ["o", "h", "l", "c", "v"]
    allowed = {"o": "Open", "h": "High", "l": "Low", "c": "Close", "v": "Volume"}
    if any(field not in allowed for field in fields):
        raise ValueError("fields must be a subset of o,h,l,c,v")
    limit = validate_limit(limit)
    frame = _resample(frame, resample) if resample else frame
    columns = [allowed[field] for field in fields]
    frame = frame.dropna(subset=columns)
    if limit:
        frame = frame.tail(limit)
    if frame.empty:
        raise NoData("no data")

    closes = frame["Close"].dropna()
    result: dict[str, Any] = {
        "as_of": _as_of(frame),
        "delayed": True,
    }
    if summary_only:
        first, last = float(closes.iloc[0]), float(closes.iloc[-1])
        avg_volume = frame["Volume"].dropna().mean() if "Volume" in frame else None
        result.update(
            {
                "open": rounded(frame["Open"].iloc[0]),
                "high": rounded(frame["High"].max()),
                "low": rounded(frame["Low"].min()),
                "close": rounded(last),
                "return_pct": rounded((last / first - 1) * 100) if first else 0,
                "avg_volume": rounded(avg_volume, 0) if avg_volume is not None else None,
                "bars": int(len(frame)),
            }
        )
        return {key: value for key, value in result.items() if value is not None}

    result["t"] = [_as_of(pd.DataFrame(index=[stamp])) for stamp in frame.index]
    for short, column in allowed.items():
        if short in fields:
            digits = 4 if short == "v" else 2
            result[short] = [
                rounded(value, digits) if short != "v" else int(value)
                for value in frame[column].tolist()
            ]
    return result


def series(frame: pd.DataFrame, column: str = "Close") -> pd.Series:
    return pd.to_numeric(frame[column], errors="coerce").dropna()


def sma(values: pd.Series, window: int) -> pd.Series:
    return values.rolling(window).mean()


def ema(values: pd.Series, window: int) -> pd.Series:
    return values.ewm(span=window, adjust=False).mean()


def rsi(values: pd.Series, window: int = 14) -> pd.Series:
    delta = values.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / window, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / window, adjust=False).mean()
    return 100 - (100 / (1 + gain / loss.replace(0, float("nan"))))


def atr(frame: pd.DataFrame, window: int = 14) -> pd.Series:
    high, low, close = frame["High"], frame["Low"], frame["Close"]
    previous = close.shift(1)
    true_range = pd.concat(
        [(high - low), (high - previous).abs(), (low - previous).abs()], axis=1
    ).max(axis=1)
    return true_range.rolling(window).mean()


def indicator_snapshot(frame: pd.DataFrame) -> dict[str, Any]:
    close = series(frame)
    high, low = series(frame, "High"), series(frame, "Low")
    volume = series(frame, "Volume")
    current = float(close.iloc[-1])
    sma20, sma50, sma200 = sma(close, 20), sma(close, 50), sma(close, 200)
    ema20 = ema(close, 20)
    rsi14 = rsi(close)
    macd_line = ema(close, 12) - ema(close, 26)
    macd_signal = ema(macd_line, 9)
    macd_hist = macd_line - macd_signal
    atr14 = atr(frame).dropna()
    high_52 = high.tail(252).max()
    low_52 = low.tail(252).min()
    sma50_last = sma50.dropna().iloc[-1] if not sma50.dropna().empty else None
    sma200_last = sma200.dropna().iloc[-1] if not sma200.dropna().empty else None
    slope = 0
    if len(sma50.dropna()) >= 10:
        slope = float(sma50.dropna().iloc[-1] - sma50.dropna().iloc[-10])
    trend = "range"
    if sma50_last is not None and sma200_last is not None:
        if current > sma50_last > sma200_last and slope > 0:
            trend = "up"
        elif current < sma50_last < sma200_last and slope < 0:
            trend = "down"

    swings = frame.tail(60)
    supports = [
        float(swings["Low"].iloc[i])
        for i in range(2, len(swings) - 2)
        if swings["Low"].iloc[i] < swings["Low"].iloc[i - 1]
        and swings["Low"].iloc[i] < swings["Low"].iloc[i + 1]
        and swings["Low"].iloc[i] < current
    ]
    resistances = [
        float(swings["High"].iloc[i])
        for i in range(2, len(swings) - 2)
        if swings["High"].iloc[i] > swings["High"].iloc[i - 1]
        and swings["High"].iloc[i] > swings["High"].iloc[i + 1]
        and swings["High"].iloc[i] > current
    ]
    supports = sorted(set(supports), reverse=True)[:2]
    resistances = sorted(set(resistances))[:2]
    macd_state = "bull" if float(macd_line.iloc[-1]) >= float(macd_signal.iloc[-1]) else "bear"
    if len(macd_line.dropna()) >= 2 and len(macd_signal.dropna()) >= 2:
        previous_gap = macd_line.iloc[-2] - macd_signal.iloc[-2]
        current_gap = macd_line.iloc[-1] - macd_signal.iloc[-1]
        if previous_gap <= 0 < current_gap:
            macd_state = "cross_up"
        elif previous_gap >= 0 > current_gap:
            macd_state = "cross_down"
    avg_volume = volume.tail(20).mean() if not volume.empty else None
    last_volume = float(volume.iloc[-1]) if not volume.empty else None
    atr_value = float(atr14.iloc[-1]) if not atr14.empty else None
    rsi_value = rsi14.dropna().iloc[-1] if not rsi14.dropna().empty else None
    result = {
        "price": rounded(current),
        "chg_pct": rounded((current / float(close.iloc[-2]) - 1) * 100) if len(close) > 1 else 0,
        "sma20": rounded(sma20.dropna().iloc[-1]) if not sma20.dropna().empty else None,
        "sma50": rounded(sma50_last),
        "sma200": rounded(sma200_last),
        "sma50_pos": "above" if sma50_last and current >= sma50_last else "below",
        "sma200_pos": "above" if sma200_last and current >= sma200_last else "below",
        "ema20": rounded(ema20.iloc[-1]),
        "rsi14": rounded(rsi_value),
        "macd": {
            "line": rounded(macd_line.iloc[-1]),
            "signal": rounded(macd_signal.iloc[-1]),
            "hist": rounded(macd_hist.iloc[-1]),
            "state": macd_state,
        },
        "atr14": rounded(atr_value),
        "atr_pct": rounded(atr_value / current * 100) if atr_value and current else None,
        "high_52w": rounded(high_52),
        "low_52w": rounded(low_52),
        "dist_high_pct": rounded((current / float(high_52) - 1) * 100) if high_52 else None,
        "dist_low_pct": rounded((current / float(low_52) - 1) * 100) if low_52 else None,
        "vol_ratio": rounded(last_volume / avg_volume) if avg_volume else None,
        "support": [rounded(value) for value in supports],
        "resistance": [rounded(value) for value in resistances],
        "trend": trend,
    }
    return drop_nulls(result)


def drop_nulls(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: drop_nulls(item) for key, item in value.items() if item is not None}
    if isinstance(value, list):
        return [drop_nulls(item) for item in value if item is not None]
    return clean(value)


def returns(frame: pd.DataFrame) -> pd.Series:
    return series(frame).pct_change().dropna()


def stats(frame: pd.DataFrame) -> dict[str, Any]:
    values = series(frame)
    daily = values.pct_change().dropna()
    total = (float(values.iloc[-1]) / float(values.iloc[0]) - 1) * 100 if len(values) > 1 else 0
    drawdown = values / values.cummax() - 1
    return {
        "return_pct": rounded(total),
        "max_drawdown_pct": rounded(float(drawdown.min()) * 100),
        "volatility_ann_pct": rounded(float(daily.std() * math.sqrt(252) * 100)) if len(daily) > 1 else 0,
    }