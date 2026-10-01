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


def price_digits(reference: Any) -> int:
    """Decimals that keep ~4 significant figures: 2 for $336, 4 for EURUSD 1.1712, 9 for SHIB."""
    value = clean(reference)
    if not value:
        return 2
    return max(2, min(10, 4 - math.floor(math.log10(abs(float(value))))))


def px(value: Any, reference: Any = None) -> float | int | None:
    """Round a price to decimals suited to its magnitude (reference defaults to the value)."""
    return rounded(value, price_digits(reference if reference is not None else value))


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
    return _timestamp_iso(frame.index[-1])


def _timestamp_iso(value: Any) -> str:
    stamp = value
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
    max_symbols: int = MAX_BATCH_TICKERS,
) -> tuple[dict[str, pd.DataFrame], bool, str | None]:
    symbols = [ticker_name(symbol) for symbol in dict.fromkeys(symbols)]
    if not symbols:
        raise ValueError("symbols must not be empty")
    if len(symbols) > max_symbols:
        raise ValueError(f"at most {max_symbols} symbols are supported")
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


def _time_labels(index: pd.DatetimeIndex) -> tuple[list[str], str | None]:
    """Short bar labels: date for daily+, exchange-local time for intraday."""
    intraday = bool((index != index.normalize()).any())
    if not intraday:
        return [stamp.strftime("%Y-%m-%d") for stamp in index], None
    long_span = (index[-1] - index[0]).days > 300
    fmt = "%Y-%m-%d %H:%M" if long_span else "%m-%d %H:%M"
    tz = str(index.tz) if getattr(index, "tz", None) is not None else None
    return [stamp.strftime(fmt) for stamp in index], tz


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
                "open": px(frame["Open"].iloc[0], last),
                "high": px(frame["High"].max(), last),
                "low": px(frame["Low"].min(), last),
                "close": px(last),
                "return_pct": rounded((last / first - 1) * 100) if first else 0,
                "avg_volume": rounded(avg_volume, 0) if avg_volume is not None else None,
                "bars": int(len(frame)),
            }
        )
        return {key: value for key, value in result.items() if value is not None}

    labels, tz = _time_labels(frame.index)
    result["t"] = labels
    price_dp = price_digits(closes.iloc[-1])
    if tz:
        result["tz"] = tz
    for short, column in allowed.items():
        if short in fields:
            result[short] = [
                rounded(value, price_dp) if short != "v" else int(value)
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
    """Wilder RSI (matches charting platforms)."""
    delta = values.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / window, adjust=False, min_periods=window).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / window, adjust=False, min_periods=window).mean()
    result = 100 - (100 / (1 + gain / loss.replace(0, float("nan"))))
    return result.where(~((loss == 0) & gain.notna()), 100.0)


def atr(frame: pd.DataFrame, window: int = 14) -> pd.Series:
    """Wilder ATR (matches charting platforms)."""
    high, low, close = frame["High"], frame["Low"], frame["Close"]
    previous = close.shift(1)
    true_range = pd.concat(
        [(high - low), (high - previous).abs(), (low - previous).abs()], axis=1
    ).max(axis=1)
    return true_range.ewm(alpha=1 / window, adjust=False, min_periods=window).mean()


INTRADAY_INTERVALS = frozenset({"1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h"})
BARS_PER_YEAR = {"1d": 252, "5d": 52, "1wk": 52, "1mo": 12, "3mo": 4}
# Cumulative share of a US session's volume normally traded N minutes after the open.
_US_VOLUME_CURVE = [(0, 0.0), (30, 0.13), (60, 0.22), (120, 0.36), (180, 0.50),
                    (240, 0.62), (300, 0.75), (360, 0.90), (390, 1.0)]


def _partial_day_fraction(frame: pd.DataFrame, interval: str) -> float | None:
    """Share of the day's volume expected by now when the last daily bar is still forming."""
    tz = getattr(frame.index, "tz", None)
    if interval != "1d" or frame.empty or tz is None:
        return None
    now = pd.Timestamp.now(tz)
    if frame.index[-1].date() != now.date():
        return None
    name = str(tz)
    if name in {"UTC", "GMT"}:
        fraction = (now.hour * 60 + now.minute) / 1440
    elif name == "America/New_York" and now.weekday() < 5:
        minutes = now.hour * 60 + now.minute - 570
        if minutes <= 0 or minutes >= 395:
            return None
        fraction = _US_VOLUME_CURVE[-1][1]
        for (m0, f0), (m1, f1) in zip(_US_VOLUME_CURVE, _US_VOLUME_CURVE[1:]):
            if m0 <= minutes <= m1:
                fraction = f0 + (f1 - f0) * (minutes - m0) / (m1 - m0)
                break
    else:
        return None
    return fraction if 0.04 <= fraction < 1 else None


def swing_levels(
    frame: pd.DataFrame,
    atr_value: float | None,
    ref: float,
    lookback: int = 120,
    k: int = 2,
) -> tuple[list[tuple[float, int]], list[tuple[float, int]]]:
    """Clustered swing support/resistance around ref: ([(price, touches)] nearest first, same)."""
    sub = frame.tail(lookback)
    highs = sub["High"].to_numpy(dtype=float)
    lows = sub["Low"].to_numpy(dtype=float)
    n = len(sub)
    if n < 2 * k + 1:
        return [], []
    points: set[tuple[float, int]] = set()
    for i in range(k, n - k):
        if highs[i] >= highs[i - k : i + k + 1].max():
            points.add((float(highs[i]), i))
        if lows[i] <= lows[i - k : i + k + 1].min():
            points.add((float(lows[i]), i))
    points.add((float(highs.max()), int(highs.argmax())))
    points.add((float(lows.min()), int(lows.argmin())))
    recent = sub.tail(20)
    points.add((float(recent["High"].max()), n - 20 + int(recent["High"].to_numpy().argmax())))
    points.add((float(recent["Low"].min()), n - 20 + int(recent["Low"].to_numpy().argmin())))
    tolerance = 0.5 * atr_value if atr_value else 0.005 * ref
    clusters: list[list[float]] = []
    for price, _ in sorted(points):
        if clusters and price - (sum(clusters[-1]) / len(clusters[-1])) <= tolerance:
            clusters[-1].append(price)
        else:
            clusters.append([price])
    levels = [(sum(group) / len(group), len(group)) for group in clusters]
    supports = sorted((item for item in levels if item[0] < ref), key=lambda item: -item[0])
    resistances = sorted((item for item in levels if item[0] > ref), key=lambda item: item[0])
    return supports, resistances


def _classify_trend(
    current: float, s20: pd.Series, s50: pd.Series, s200: pd.Series, short_basis: bool = False
) -> tuple[str, str | None]:
    """up/down/range from price vs fast/slow SMA and fast slope; 20/50 when 200 bars are missing
    or the bars are weekly/monthly (a 200-bar weekly SMA is a four-year filter)."""
    fast, slow, basis = s50.dropna(), s200.dropna(), None
    if slow.empty or short_basis:
        fast, slow, basis = s20.dropna(), s50.dropna(), "20/50"
    if fast.empty or slow.empty or len(fast) < 11:
        return "range", basis
    slope = float(fast.iloc[-1] - fast.iloc[-11])
    if current > fast.iloc[-1] > slow.iloc[-1] and slope > 0:
        return "up", basis
    if current < fast.iloc[-1] < slow.iloc[-1] and slope < 0:
        return "down", basis
    return "range", basis


def _vwap(frame: pd.DataFrame) -> float | None:
    if not bool((frame.index != frame.index.normalize()).any()):
        return None
    day = frame[frame.index.date == frame.index[-1].date()]
    volume = pd.to_numeric(day["Volume"], errors="coerce").fillna(0)
    if volume.sum() <= 0:
        return None
    typical = (day["High"] + day["Low"] + day["Close"]) / 3
    return float((typical * volume).sum() / volume.sum())


def indicator_snapshot(frame: pd.DataFrame, interval: str = "1d") -> dict[str, Any]:
    frame = frame.dropna(subset=["High", "Low", "Close"])
    close = series(frame)
    current = float(close.iloc[-1])
    volume = pd.to_numeric(frame["Volume"], errors="coerce").fillna(0)
    sma20, sma50, sma200 = sma(close, 20), sma(close, 50), sma(close, 200)
    ema20 = ema(close, 20)
    ema5, ema13, ema50, ema200 = ema(close, 5), ema(close, 13), ema(close, 50), ema(close, 200)
    bb_mid, bb_std = close.rolling(20).mean(), close.rolling(20).std()
    bb_low, bb_up = bb_mid - 2 * bb_std, bb_mid + 2 * bb_std
    bb_width = float(bb_up.iloc[-1] - bb_low.iloc[-1]) if len(close) >= 20 else 0
    ema_stack = None
    if len(close) >= 50:
        pile = [float(e.iloc[-1]) for e in (ema5, ema13, ema20, ema50)]
        ema_stack = "bull" if pile[0] > pile[1] > pile[2] > pile[3] else "bear" if pile[0] < pile[1] < pile[2] < pile[3] else "mixed"
    rsi14 = rsi(close).dropna()
    macd_line = ema(close, 12) - ema(close, 26)
    macd_signal = ema(macd_line, 9)
    macd_hist = macd_line - macd_signal
    atr14 = atr(frame).dropna()
    atr_value = float(atr14.iloc[-1]) if not atr14.empty else None
    sma20_last = float(sma20.dropna().iloc[-1]) if not sma20.dropna().empty else None
    sma50_last = float(sma50.dropna().iloc[-1]) if not sma50.dropna().empty else None
    sma200_last = float(sma200.dropna().iloc[-1]) if not sma200.dropna().empty else None
    trend, basis = _classify_trend(current, sma20, sma50, sma200, interval in {"1wk", "1mo", "3mo", "5d"})

    per_year = BARS_PER_YEAR.get(interval)
    high_52 = low_52 = None
    if per_year and len(frame) >= per_year * 0.95:
        high_52 = float(frame["High"].tail(per_year).max())
        low_52 = float(frame["Low"].tail(per_year).min())

    supports, resistances = swing_levels(frame, atr_value, current)
    macd_state = "bull" if float(macd_line.iloc[-1]) >= float(macd_signal.iloc[-1]) else "bear"
    if len(macd_line) >= 2:
        previous_gap = macd_line.iloc[-2] - macd_signal.iloc[-2]
        current_gap = macd_line.iloc[-1] - macd_signal.iloc[-1]
        if previous_gap <= 0 < current_gap:
            macd_state = "cross_up"
        elif previous_gap >= 0 > current_gap:
            macd_state = "cross_down"

    vol_ratio, vol_projected = None, False
    if len(volume) > 22 and interval not in INTRADAY_INTERVALS:
        fraction = _partial_day_fraction(frame, interval)
        # weekly/monthly: the last bar is still forming, use the last completed one
        offset = 2 if interval in {"1wk", "1mo", "3mo", "5d"} else 1
        last_volume = float(volume.iloc[-offset])
        baseline = float(volume.iloc[-offset - 20 : -offset].mean())
        if baseline > 0:
            if fraction and offset == 1:
                last_volume, vol_projected = last_volume / fraction, True
            vol_ratio = last_volume / baseline

    def ret(bars: int) -> float | None:
        if interval != "1d" or len(close) <= bars:
            return None
        return (current / float(close.iloc[-1 - bars]) - 1) * 100

    dp = price_digits(current)
    result = {
        "price": rounded(current, dp),
        "chg_pct": rounded((current / float(close.iloc[-2]) - 1) * 100) if len(close) > 1 else 0,
        "sma20": rounded(sma20_last, dp),
        "sma50": rounded(sma50_last, dp),
        "sma200": rounded(sma200_last, dp),
        "sma50_pos": None if sma50_last is None else "above" if current >= sma50_last else "below",
        "sma200_pos": None if sma200_last is None else "above" if current >= sma200_last else "below",
        "ema20": rounded(ema20.iloc[-1], dp),
        "ema5": rounded(ema5.iloc[-1], dp),
        "ema13": rounded(ema13.iloc[-1], dp),
        "ema50": rounded(ema50.iloc[-1], dp) if len(close) >= 50 else None,
        "ema200": rounded(ema200.iloc[-1], dp) if len(close) >= 200 else None,
        "ema_stack": ema_stack,
        "bb_low": rounded(bb_low.iloc[-1], dp) if bb_width else None,
        "bb_up": rounded(bb_up.iloc[-1], dp) if bb_width else None,
        "bb_pos": rounded((current - float(bb_low.iloc[-1])) / bb_width) if bb_width else None,
        "rsi14": rounded(rsi14.iloc[-1]) if not rsi14.empty else None,
        "macd": {
            "line": rounded(macd_line.iloc[-1], dp + 1),
            "signal": rounded(macd_signal.iloc[-1], dp + 1),
            "hist": rounded(macd_hist.iloc[-1], dp + 1),
            "state": macd_state,
        },
        "atr14": rounded(atr_value, dp + 1),
        "atr_pct": rounded(atr_value / current * 100) if atr_value and current else None,
        "ext_atr": rounded((current - float(ema20.iloc[-1])) / atr_value) if atr_value else None,
        "high_52w": rounded(high_52, dp),
        "low_52w": rounded(low_52, dp),
        "dist_high_pct": rounded((current / high_52 - 1) * 100) if high_52 else None,
        "dist_low_pct": rounded((current / low_52 - 1) * 100) if low_52 else None,
        "ret_5d": rounded(ret(5)),
        "ret_1m": rounded(ret(21)),
        "ret_3m": rounded(ret(63)),
        "vol_ratio": rounded(vol_ratio),
        "vol_proj": True if vol_projected else None,
        "vwap": rounded(_vwap(frame), dp),
        "support": [rounded(p, dp) for p, _ in supports if atr_value is None or current - p >= 0.2 * atr_value][:2],
        "resistance": [rounded(p, dp) for p, _ in resistances if atr_value is None or p - current >= 0.2 * atr_value][:2],
        "trend": trend,
        "trend_basis": basis,
    }
    return drop_nulls(result)


def bull_score(a: dict[str, Any], rs: float | None = None) -> int:
    """Deterministic 0-100 bullishness (50 neutral): trend, MAs, MACD, RSI, relative strength."""
    score = 50.0
    score += {"up": 16, "down": -16}.get(a.get("trend"), 0)
    score += {"above": 6, "below": -6}.get(a.get("sma50_pos"), 0)
    score += {"above": 4, "below": -4}.get(a.get("sma200_pos"), 0)
    score += {"bull": 5, "cross_up": 5, "bear": -5, "cross_down": -5}.get(a.get("macd", {}).get("state"), 0)
    rsi_value = a.get("rsi14")
    if rsi_value is not None:
        score += max(-5.0, min(4.0, (rsi_value - 50) / 4)) - (3 if rsi_value >= 75 else 0)
    if rs is not None:
        score += max(-7.0, min(7.0, rs / 3))
    if (a.get("ext_atr") or 0) > 2.5:
        score -= 4
    return int(max(0, min(100, round(score))))


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
