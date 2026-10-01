"""Alpaca real-time trade prices over the market-data websocket.

Alpaca's REST endpoints serve free-plan data at least 15 minutes late; the
websocket (IEX feed) is live. One shared connection runs on a daemon thread,
subscribes lazily per symbol and keeps the latest trade in memory.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any

import websockets

logger = logging.getLogger("market-mcp.alpaca")

STREAM_URL = os.environ.get("ALPACA_STREAM_URL", "wss://stream.data.alpaca.markets/v2/iex")
FIRST_TRADE_WAIT = 3.0
MAX_TRADE_AGE_SECONDS = 15 * 60


def configured() -> bool:
    return bool(os.environ.get("ALPACA_API_KEY") and os.environ.get("ALPACA_API_SECRET"))


class AlpacaStream:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._start_lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ws: Any = None
        self._ready = threading.Event()
        self._subs: set[str] = set()
        self._trades: dict[str, tuple[float, float]] = {}  # symbol -> (price, epoch seconds)
        self._waiters: dict[str, threading.Event] = {}
        self._failed = False

    def _ensure_started(self) -> None:
        with self._start_lock:
            if self._loop is not None:
                return
            loop = asyncio.new_event_loop()
            self._loop = loop
            threading.Thread(target=self._run, args=(loop,), daemon=True, name="alpaca-ws").start()

    def _run(self, loop: asyncio.AbstractEventLoop) -> None:
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._connect_forever())

    async def _connect_forever(self) -> None:
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(STREAM_URL, ping_interval=20, open_timeout=8) as ws:
                    await self._handshake(ws)
                    backoff = 1.0
                    self._ws = ws
                    with self._lock:
                        symbols = sorted(self._subs)
                    if symbols:
                        await ws.send(json.dumps({"action": "subscribe", "trades": symbols}))
                    self._failed = False
                    self._ready.set()
                    async for raw in ws:
                        self._handle(raw)
            except Exception as error:
                logger.warning("alpaca stream down reason=%s", str(error).splitlines()[0][:160])
            self._ws = None
            self._ready.clear()
            self._failed = True
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)

    async def _handshake(self, ws: Any) -> None:
        await ws.recv()  # [{"T":"success","msg":"connected"}]
        await ws.send(
            json.dumps(
                {
                    "action": "auth",
                    "key": os.environ.get("ALPACA_API_KEY", ""),
                    "secret": os.environ.get("ALPACA_API_SECRET", ""),
                }
            )
        )
        for message in json.loads(await ws.recv()):
            if message.get("T") == "error":
                raise RuntimeError(f"auth failed code={message.get('code')}")

    def _handle(self, raw: str | bytes) -> None:
        try:
            messages = json.loads(raw)
        except ValueError:
            return
        for message in messages:
            kind = message.get("T")
            if kind == "t":
                try:
                    stamp = datetime.fromisoformat(message["t"].replace("Z", "+00:00")).timestamp()
                    price = float(message["p"])
                except (KeyError, ValueError, TypeError):
                    continue
                symbol = message.get("S", "")
                with self._lock:
                    previous = self._trades.get(symbol)
                    if previous is None or stamp >= previous[1]:
                        self._trades[symbol] = (price, stamp)
                    waiter = self._waiters.get(symbol)
                if waiter:
                    waiter.set()
            elif kind == "error":
                logger.warning("alpaca stream error code=%s", message.get("code"))

    def latest_trade(self, symbol: str) -> tuple[float, float]:
        """Return (price, epoch seconds) of the newest trade; raise if none arrives in time."""
        if not configured():
            raise RuntimeError("Alpaca API keys are not configured")
        self._ensure_started()
        if not self._ready.wait(FIRST_TRADE_WAIT + 4):
            raise RuntimeError("Alpaca stream not connected")
        with self._lock:
            trade = self._trades.get(symbol)
            new = symbol not in self._subs
            if new:
                self._subs.add(symbol)
                self._waiters[symbol] = threading.Event()
            waiter = self._waiters.get(symbol)
        if new and self._loop and self._ws:
            asyncio.run_coroutine_threadsafe(
                self._ws.send(json.dumps({"action": "subscribe", "trades": [symbol]})),
                self._loop,
            )
        if trade is None and waiter is not None:
            waiter.wait(FIRST_TRADE_WAIT)
            with self._lock:
                trade = self._trades.get(symbol)
        if trade is None:
            raise RuntimeError("Alpaca has no recent trade")
        if time.time() - trade[1] > MAX_TRADE_AGE_SECONDS:
            raise RuntimeError("Alpaca trade is stale")
        return trade


stream = AlpacaStream()
