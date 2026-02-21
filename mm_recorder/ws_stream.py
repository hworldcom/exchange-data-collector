import asyncio
import contextlib
import json
import logging
import os
import random
import ssl
import time
from decimal import Decimal
from typing import Callable, Optional

from websockets.asyncio.client import connect as ws_connect  # type: ignore
from websockets.exceptions import ConnectionClosed  # type: ignore


class BinanceWSStream:
    """Async websocket wrapper that routes depth/trade messages to callbacks."""

    def __init__(
        self,
        ws_url: str,
        on_depth: Callable[[dict, int], None],
        on_trade: Callable[[dict, int], None],
        on_open: Optional[Callable[[], None]] = None,
        on_message: Optional[Callable[[dict, int], None]] = None,
        on_status: Optional[Callable[[str, dict], None]] = None,
        insecure_tls: bool = False,
        subscribe_messages: Optional[list] = None,
        ping_interval_s: int = 20,
        ping_timeout_s: int = 60,
        reconnect_backoff_s: float = 1.0,
        reconnect_backoff_max_s: float = 30.0,
        max_session_s: float = 23 * 3600 + 50 * 60,
        open_timeout_s: float = 10.0,
        recv_poll_timeout_s: float = 5.0,
        max_queue: int = 256,
    ):
        self.ws_url = ws_url
        self.on_depth = on_depth
        self.on_trade = on_trade
        self.on_open_cb = on_open
        self.on_message_cb = on_message
        self.on_status_cb = on_status
        self.insecure_tls = insecure_tls
        self.subscribe_messages = subscribe_messages or []

        self.ping_interval_s = max(0, int(ping_interval_s))
        self.ping_timeout_s = max(1, int(ping_timeout_s))
        self.reconnect_backoff_s = max(0.0, float(reconnect_backoff_s))
        self.reconnect_backoff_max_s = max(self.reconnect_backoff_s, float(reconnect_backoff_max_s))
        self.max_session_s = max(60.0, float(max_session_s))
        self.open_timeout_s = max(1.0, float(open_timeout_s))
        self.recv_poll_timeout_s = max(0.5, float(recv_poll_timeout_s))
        self.max_queue = max(1, int(max_queue))

        self._ws = None
        self._stop = False
        self._log = logging.getLogger("websocket")
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def _emit_status(self, typ: str, details: dict) -> None:
        try:
            if self.on_status_cb:
                self.on_status_cb(typ, details)
        except Exception:
            self._log.exception("Status callback error (type=%s)", typ)

    async def _ping_loop(self) -> None:
        if self.ping_interval_s <= 0 or self._ws is None:
            return
        while not self._stop:
            try:
                await asyncio.sleep(self.ping_interval_s)
            except asyncio.CancelledError:
                return
            if self._stop or self._ws is None:
                return
            try:
                payload = os.urandom(4)
                pong_waiter = self._ws.ping(payload)
                self._emit_status("ws_ping", {"nbytes": len(payload)})
                await asyncio.wait_for(pong_waiter, timeout=self.ping_timeout_s)
                self._emit_status("ws_pong", {"nbytes": len(payload)})
            except Exception as exc:
                self._emit_status("ws_ping_timeout", {"error": str(exc)})
                try:
                    await self._ws.close()
                except Exception:
                    pass
                return

    async def _read_loop(self, session_deadline: float) -> None:
        assert self._ws is not None
        while not self._stop:
            if time.monotonic() >= session_deadline:
                self._emit_status("ws_session_expired", {"max_session_s": self.max_session_s})
                return

            try:
                msg = await asyncio.wait_for(self._ws.recv(), timeout=self.recv_poll_timeout_s)
            except asyncio.TimeoutError:
                continue
            except ConnectionClosed as exc:
                self._emit_status("ws_close", {"code": getattr(exc, "code", None), "msg": str(exc)})
                return
            except Exception as exc:
                self._emit_status("ws_error", {"error": str(exc)})
                return

            if msg is None:
                return

            recv_ms = int(time.time() * 1000)
            try:
                if self.on_message_cb is not None:
                    payload = json.loads(msg, parse_float=Decimal)
                else:
                    payload = json.loads(msg)
            except Exception:
                self._log.exception("Failed to parse WS message")
                continue

            stream = payload.get("stream", "") if isinstance(payload, dict) else ""
            data = payload.get("data", payload) if isinstance(payload, dict) else payload

            try:
                if self.on_message_cb is not None:
                    # Custom handlers expect the full exchange payload.
                    self.on_message_cb(payload, recv_ms)
                else:
                    if "@depth" in stream or data.get("e") == "depthUpdate":
                        self.on_depth(data, recv_ms)
                    elif "@trade" in stream or data.get("e") == "trade":
                        self.on_trade(data, recv_ms)
            except Exception:
                self._log.exception("Callback error (stream=%s)", stream)

    def _ssl_context(self) -> Optional[ssl.SSLContext]:
        if not self.insecure_tls:
            return None
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    async def _run_async(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._stop = False
        attempt = 0

        while not self._stop:
            attempt += 1
            session_deadline = time.monotonic() + self.max_session_s
            ssl_ctx = self._ssl_context()
            try:
                self._emit_status("ws_connecting", {"attempt": attempt})
                connect_kwargs = {
                    "ping_interval": None,
                    "ping_timeout": None,
                    "close_timeout": 5,
                    "max_queue": self.max_queue,
                    "open_timeout": self.open_timeout_s,
                }
                if ssl_ctx is not None:
                    connect_kwargs["ssl"] = ssl_ctx
                async with ws_connect(self.ws_url, **connect_kwargs) as ws:
                    self._ws = ws
                    if self.on_open_cb:
                        self.on_open_cb()
                    self._emit_status("ws_connect", {"attempt": attempt})

                    for msg in self.subscribe_messages:
                        try:
                            if isinstance(msg, str):
                                await ws.send(msg)
                            else:
                                await ws.send(json.dumps(msg))
                            self._emit_status("ws_subscribe", {"message": msg})
                        except Exception as exc:
                            self._emit_status("ws_subscribe_error", {"error": str(exc)})
                            raise

                    ping_task = asyncio.create_task(self._ping_loop())
                    try:
                        await self._read_loop(session_deadline=session_deadline)
                    finally:
                        ping_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError, Exception):
                            await ping_task
                    close_code = getattr(ws, "close_code", None)
                    close_reason = getattr(ws, "close_reason", None)
                    if close_code is not None or close_reason is not None:
                        self._emit_status("ws_close", {"code": close_code, "msg": close_reason})
            except Exception as exc:
                self._emit_status("ws_run_exception", {"error": str(exc)})
                self._log.exception("WebSocket run exception")
            finally:
                self._ws = None

            if self._stop:
                break

            # Exponential backoff with jitter to respect connection attempt limits.
            base = self.reconnect_backoff_s
            cap = self.reconnect_backoff_max_s
            if base <= 0.0 or cap <= 0.0:
                backoff = 0.0
            else:
                backoff = min(cap, base * (2 ** max(0, attempt - 1)))
                backoff = backoff * (0.7 + 0.6 * random.random())
            self._emit_status("ws_reconnect_wait", {"sleep_s": float(backoff), "attempt": attempt})
            await asyncio.sleep(backoff)

    def run(self) -> None:
        """Run the websocket loop with auto-reconnect."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop is not None:
            raise RuntimeError("BinanceWSStream.run() cannot be called from an active event loop.")
        asyncio.run(self._run_async())

    def run_forever(self) -> None:
        """Backwards-compatible alias for run()."""
        self.run()

    def close(self) -> None:
        self._stop = True
        ws = self._ws
        if ws is None:
            return
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(ws.close())
            return
        except RuntimeError:
            loop = self._loop
        if loop and loop.is_running():
            asyncio.run_coroutine_threadsafe(ws.close(), loop)

    def disconnect(self) -> None:
        """Close the active websocket but allow the reconnect loop to continue."""
        ws = self._ws
        if ws is None:
            return
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(ws.close())
            return
        except RuntimeError:
            loop = self._loop
        if loop and loop.is_running():
            asyncio.run_coroutine_threadsafe(ws.close(), loop)
