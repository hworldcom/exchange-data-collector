import asyncio
import time

import mm_recorder.ws_stream as ws_mod


class _FakeWS:
    def __init__(self):
        self.closed = False
        self.close_code = None
        self.close_reason = None

    async def recv(self):
        await asyncio.sleep(1.0)
        return None

    def ping(self, payload: bytes):
        fut = asyncio.get_running_loop().create_future()
        return fut

    async def close(self):
        self.closed = True
        self.close_code = 1000
        self.close_reason = "client_close"


class _FakeConnect:
    def __init__(self, ws: _FakeWS):
        self._ws = ws

    async def __aenter__(self):
        return self._ws

    async def __aexit__(self, exc_type, exc, tb):
        return False


def test_ws_session_expiry_emits_status(monkeypatch):
    events = []
    ws = _FakeWS()

    def on_status(typ, details):
        events.append(typ)
        if typ == "ws_session_expired":
            stream.close()

    def fake_connect(*args, **kwargs):
        return _FakeConnect(ws)

    t = {"v": 0.0}

    def fake_monotonic():
        t["v"] += 0.2
        return t["v"]

    monkeypatch.setattr(ws_mod, "ws_connect", fake_connect)
    monkeypatch.setattr(ws_mod.time, "monotonic", fake_monotonic)

    stream = ws_mod.BinanceWSStream(
        ws_url="wss://example",
        on_depth=lambda *_: None,
        on_trade=lambda *_: None,
        on_status=on_status,
        ping_interval_s=0,
        max_session_s=0.1,
        recv_poll_timeout_s=0.01,
        reconnect_backoff_s=0.0,
        reconnect_backoff_max_s=0.0,
    )
    stream.run()

    assert "ws_session_expired" in events


def test_ws_session_expiry_emits_reconnect_wait(monkeypatch):
    events = []
    ws = _FakeWS()

    def on_status(typ, details):
        events.append(typ)
        if typ == "ws_reconnect_wait":
            stream.close()

    def fake_connect(*args, **kwargs):
        return _FakeConnect(ws)

    t = {"v": 0.0}

    def fake_monotonic():
        t["v"] += 0.2
        return t["v"]

    monkeypatch.setattr(ws_mod, "ws_connect", fake_connect)
    monkeypatch.setattr(ws_mod.time, "monotonic", fake_monotonic)

    stream = ws_mod.BinanceWSStream(
        ws_url="wss://example",
        on_depth=lambda *_: None,
        on_trade=lambda *_: None,
        on_status=on_status,
        ping_interval_s=0,
        max_session_s=0.1,
        recv_poll_timeout_s=0.01,
        reconnect_backoff_s=0.01,
        reconnect_backoff_max_s=0.01,
    )
    stream.run()

    assert "ws_session_expired" in events
    assert "ws_reconnect_wait" in events


def test_ws_ping_timeout_emits_status(monkeypatch):
    events = []
    ws = _FakeWS()

    def fail_ping(payload: bytes):
        stream._stop = True
        raise RuntimeError("boom")

    ws.ping = fail_ping

    def on_status(typ, details):
        events.append(typ)
        if typ == "ws_ping_timeout":
            stream.close()

    def fake_connect(*args, **kwargs):
        return _FakeConnect(ws)

    monkeypatch.setattr(ws_mod, "ws_connect", fake_connect)

    async def fast_sleep(_):
        return None

    monkeypatch.setattr(ws_mod.asyncio, "sleep", fast_sleep)

    stream = ws_mod.BinanceWSStream(
        ws_url="wss://example",
        on_depth=lambda *_: None,
        on_trade=lambda *_: None,
        on_status=on_status,
        ping_interval_s=1,
        ping_timeout_s=1,
        max_session_s=10.0,
        recv_poll_timeout_s=0.01,
        reconnect_backoff_s=0.0,
        reconnect_backoff_max_s=0.0,
    )
    stream._ws = ws
    asyncio.run(stream._ping_loop())

    assert "ws_ping_timeout" in events
