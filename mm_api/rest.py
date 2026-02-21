from __future__ import annotations

import json
import os
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional
from urllib.parse import parse_qs, urlparse

from mm_api.sources import resolve_latest_paths


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path != "/snapshot":
            self._send_json(404, {"error": "not_found"})
            return

        params = parse_qs(parsed.query)
        exchange = (params.get("exchange", ["binance"])[0] or "binance").lower()
        symbol = params.get("symbol", [None])[0]
        if not symbol:
            self._send_json(400, {"error": "symbol_required"})
            return

        try:
            paths = resolve_latest_paths(exchange, symbol)
        except ValueError as exc:
            self._send_json(400, {"error": f"invalid_params: {exc}"})
            return
        snapshot_path = paths.get("snapshot")
        if not snapshot_path:
            self._send_json(404, {"error": "snapshot_not_found"})
            return

        try:
            with open(snapshot_path, "r", encoding="utf-8") as fh:
                payload = json.load(fh)
        except Exception as exc:
            self._send_json(500, {"error": f"snapshot_read_failed: {exc}"})
            return

        self._send_json(200, {"exchange": exchange, "symbol": symbol, "data": payload})

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return

    def _send_json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    host = os.getenv("REST_HOST", "0.0.0.0")
    port = int(os.getenv("REST_PORT", "8080"))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    server = HTTPServer((host, port), _Handler)
    logging.getLogger("mm_api.rest").info("REST API listening on http://%s:%s", host, port)
    server.serve_forever()


if __name__ == "__main__":
    main()
