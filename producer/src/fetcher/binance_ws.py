"""
Binance WebSocket stream: subscribe to ticker and yield normalized price events.
Reconnects automatically when the connection drops.
"""
import json
import logging
import queue
import threading
import time
from datetime import datetime, timezone
from typing import Any, Iterator

import websocket

logger = logging.getLogger(__name__)

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"
RECONNECT_DELAY_SEC = 5


def _stream_url(symbols: list[str]) -> str:
    """Build combined stream URL for multiple symbols (e.g. btcusdt@ticker)."""
    streams = [f"{s.lower()}@ticker" for s in symbols]
    return f"{BINANCE_WS_URL}/{'/'.join(streams)}"


def _normalize_ticker(message: dict) -> dict[str, Any] | None:
    """Map Binance ticker to our canonical format."""
    try:
        ts_ms = message.get("E")
        ts_iso = (
            datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z")
            if ts_ms else None
        )
        return {
            "symbol": message.get("s", "").upper(),
            "timestamp": ts_iso,
            "price": float(message.get("c", 0)),
            "volume": float(message.get("v", 0)),
        }
    except (TypeError, ValueError):
        return None


def stream_ticks(symbols: list[str]) -> Iterator[dict[str, Any]]:
    """
    Connect to Binance WebSocket and yield normalized tick events.
    Reconnects automatically when the connection closes (e.g. Binance timeout).
    symbols: e.g. ["btcusdt", "ethusdt"]
    """
    url = _stream_url(symbols)
    q: queue.Queue[dict[str, Any]] = queue.Queue()

    def on_message_q(ws: websocket.WebSocketApp, raw: str) -> None:
        try:
            data = json.loads(raw)
            if "e" in data and data.get("e") == "24hrTicker":
                out = _normalize_ticker(data)
                if out:
                    q.put(out)
        except Exception as e:
            logger.warning("Parse error: %s", e)

    def on_error(ws: websocket.WebSocketApp, err: Exception) -> None:
        logger.error("WebSocket error: %s", err)

    def on_close(ws: websocket.WebSocketApp, close_status_code: int, close_msg: str) -> None:
        logger.warning("WebSocket closed: %s %s", close_status_code, close_msg)

    def run_ws() -> None:
        while True:
            logger.info("Connecting to %s", url)
            try:
                ws = websocket.WebSocketApp(
                    url,
                    on_message=on_message_q,
                    on_error=on_error,
                    on_close=on_close,
                )
                ws.run_forever()
            except Exception as e:
                logger.exception("WebSocket exception: %s", e)
            logger.info("Reconnecting in %ss...", RECONNECT_DELAY_SEC)
            time.sleep(RECONNECT_DELAY_SEC)

    t = threading.Thread(target=run_ws, daemon=True)
    t.start()

    while True:
        try:
            yield q.get(timeout=60)
        except queue.Empty:
            continue
