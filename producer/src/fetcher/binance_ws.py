"""
Binance WebSocket stream: subscribe to ticker and yield normalized price events.
"""
import json
import logging
from datetime import datetime, timezone
from typing import Any, Iterator

import websocket

logger = logging.getLogger(__name__)

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"


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
    symbols: e.g. ["btcusdt", "ethusdt"]
    """
    url = _stream_url(symbols)
    logger.info("Connecting to %s", url)

    def on_message(ws: websocket.WebSocketApp, raw: str) -> None:
        try:
            data = json.loads(raw)
            if "e" in data and data.get("e") == "24hrTicker":
                out = _normalize_ticker(data)
                if out:
                    # Store in a queue so the generator can yield; for simplicity we use a list
                    # that the generator will read. Here we use a simpler callback model:
                    # this module exposes a blocking generator that runs the ws in a thread.
                    pass  # handled via queue in run below
        except Exception as e:
            logger.warning("Parse error: %s", e)

    # Use a queue to pass messages from WS thread to generator
    import queue
    q: queue.Queue[dict[str, Any] | None] = queue.Queue()

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
        logger.info("WebSocket closed: %s %s", close_status_code, close_msg)
        q.put(None)  # signal end

    def run_ws() -> None:
        ws = websocket.WebSocketApp(
            url,
            on_message=on_message_q,
            on_error=on_error,
            on_close=on_close,
        )
        ws.run_forever()

    import threading
    t = threading.Thread(target=run_ws, daemon=True)
    t.start()

    while True:
        try:
            item = q.get(timeout=30)
            if item is None:
                break
            yield item
        except queue.Empty:
            continue
