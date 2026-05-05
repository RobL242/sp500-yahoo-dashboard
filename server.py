#!/usr/bin/env python3
"""Local static server + Yahoo Finance proxy for S&P 500 leaderboard UI."""

from __future__ import annotations

import csv
import io
import json
import random
import re
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse, quote
from urllib.request import Request, urlopen

HOST = "127.0.0.1"
PORT = 8000
BASE_DIR = Path(__file__).resolve().parent

BENCHMARK_SYMBOL = "VOO"
SP500_CSV_URLS = (
    "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv",
    "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv",
)

# Yahoo: smaller batches + pacing + retries reduce 429 / edge rate limits
SPARK_BATCH_SIZE = 40
SPARK_BATCH_DELAY_SEC = 0.38
SPARK_MAX_RETRIES = 5
SPARK_RETRY_BACKOFF_CAP_SEC = 18.0
SPARK_MAX_POINTS = 80
SPARK_RANGE = "1d"
SPARK_INTERVAL = "1m"

_sp500_cache: list[str] | None = None


def to_yahoo_symbol(raw: str) -> str:
    """S&P CSV uses BRK.B; Yahoo expects BRK-B."""
    return raw.strip().upper().replace(".", "-")


def fetch_sp500_symbols() -> list[str]:
    global _sp500_cache
    if _sp500_cache is not None:
        return _sp500_cache

    last_err: Exception | None = None
    for url in SP500_CSV_URLS:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            with urlopen(req, timeout=20) as resp:
                text = resp.read().decode("utf-8")
        except Exception as exc:
            last_err = exc
            continue
        reader = csv.DictReader(io.StringIO(text))
        if not reader.fieldnames or "Symbol" not in reader.fieldnames:
            last_err = ValueError("CSV missing Symbol column")
            continue
        syms: list[str] = []
        seen: set[str] = set()
        for row in reader:
            sym = to_yahoo_symbol(row.get("Symbol", ""))
            if sym and sym not in seen:
                seen.add(sym)
                syms.append(sym)
        if len(syms) < 400:
            last_err = ValueError(f"Universe too small ({len(syms)} symbols)")
            continue
        _sp500_cache = syms
        return _sp500_cache

    raise RuntimeError(f"Could not load S&P 500 list: {last_err}")


def _session_from_meta(meta: dict) -> dict:
    """Normalize Yahoo meta into UI-friendly session hints (spark/chart)."""
    ms = meta.get("marketState") or meta.get("market_state")
    label = "unknown"
    if isinstance(ms, str):
        u = ms.upper().replace(" ", "_")
        if u == "REGULAR":
            label = "regular"
        elif u in ("PRE", "PRE_MARKET", "PREMARKET"):
            label = "pre"
        elif u in ("POST", "POST_MARKET", "POSTMARKET"):
            label = "post"
        elif u == "CLOSED":
            label = "closed"
    tz = meta.get("exchangeTimezoneName") or ""
    if isinstance(tz, str):
        tz = tz.strip()
    ex = meta.get("fullExchangeName") or meta.get("exchangeName") or ""
    if isinstance(ex, str):
        ex = ex.strip()
    rmt = meta.get("regularMarketTime")
    return {
        "sessionLabel": label,
        "exchangeTimezoneName": tz,
        "regularMarketTime": rmt,
        "exchangeDisplay": ex,
    }


def _downsample(series: list[float], max_points: int) -> list[float]:
    n = len(series)
    if n <= max_points:
        return series
    step = (n - 1) / (max_points - 1)
    return [series[int(round(i * step))] for i in range(max_points)]


def _parse_spark_payload(payload: dict) -> list[dict]:
    rows: list[dict] = []
    for item in payload.get("spark", {}).get("result", []):
        symbol = item.get("symbol")
        block = (item.get("response") or [{}])[0]
        meta = block.get("meta") or {}
        indicators = block.get("indicators") or {}
        quote_row = (indicators.get("quote") or [{}])[0]
        closes = quote_row.get("close") or []
        sparkline: list[float] = []
        for x in closes:
            if x is None:
                continue
            try:
                sparkline.append(float(x))
            except (TypeError, ValueError):
                continue
        sparkline = _downsample(sparkline, SPARK_MAX_POINTS)

        name = (
            (meta.get("shortName") or meta.get("longName") or "")
        )
        if isinstance(name, str):
            name = name.strip()
        else:
            name = ""
        rows.append(
            {
                "symbol": symbol,
                "shortName": name,
                "regularMarketPrice": meta.get("regularMarketPrice"),
                "chartPreviousClose": meta.get("chartPreviousClose"),
                "previousClose": meta.get("previousClose"),
                "sparkline": sparkline,
                "session": _session_from_meta(meta),
            }
        )
    return rows


def fetch_yahoo_spark(symbols: list[str]) -> list[dict]:
    """One Yahoo spark request with retries for rate limits and transient errors."""
    if not symbols:
        return []
    yahoo_url = (
        "https://query1.finance.yahoo.com/v7/finance/spark"
        f"?symbols={quote(','.join(symbols), safe=',')}"
        f"&range={SPARK_RANGE}&interval={SPARK_INTERVAL}"
    )
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

    last_err: Exception | None = None
    for attempt in range(SPARK_MAX_RETRIES):
        if attempt:
            base = min(2.0 ** (attempt - 1), SPARK_RETRY_BACKOFF_CAP_SEC)
            jitter = random.uniform(0.05, 0.35)
            time.sleep(base + jitter)

        req = Request(yahoo_url, headers=headers)
        try:
            with urlopen(req, timeout=35) as resp:
                raw = resp.read().decode("utf-8")
        except HTTPError as exc:
            last_err = exc
            if exc.code in (429, 408, 500, 502, 503, 504):
                continue
            raise
        except URLError as exc:
            last_err = exc
            continue

        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            last_err = exc
            continue

        fin_err = payload.get("finance", {}).get("error")
        if fin_err:
            desc = str(fin_err.get("description") or fin_err)
            code = str(fin_err.get("code") or "").lower()
            last_err = RuntimeError(desc)
            if (
                "unable to access" in desc.lower()
                or "too many" in desc.lower()
                or "rate" in desc.lower()
                or code == "unauthorized"
            ):
                continue
            raise last_err

        if not payload.get("spark"):
            last_err = RuntimeError("Yahoo returned empty spark payload")
            continue

        return _parse_spark_payload(payload)

    raise RuntimeError(f"Yahoo spark failed after {SPARK_MAX_RETRIES} tries: {last_err}")


def fetch_yahoo_spark_split_fallback(symbols: list[str]) -> list[dict]:
    """
    Fetch one batch; if it still fails after retries, split in half and recurse.
    Helps when a single request is too large or intermittently throttled.
    """
    if not symbols:
        return []
    try:
        return fetch_yahoo_spark(symbols)
    except Exception:
        if len(symbols) <= 1:
            raise
        mid = len(symbols) // 2
        left = fetch_yahoo_spark_split_fallback(symbols[:mid])
        time.sleep(SPARK_BATCH_DELAY_SEC)
        right = fetch_yahoo_spark_split_fallback(symbols[mid:])
        return left + right


def pct_change(row: dict) -> float | None:
    last = row.get("regularMarketPrice")
    prev = row.get("chartPreviousClose")
    if prev is None:
        prev = row.get("previousClose")
    try:
        last_f = float(last)
        prev_f = float(prev)
    except (TypeError, ValueError):
        return None
    if prev_f == 0:
        return None
    return (last_f - prev_f) / prev_f * 100.0


def batched(iterable: list[str], size: int):
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


CHART_RANGE_MAP: dict[str, tuple[str, str]] = {
    "1d": ("1d", "5m"),
    "5d": ("5d", "15m"),
    "1mo": ("1mo", "1d"),
}


def _valid_symbol_token(sym: str) -> bool:
    if not sym or len(sym) > 16:
        return False
    return re.fullmatch(r"[A-Z0-9\-]+", sym) is not None


def fetch_yahoo_chart(symbol: str, range_key: str) -> dict:
    """Yahoo v8 chart API for detail panel (closes + meta)."""
    sym = to_yahoo_symbol(symbol)
    if not _valid_symbol_token(sym):
        raise ValueError("Invalid symbol")

    rng, interval = CHART_RANGE_MAP.get(range_key, CHART_RANGE_MAP["5d"])
    url = (
        "https://query1.finance.yahoo.com/v8/finance/chart/"
        f"{quote(sym, safe='')}?range={rng}&interval={interval}"
    )
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

    last_err: Exception | None = None
    for attempt in range(SPARK_MAX_RETRIES):
        if attempt:
            base = min(2.0 ** (attempt - 1), SPARK_RETRY_BACKOFF_CAP_SEC)
            jitter = random.uniform(0.05, 0.35)
            time.sleep(base + jitter)

        req = Request(url, headers=headers)
        try:
            with urlopen(req, timeout=40) as resp:
                raw = resp.read().decode("utf-8")
        except HTTPError as exc:
            last_err = exc
            if exc.code in (429, 408, 500, 502, 503, 504):
                continue
            raise
        except URLError as exc:
            last_err = exc
            continue

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            last_err = exc
            continue

        chart = data.get("chart") or {}
        results = chart.get("result") or []
        if not results:
            err = chart.get("error") or {}
            desc = err.get("description") or "Empty chart result"
            last_err = RuntimeError(str(desc))
            if "rate" in str(desc).lower() or "too many" in str(desc).lower():
                continue
            raise last_err

        res = results[0]
        meta = res.get("meta") or {}
        timestamps = res.get("timestamp") or []
        indicators = res.get("indicators") or {}
        quotes = indicators.get("quote") or [{}]
        closes = (quotes[0] or {}).get("close") if quotes else None
        if not isinstance(closes, list):
            closes = []

        series: list[dict] = []
        n = min(len(timestamps), len(closes))
        for i in range(n):
            c = closes[i]
            if c is None:
                continue
            try:
                series.append({"t": int(timestamps[i]), "c": float(c)})
            except (TypeError, ValueError):
                continue

        prev_close = meta.get("chartPreviousClose")
        if prev_close is None:
            prev_close = meta.get("previousClose")

        detail_meta = {
            "currency": meta.get("currency"),
            "regularMarketPrice": meta.get("regularMarketPrice"),
            "previousClose": prev_close,
            "fiftyTwoWeekHigh": meta.get("fiftyTwoWeekHigh"),
            "fiftyTwoWeekLow": meta.get("fiftyTwoWeekLow"),
            "shortName": meta.get("shortName") or meta.get("longName") or "",
            "session": _session_from_meta(meta),
        }

        return {
            "symbol": sym,
            "range": range_key,
            "yahooRange": rng,
            "interval": interval,
            "series": series,
            "meta": detail_meta,
        }

    raise RuntimeError(f"Yahoo chart failed after {SPARK_MAX_RETRIES} tries: {last_err}")


class AppHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        return

    def _send_json(self, status_code: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_file(self, path: Path, content_type: str) -> None:
        if not path.exists():
            self.send_error(404, "Not found")
            return
        data = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._serve_file(BASE_DIR / "index.html", "text/html; charset=utf-8")
            return

        if parsed.path == "/api/leaderboard":
            query = parse_qs(parsed.query)
            mode = (query.get("mode", ["gainers"])[0] or "gainers").lower()
            if mode not in ("gainers", "losers"):
                self._send_json(400, {"error": "mode must be gainers or losers"})
                return

            try:
                sp500 = fetch_sp500_symbols()
            except Exception as exc:
                self._send_json(502, {"error": f"S&P 500 list failed: {exc}"})
                return

            # Full universe for ranking; benchmark fetched alongside
            rank_set = list(dict.fromkeys(sp500))
            if BENCHMARK_SYMBOL in rank_set:
                rank_set.remove(BENCHMARK_SYMBOL)

            all_for_spark = list(dict.fromkeys([BENCHMARK_SYMBOL, *rank_set]))
            merged: dict[str, dict] = {}
            for i, batch in enumerate(batched(all_for_spark, SPARK_BATCH_SIZE)):
                if i:
                    time.sleep(SPARK_BATCH_DELAY_SEC)
                try:
                    chunk = fetch_yahoo_spark_split_fallback(batch)
                except Exception as exc:
                    self._send_json(502, {"error": f"Yahoo fetch failed: {exc}"})
                    return
                for row in chunk:
                    sym = row.get("symbol")
                    if sym:
                        merged[sym] = row

            bench = merged.get(BENCHMARK_SYMBOL)
            if not bench:
                self._send_json(502, {"error": f"Benchmark {BENCHMARK_SYMBOL} not returned by Yahoo"})
                return

            bench_pct = pct_change(bench)
            if bench_pct is None:
                self._send_json(502, {"error": "Could not compute benchmark % change"})
                return

            candidates: list[tuple[float, dict]] = []
            for sym in rank_set:
                row = merged.get(sym)
                if not row:
                    continue
                p = pct_change(row)
                if p is None:
                    continue
                row_out = {
                    "symbol": sym,
                    "shortName": row.get("shortName") or "",
                    "regularMarketPrice": row["regularMarketPrice"],
                    "chartPreviousClose": row.get("chartPreviousClose"),
                    "previousClose": row.get("previousClose"),
                    "sparkline": row.get("sparkline") or [],
                    "pctChange": round(p, 6),
                    "session": row.get("session") or _session_from_meta({}),
                }
                candidates.append((p, row_out))

            reverse = mode == "gainers"
            candidates.sort(key=lambda t: t[0], reverse=reverse)
            top = [t[1] for t in candidates[:10]]

            benchmark_out = {
                "symbol": BENCHMARK_SYMBOL,
                "shortName": bench.get("shortName") or "",
                "regularMarketPrice": bench["regularMarketPrice"],
                "chartPreviousClose": bench.get("chartPreviousClose"),
                "previousClose": bench.get("previousClose"),
                "sparkline": bench.get("sparkline") or [],
                "pctChange": round(bench_pct, 6),
                "isBenchmark": True,
                "session": bench.get("session") or _session_from_meta({}),
            }

            self._send_json(
                200,
                {
                    "mode": mode,
                    "benchmark": benchmark_out,
                    "rows": top,
                    "universeSize": len(sp500),
                },
            )
            return

        if parsed.path == "/api/quote-detail":
            query = parse_qs(parsed.query)
            sym_raw = (query.get("symbol", [""])[0] or "").strip()
            rng = (query.get("range", ["5d"])[0] or "5d").lower()
            if rng not in CHART_RANGE_MAP:
                rng = "5d"
            if not sym_raw:
                self._send_json(400, {"error": "Missing symbol"})
                return
            try:
                payload = fetch_yahoo_chart(sym_raw, rng)
            except ValueError as exc:
                self._send_json(400, {"error": str(exc)})
                return
            except Exception as exc:
                self._send_json(502, {"error": f"Chart fetch failed: {exc}"})
                return
            self._send_json(200, payload)
            return

        if parsed.path == "/api/stocks":
            query = parse_qs(parsed.query)
            symbols = query.get("symbols", [""])[0].strip()
            if not symbols:
                self._send_json(400, {"error": "Missing symbols parameter"})
                return
            syms = [s.strip() for s in symbols.split(",") if s.strip()]
            rows: list[dict] = []
            try:
                for i, batch in enumerate(batched(syms, SPARK_BATCH_SIZE)):
                    if i:
                        time.sleep(SPARK_BATCH_DELAY_SEC)
                    rows.extend(fetch_yahoo_spark_split_fallback(batch))
            except Exception as exc:
                self._send_json(502, {"error": f"Yahoo fetch failed: {exc}"})
                return
            self._send_json(200, {"result": rows})
            return

        if parsed.path == "/favicon.ico":
            self.send_response(204)
            self.end_headers()
            return

        self.send_error(404, "Not found")


if __name__ == "__main__":
    httpd = HTTPServer((HOST, PORT), AppHandler)
    print(f"Serving on http://{HOST}:{PORT}")
    httpd.serve_forever()
