#!/usr/bin/env python3
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse, quote
from urllib.request import Request, urlopen


HOST = "127.0.0.1"
PORT = 8000
BASE_DIR = Path(__file__).resolve().parent


class AppHandler(BaseHTTPRequestHandler):
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

        if parsed.path == "/api/stocks":
            query = parse_qs(parsed.query)
            symbols = query.get("symbols", [""])[0].strip()
            if not symbols:
                self._send_json(400, {"error": "Missing symbols parameter"})
                return

            yahoo_url = (
                "https://query1.finance.yahoo.com/v7/finance/spark"
                f"?symbols={quote(symbols, safe=',')}&range=1d&interval=1m"
            )
            req = Request(yahoo_url, headers={"User-Agent": "Mozilla/5.0"})
            try:
                with urlopen(req, timeout=10) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
            except Exception as exc:
                self._send_json(502, {"error": f"Yahoo fetch failed: {exc}"})
                return

            rows = []
            for item in payload.get("spark", {}).get("result", []):
                meta = (item.get("response") or [{}])[0].get("meta", {})
                rows.append(
                    {
                        "symbol": item.get("symbol"),
                        "regularMarketPrice": meta.get("regularMarketPrice"),
                        "chartPreviousClose": meta.get("chartPreviousClose"),
                        "previousClose": meta.get("previousClose"),
                    }
                )

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
