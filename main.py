import html
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import yfinance as yf


TICKER_SYMBOL = os.environ.get("TICKER", "AAPL").upper()
PORT = int(os.environ.get("PORT", "5000"))


def fetch_quote():
    """Fetch the latest daily quote, returning plain values for the page."""
    history = yf.Ticker(TICKER_SYMBOL).history(period="1d", interval="1d")
    if history.empty:
        raise RuntimeError(f"No market data was returned for {TICKER_SYMBOL}.")

    latest = history.iloc[-1]
    return {
        "symbol": TICKER_SYMBOL,
        "date": str(history.index[-1].date()),
        "open": float(latest["Open"]),
        "high": float(latest["High"]),
        "low": float(latest["Low"]),
        "close": float(latest["Close"]),
        "volume": int(latest["Volume"]),
    }


def format_number(value):
    return f"{value:,.2f}"


def render_page():
    try:
        quote = fetch_quote()
        content = f"""
        <section class="quote-card">
          <div class="quote-heading">
            <div>
              <p class="eyebrow">Latest daily quote</p>
              <h1>{html.escape(quote["symbol"])}</h1>
            </div>
            <div class="price">${format_number(quote["close"])}</div>
          </div>
          <p class="muted">Market date: {html.escape(quote["date"])}</p>
          <dl class="metrics">
            <div><dt>Open</dt><dd>${format_number(quote["open"])}</dd></div>
            <div><dt>High</dt><dd>${format_number(quote["high"])}</dd></div>
            <div><dt>Low</dt><dd>${format_number(quote["low"])}</dd></div>
            <div><dt>Volume</dt><dd>{quote["volume"]:,}</dd></div>
          </dl>
        </section>
        """
    except Exception as error:
        content = f"""
        <section class="quote-card error">
          <p class="eyebrow">Market data unavailable</p>
          <h1>Could not load {html.escape(TICKER_SYMBOL)}</h1>
          <p>{html.escape(str(error))}</p>
          <a href="/">Try again</a>
        </section>
        """

    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{html.escape(TICKER_SYMBOL)} market quote</title>
    <style>
      :root {{ color-scheme: dark; font-family: Inter, ui-sans-serif, system-ui, sans-serif; }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0; min-height: 100vh; display: grid; place-items: center;
        background: #0d1117; color: #f0f6fc; padding: 24px;
      }}
      main {{ width: min(100%, 680px); }}
      .brand {{ color: #79c0ff; font-size: .8rem; font-weight: 700; letter-spacing: .12em; text-transform: uppercase; }}
      h1 {{ margin: 8px 0 0; font-size: clamp(2.5rem, 8vw, 4.5rem); letter-spacing: -.05em; }}
      .quote-card {{
        margin-top: 24px; padding: clamp(24px, 5vw, 40px); border: 1px solid #30363d;
        border-radius: 20px; background: #161b22; box-shadow: 0 20px 60px #0006;
      }}
      .quote-heading {{ display: flex; align-items: end; justify-content: space-between; gap: 20px; }}
      .eyebrow {{ margin: 0; color: #8b949e; font-size: .8rem; font-weight: 700; letter-spacing: .08em; text-transform: uppercase; }}
      .price {{ color: #56d364; font-size: clamp(1.8rem, 5vw, 3rem); font-weight: 700; letter-spacing: -.04em; }}
      .muted {{ color: #8b949e; }}
      .metrics {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin: 32px 0 0; }}
      .metrics div {{ padding: 16px; border-radius: 12px; background: #21262d; }}
      dt {{ color: #8b949e; font-size: .8rem; }}
      dd {{ margin: 7px 0 0; font-weight: 700; }}
      .error {{ border-color: #f85149; }}
      a {{ color: #79c0ff; }}
      @media (max-width: 560px) {{ .quote-heading {{ align-items: start; flex-direction: column; }} .metrics {{ grid-template-columns: repeat(2, 1fr); }} }}
    </style>
  </head>
  <body>
    <main>
      <div class="brand">Market snapshot</div>
      {content}
    </main>
  </body>
</html>"""


class AppHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/healthz":
            body = b"ok\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
        elif self.path == "/":
            body = render_page().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
        else:
            body = b"Not found\n"
            self.send_response(404)
            self.send_header("Content-Type", "text/plain; charset=utf-8")

        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format_string, *args):
        # Keep deployment logs useful without duplicating every request detail.
        print(f"{self.address_string()} - {format_string % args}")


if __name__ == "__main__":
    server = ThreadingHTTPServer(("0.0.0.0", PORT), AppHandler)
    print(f"Market snapshot listening on port {PORT}")
    server.serve_forever()