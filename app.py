import os
import json
import threading
import time
from collections import deque
from flask import Flask, jsonify, Response, send_file, request, redirect
from flask_cors import CORS
from websocket import WebSocketApp
import websocket as ws_lib

APP_ID      = "33mZo1nXizilnhTUWH6sr"
MARKETS     = ["R_10", "R_25", "R_50", "R_75", "R_100"]
TICK_WINDOW = 50

data_lock   = threading.Lock()
account_info = {"balance": None, "currency": "USD", "loginid": None}
market_data = {
    m: {
        "connected": False,
        "ticks": deque(maxlen=TICK_WINDOW),
        "digit_counts": [0] * 10,
        "last_digit": None,
        "signal": None,
        "score": 0,
        "under_pct": 0,
        "over_pct": 0,
        "hot_digit": None,
        "cold_digit": None,
        "hot_pct": 0,
        "cold_pct": 0,
        "total_ticks": 0,
    }
    for m in MARKETS
}

def analyze(market):
    state  = market_data[market]
    counts = state["digit_counts"]
    total  = sum(counts) or 1
    under  = sum(counts[0:5])
    over   = sum(counts[5:10])
    u_pct  = round(under / total * 100, 1)
    o_pct  = round(over  / total * 100, 1)
    hot_d  = counts.index(max(counts))
    cold_d = counts.index(min(counts))
    h_pct  = round(counts[hot_d]  / total * 100, 1)
    c_pct  = round(counts[cold_d] / total * 100, 1)
    state["under_pct"]  = u_pct
    state["over_pct"]   = o_pct
    state["hot_digit"]  = hot_d
    state["cold_digit"] = cold_d
    state["hot_pct"]    = h_pct
    state["cold_pct"]   = c_pct
    score  = 0
    signal = None
    if u_pct >= 65:
        signal = "UNDER ({}%) | DIGIT {} DUE ({}%)".format(u_pct, cold_d, c_pct)
        score += int(u_pct - 50) * 2
    elif o_pct >= 65:
        signal = "OVER ({}%) | DIGIT {} HOT ({}%)".format(o_pct, hot_d, h_pct)
        score += int(o_pct - 50) * 2
    if h_pct >= 25:
        tag = "DIGIT {} EVEN ({}%)".format(hot_d, h_pct)
        signal = (signal + " | " + tag) if signal else tag
        score += int(h_pct - 10) * 2
    if c_pct == 0:
        tag = "DIGIT {} DUE (0%)".format(cold_d)
        signal = (signal + " | " + tag) if signal else tag
        score += 20
    state["signal"] = signal
    state["score"]  = score

def run_market(market):
    url = "wss://ws.derivws.com/websockets/v3?app_id={}".format(APP_ID)
    def on_open(ws):
        ws.send(json.dumps({"ticks": market, "subscribe": 1}))
    def on_message(ws, msg):
        data = json.loads(msg)
        if data.get("msg_type") != "tick":
            return
        quote = str(data["tick"]["quote"])
        digit = int(quote[-1])
        with data_lock:
            s = market_data[market]
            s["connected"]  = True
            s["last_digit"] = digit
            s["ticks"].append(digit)
            s["digit_counts"][digit] += 1
            s["total_ticks"] += 1
            analyze(market)
    def on_error(ws, err):
        with data_lock:
            market_data[market]["connected"] = False
    def on_close(ws, *args):
        with data_lock:
            market_data[market]["connected"] = False
        time.sleep(3)
        run_market(market)
    WebSocketApp(url, on_open=on_open, on_message=on_message,
                 on_error=on_error, on_close=on_close).run_forever()

def get_best_market():
    with data_lock:
        connected = [m for m in MARKETS if market_data[m]["connected"]]
        return sorted(connected, key=lambda m: market_data[m]["score"], reverse=True)

def start_scanner():
    for market in MARKETS:
        threading.Thread(target=run_market, args=(market,), daemon=True).start()
    while True:
        time.sleep(60)

app = Flask(__name__)
CORS(app)
threading.Thread(target=start_scanner, daemon=True).start()

@app.route("/")
def index():
    return send_file("dashboard.html")

@app.route("/callback")
def oauth_callback():
    """Handle Deriv OAuth callback — get token1 and redirect to dashboard with session."""
    token1 = request.args.get("token1", "")
    token2 = request.args.get("token2", "")
    acct1  = request.args.get("acct1", "")

    if not token1:
        return redirect("/?error=no_token")

    # Verify token and get account info
    name     = "Trader"
    balance  = "0.00"
    currency = "USD"
    loginid  = acct1

    try:
        ws = ws_lib.create_connection(
            "wss://ws.derivws.com/websockets/v3?app_id={}".format(APP_ID),
            timeout=10
        )
        ws.send(json.dumps({"authorize": token1}))
        resp = json.loads(ws.recv())
        ws.close()
        if resp.get("msg_type") == "authorize":
            auth     = resp["authorize"]
            name     = auth.get("fullname") or auth.get("email") or "Trader"
            balance  = str(auth.get("balance", "0.00"))
            currency = auth.get("currency", "USD")
            loginid  = auth.get("loginid", acct1)
    except Exception as e:
        print("OAuth verify error:", e)

    # Redirect to dashboard with token embedded in URL fragment (never hits server)
    html = """<!DOCTYPE html>
<html>
<head><title>Connecting...</title></head>
<body>
<script>
var session = {{
  token: {token},
  name: {name},
  balance: {balance},
  currency: {currency},
  loginid: {loginid},
  ts: Date.now()
}};
sessionStorage.setItem('venom_session', JSON.stringify(session));
window.location.href = '/';
</script>
<p>Connecting your account...</p>
</body>
</html>""".format(
        token=json.dumps(token1),
        name=json.dumps(name),
        balance=json.dumps(balance),
        currency=json.dumps(currency),
        loginid=json.dumps(loginid)
    )
    return html

@app.route("/api/health")
def health():
    connected = sum(1 for m in MARKETS if market_data[m]["connected"])
    return jsonify({"status": "ok", "markets_connected": connected})

@app.route("/api/markets")
def api_markets():
    result = {}
    with data_lock:
        for market in MARKETS:
            s = market_data[market]
            result[market] = {
                "connected":    s["connected"],
                "last_digit":   s.get("last_digit"),
                "signal":       s.get("signal"),
                "score":        s.get("score", 0),
                "under_pct":    s.get("under_pct", 0),
                "over_pct":     s.get("over_pct", 0),
                "hot_digit":    s.get("hot_digit"),
                "cold_digit":   s.get("cold_digit"),
                "hot_pct":      s.get("hot_pct", 0),
                "cold_pct":     s.get("cold_pct", 0),
                "total_ticks":  s.get("total_ticks", 0),
                "digit_counts": list(s["digit_counts"]),
            }
    return jsonify({"markets": result, "ranked": get_best_market()})

@app.route("/api/stream")
def api_stream():
    def event_stream():
        while True:
            result = {}
            with data_lock:
                for market in MARKETS:
                    s = market_data[market]
                    result[market] = {
                        "connected":    s["connected"],
                        "last_digit":   s.get("last_digit"),
                        "signal":       s.get("signal"),
                        "score":        s.get("score", 0),
                        "under_pct":    s.get("under_pct", 0),
                        "over_pct":     s.get("over_pct", 0),
                        "hot_digit":    s.get("hot_digit"),
                        "cold_digit":   s.get("cold_digit"),
                        "total_ticks":  s.get("total_ticks", 0),
                        "digit_counts": list(s["digit_counts"]),
                    }
            ranked = get_best_market()
            payload = json.dumps({"markets": result, "ranked": ranked, "account": account_info})
            yield "data: {}\n\n".format(payload)
            time.sleep(2)
    return Response(event_stream(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False, threaded=True)
