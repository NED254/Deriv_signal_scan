import os
import json
import threading
import time
from collections import deque
from flask import Flask, jsonify, Response, send_file
from flask_cors import CORS
from websocket import WebSocketApp

APP_ID      = "33mZo1nXizilnhTUWH6sr"
API_TOKEN   = "33mZo1nXizilnhTUWH6sr"
MARKETS     = ["R_10", "R_25", "R_50", "R_75", "R_100"]
TICK_WINDOW = 50

data_lock    = threading.Lock()
account_info = {"balance": None, "currency": "USD", "loginid": None}
market_data  = {
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
        signal = f"UNDER ({u_pct}%) | DIGIT {cold_d} DUE ({c_pct}%)"
        score += int(u_pct - 50) * 2
    elif o_pct >= 65:
        signal = f"OVER ({o_pct}%) | DIGIT {hot_d} HOT ({h_pct}%)"
        score += int(o_pct - 50) * 2
    if h_pct >= 25:
        tag = f"DIGIT {hot_d} EVEN ({h_pct}%)"
        signal = (signal + " | " + tag) if signal else tag
        score += int(h_pct - 10) * 2
    if c_pct == 0:
        tag = f"DIGIT {cold_d} DUE (0%)"
        signal = (signal + " | " + tag) if signal else tag
        score += 20
    state["signal"] = signal
    state["score"]  = score

def run_market(market):
    url = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"
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

def fetch_balance():
    import websocket as ws_lib
    try:
        ws = ws_lib.create_connection(
            f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}")
        ws.send(json.dumps({"authorize": API_TOKEN}))
        resp = json.loads(ws.recv())
        if resp.get("msg_type") == "authorize":
            auth = resp["authorize"]
            with data_lock:
                account_info["balance"]  = auth.get("balance")
                account_info["currency"] = auth.get("currency", "USD")
                account_info["loginid"]  = auth.get("loginid")
        ws.close()
    except Exception as e:
        print(f"Balance fetch error: {e}")

def get_best_market():
    with data_lock:
        connected = [m for m in MARKETS if market_data[m]["connected"]]
        return sorted(connected, key=lambda m: market_data[m]["score"], reverse=True)

def start_scanner():
    threading.Thread(target=fetch_balance, daemon=True).start()
    for market in MARKETS:
        threading.Thread(target=run_market, args=(market,), daemon=True).start()
    while True:
        time.sleep(60)
        threading.Thread(target=fetch_balance, daemon=True).start()

app = Flask(__name__)
CORS(app)
threading.Thread(target=start_scanner, daemon=True).start()

@app.route("/")
def index():
    return send_file("dashboard.html")

@app.route("/api/health")
def health():
    connected = sum(1 for m in MARKETS if market_data[m]["connected"])
    return jsonify({"status": "ok", "markets_connected": connected})

@app.route("/api/account")
def api_account():
    with data_lock:
        return jsonify(account_info)

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
                "score":        s.g
