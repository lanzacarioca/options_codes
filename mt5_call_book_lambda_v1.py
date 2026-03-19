import os
import io
import re
import zipfile
import json
import urllib.request
from datetime import datetime, timedelta


UNDERLYING_MAP = {
    "PETR": "PETR4",
    "VALE": "VALE3",
    "BBDC": "BBDC4",
    "BBAS": "BBAS3",
    "ABEV": "ABEV3",
    "WEGE": "WEGE3",
    "B3SA": "B3SA3",
    "SUZB": "SUZB3",
}

UNDERLYINGS_BASE = list(UNDERLYING_MAP.keys())
OPTION_REGEX = re.compile(r"^[A-Z]{4}[A-Z][0-9]{2,3}$")

MIN_SELL_VOLUME = 5000
MIN_BUY_VOLUME = 1000
MIN_OTM_PCT = 2.0
MIN_RETURN_PCT = 30.0
MIN_CREDIT = 0.5
MIN_SPREAD_WIDTH = 1.0
MAX_SPREAD_WIDTH = 2.0

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]


# =========================================
# DOWNLOAD ROBUSTO
# =========================================
def download_latest_cotahist(max_lookback_days=30):
    base_url = "https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D{date}.ZIP"
    today = datetime.today()

    for i in range(max_lookback_days):
        ref = today - timedelta(days=i)
        date_str = ref.strftime("%d%m%Y")
        url = base_url.format(date=date_str)

        print("Tentando:", url)

        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0"
                }
            )

            with urllib.request.urlopen(req, timeout=60) as response:
                content = response.read()

            z = zipfile.ZipFile(io.BytesIO(content))
            file_name = z.namelist()[0]
            data = z.read(file_name).decode("latin1")
            lines = data.splitlines()

            print("SUCESSO:", date_str)
            return ref.date(), lines

        except Exception as e:
            print("Falhou:", date_str, e)
            continue

    return None, []


# =========================================
# PARSE
# =========================================
def parse_cotahist(lines):
    records = []

    for l in lines:
        if not l.startswith("01"):
            continue

        ticker = l[12:24].strip().upper()

        if not any(ticker.startswith(u) for u in UNDERLYINGS_BASE):
            continue

        close = int(l[108:121]) / 100
        volume = int(l[152:170])

        strike_raw = l[188:201]
        expiration_raw = l[202:210]

        strike = None
        expiration = None

        if strike_raw.strip().isdigit():
            val = int(strike_raw)
            if val != 0:
                strike = val / 100
                if strike.is_integer():
                    strike = None

        if expiration_raw.strip().isdigit():
            val = int(expiration_raw)
            if val != 0:
                expiration = datetime.strptime(expiration_raw, "%Y%m%d").date()

        if strike and not OPTION_REGEX.match(ticker):
            continue

        records.append({
            "ticker": ticker,
            "close": close,
            "volume": volume,
            "strike": strike,
            "expiration": expiration
        })

    return records


def third_friday(year, month):
    d = datetime(year, month, 1)
    days = (4 - d.weekday()) % 7
    return (d + timedelta(days=days + 14)).date()


def classify(ticker, strike):
    if strike:
        letter = ticker[4]
        if letter in "ABCDEFGHIJKL":
            return "CALL"
        if letter in "MNOPQRSTUVWX":
            return "PUT"
    return "SPOT"


# =========================================
# TELEGRAM
# =========================================
def send(text):
    print("ENVIANDO TELEGRAM")

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    data = json.dumps({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown"
    }).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"}
    )

    with urllib.request.urlopen(req, timeout=30) as response:
        resp = response.read().decode()

    print("OK TELEGRAM")


# =========================================
# CORE
# =========================================
def run():
    trade_date, lines = download_latest_cotahist()

    if not lines:
        send("⚠️ COTAHIST não encontrado")
        return

    data = parse_cotahist(lines)

    if not data:
        send("Sem dados")
        return

    now = trade_date

    current_exp = third_friday(now.year, now.month)
    next_month = 1 if now.month == 12 else now.month + 1
    next_year = now.year + 1 if now.month == 12 else now.year
    next_exp = third_friday(next_year, next_month)

    spots = {}
    calls = []

    for r in data:
        t = classify(r["ticker"], r["strike"])
        r["type"] = t

        if t == "SPOT":
            spots[r["ticker"]] = r["close"]

        if t == "CALL" and r["expiration"] in [current_exp, next_exp]:
            base = r["ticker"][:4]
            r["underlying"] = UNDERLYING_MAP.get(base)
            calls.append(r)

    calls_valid = []

    for c in calls:
        spot = spots.get(c["underlying"])
        if not spot:
            continue

        if c["strike"] and c["strike"] > spot:
            c["spot"] = spot
            calls_valid.append(c)

    spreads = []

    grouped = {}
    for c in calls_valid:
        key = (c["underlying"], c["expiration"])
        grouped.setdefault(key, []).append(c)

    for key, group in grouped.items():
        group = sorted(group, key=lambda x: x["strike"])

        for sell in group:
            for buy in group:
                if buy["strike"] <= sell["strike"]:
                    continue

                credit = sell["close"] - buy["close"]
                if credit <= 0:
                    continue

                width = buy["strike"] - sell["strike"]
                otm = (sell["strike"] - sell["spot"]) / sell["spot"] * 100
                loss = width - credit

                if loss <= 0:
                    continue

                ret = credit / loss * 100

                if not (
                    sell["volume"] >= MIN_SELL_VOLUME and
                    buy["volume"] >= MIN_BUY_VOLUME and
                    otm >= MIN_OTM_PCT and
                    ret >= MIN_RETURN_PCT and
                    credit >= MIN_CREDIT and
                    MIN_SPREAD_WIDTH <= width <= MAX_SPREAD_WIDTH
                ):
                    continue

                spreads.append({
                    "sell": sell["ticker"],
                    "buy": buy["ticker"],
                    "otm": otm,
                    "credit": credit,
                    "ret": ret,
                    "exp": sell["expiration"]
                })

    next_spreads = [s for s in spreads if s["exp"] == next_exp]
    next_spreads.sort(key=lambda x: (-x["ret"], -x["otm"]))

    if not next_spreads:
        send(f"Nenhuma trava\n{trade_date}")
        return

    msg = "📊 *TOP 10 TRAVAS*\n\n"
    msg += f"Data: `{trade_date}`\n"
    msg += f"Vencimento: `{next_exp}`\n\n"
    msg += "`SELL/BUY   OTM   CR   RET`\n"

    for s in next_spreads[:10]:
        msg += f"`{s['sell']}/{s['buy'][-3:]} {s['otm']:.1f} {s['credit']:.2f} {s['ret']:.0f}`\n"

    send(msg)


# =========================================
# LAMBDA ENTRY
# =========================================
def lambda_handler(event, context):
    return run()
