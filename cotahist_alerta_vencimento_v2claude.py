import os
import io
import re
import zipfile
import json
import urllib.request
from datetime import datetime, timedelta


# =========================================
# TRAVAS VIGENTES
# Atualize sempre que montar/desmontar
# (use o export do mt5_monitor_travas_v2)
# =========================================

MINHAS_TRAVAS = [
    {"sell": "BBASD241", "buy": "BBASD251", "underlying": "BBAS3", "sell_strike": 23.77, "buy_strike": 24.77},
    {"sell": "VALED779", "buy": "VALED789", "underlying": "VALE3", "sell_strike": 77.9,  "buy_strike": 78.9},
    {"sell": "WEGED483", "buy": "WEGED493", "underlying": "WEGE3", "sell_strike": 47.03, "buy_strike": 48.03},
]


# =========================================
# CONFIG
# =========================================

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID   = os.environ["TELEGRAM_CHAT_ID"]


# =========================================
# HELPERS — DATAS
# =========================================

def third_friday(year, month):
    d = datetime(year, month, 1)
    days_until_friday = (4 - d.weekday()) % 7
    first_friday = d + timedelta(days=days_until_friday)
    return (first_friday + timedelta(days=14)).date()


def business_days_until(target_date):
    today = datetime.today().date()
    if target_date <= today:
        return 0
    count = 0
    current = today
    while current < target_date:
        current += timedelta(days=1)
        if current.weekday() < 5:
            count += 1
    return count


def next_expiry():
    today = datetime.today().date()
    candidate = third_friday(today.year, today.month)
    if candidate <= today:
        if today.month == 12:
            candidate = third_friday(today.year + 1, 1)
        else:
            candidate = third_friday(today.year, today.month + 1)
    return candidate


# =========================================
# HELPERS — COTAHIST (só spots)
# =========================================

def download_latest_cotahist(max_lookback_days=10):
    base_url = "https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D{date}.ZIP"
    today = datetime.today()

    for i in range(max_lookback_days):
        ref      = today - timedelta(days=i)
        date_str = ref.strftime("%d%m%Y")
        url      = base_url.format(date=date_str)

        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=60) as response:
                content = response.read()

            z         = zipfile.ZipFile(io.BytesIO(content))
            file_name = z.namelist()[0]
            data      = z.read(file_name).decode("latin1")
            lines     = data.splitlines()

            print(f"COTAHIST OK: {date_str} ({len(lines)} linhas)")
            return ref.date(), lines

        except Exception as e:
            print(f"Falhou {date_str}: {e}")
            continue

    return None, []


def parse_spots(lines, underlyings):
    """
    Extrai só os preços de fechamento dos ativos spot que interessam.
    Retorna dict {ticker: close}, ex: {"BBAS3": 23.43, "VALE3": 83.69}
    """
    # tickers spot que precisamos: ex. {"BBAS3", "VALE3", "WEGE3"}
    targets = set(underlyings)
    spots   = {}

    for line in lines:
        if not line.startswith("01"):
            continue

        ticker = line[12:24].strip().upper()

        if ticker not in targets:
            continue

        # strike_raw — se preenchido, é opção, não spot
        strike_raw = line[188:201].strip()
        if strike_raw.isdigit() and int(strike_raw) != 0:
            continue

        close = int(line[108:121]) / 100

        if close > 0:
            spots[ticker] = close

    return spots


# =========================================
# HELPERS — TELEGRAM
# =========================================

def send(text):
    url  = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = json.dumps({
        "chat_id"   : TELEGRAM_CHAT_ID,
        "text"      : text,
        "parse_mode": "Markdown",
    }).encode("utf-8")
    req = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        resp.read()


# =========================================
# CORE
# =========================================

def run():

    if not MINHAS_TRAVAS:
        print("Nenhuma trava configurada.")
        return

    expiry     = next_expiry()
    dias_uteis = business_days_until(expiry)

    # --- baixa Cotahist só para pegar spots ---
    underlyings = list({t["underlying"] for t in MINHAS_TRAVAS})
    trade_date, lines = download_latest_cotahist()

    spots = {}
    if lines:
        spots = parse_spots(lines, underlyings)
        print("Spots encontrados:", spots)
    else:
        print("AVISO: Cotahist indisponível — sem preços de spot.")

    # --- monta linhas de cada trava ---
    linhas = ""
    for t in MINHAS_TRAVAS:
        spot = spots.get(t["underlying"])

        if spot:
            otm_pct = (t["sell_strike"] - spot) / spot * 100

            if otm_pct > 2.0:
                status_emoji = "🟢"
                status_txt   = f"OTM {otm_pct:+.1f}%"
            elif otm_pct >= 0:
                status_emoji = "🟡"
                status_txt   = f"ATM {otm_pct:+.1f}%"
            else:
                status_emoji = "🔴"
                status_txt   = f"ITM {otm_pct:+.1f}%"

            spot_txt = f"spot {spot:.2f}  {status_emoji} {status_txt}"
        else:
            spot_txt = "spot n/d"

        linhas += (
            f"`{t['sell']}/{t['buy'][-3:]}`  "
            f"{t['underlying']}  "
            f"{t['sell_strike']:.2f}/{t['buy_strike']:.2f}  "
            f"{spot_txt}\n"
        )

    # --- urgência geral ---
    if dias_uteis <= 3:
        emoji    = "🚨"
        urgencia = f"*URGENTE — vence em {dias_uteis} dia(s) útil(is)!*\nRode o monitor local agora."
    elif dias_uteis <= 7:
        emoji    = "🔴"
        urgencia = f"*Vencimento em {dias_uteis} dias úteis* — rode o monitor local."
    elif dias_uteis <= 15:
        emoji    = "⚠️"
        urgencia = f"Faltam {dias_uteis} dias úteis — acompanhe diariamente."
    else:
        emoji    = "📅"
        urgencia = f"Faltam {dias_uteis} dias úteis."

    ref_txt = f"_Ref: {trade_date}_\n\n" if trade_date else ""

    msg = (
        f"{emoji} *TRAVAS VIGENTES — vencimento {expiry.strftime('%d/%m/%Y')}*\n\n"
        f"{linhas}\n"
        f"{ref_txt}"
        f"{urgencia}"
    )

    send(msg)
    print("Telegram enviado.")
    print(msg)


# =========================================
# LAMBDA ENTRY
# =========================================

def lambda_handler(event, context):
    run()
