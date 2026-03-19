# %%
import MetaTrader5 as mt5
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re

# inicializa conexão (MT5 já aberto manualmente)
if not mt5.initialize():
    raise RuntimeError(f"❌ Falha ao inicializar MT5: {mt5.last_error()}")

print("✅ MT5 conectado")

account = mt5.account_info()
print(account)


# %%
# ===============================
# CLASSIFICAÇÃO CANÔNICA (XP / B3)
# ===============================

def classify_type(info):

    if info is None or info.path is None:
        return "UNKNOWN"

    path = info.path.upper()

    if "OPCOES" in path:
        if info.option_right == 0:
            return "CALL"
        elif info.option_right == 1:
            return "PUT"
        else:
            return "OPTION"

    return "SPOT"


# ===============================
# FECHAMENTO E VOLUME D-1
# ===============================

def d1_close_and_volume(ticker):

    rates = mt5.copy_rates_range(
        ticker,
        mt5.TIMEFRAME_D1,
        datetime.utcnow() - timedelta(days=15),
        datetime.utcnow()
    )

    fechamento_d1 = np.nan
    volume_d1 = np.nan

    if rates is not None and len(rates) >= 2:

        candle_d1 = rates[-1]

        fechamento_d1 = float(candle_d1["close"])

        if candle_d1["real_volume"] > 0:
            volume_d1 = int(candle_d1["real_volume"])
        else:
            volume_d1 = int(candle_d1["tick_volume"])

    return fechamento_d1, volume_d1


# ===============================
# MONTA LINHA DA TABELA
# ===============================

def build_row(ticker):

    mt5.symbol_select(ticker, True)
    info = mt5.symbol_info(ticker)

    description = info.description if info else None
    option_right = info.option_right if info else None

    strike = (
        float(info.option_strike)
        if info and info.option_strike and info.option_strike > 0
        else np.nan
    )

    expiration = pd.NaT
    try:
        if info and info.expiration_time and info.expiration_time > 0:
            expiration = datetime.fromtimestamp(info.expiration_time)
    except:
        expiration = pd.NaT

    asset_type = classify_type(info)

    fechamento_d1, volume_d1 = d1_close_and_volume(ticker)

    path = info.path if info else None

    return {
        "ticker": ticker,
        "description": description,
        "type": asset_type,
        "option_right": option_right,
        "strike": strike,
        "expiration": expiration,
        "fechamento_d-1": fechamento_d1,
        "volume_d-1": volume_d1,
        "path": path
    }


# %%
# ===============================
# UNDERLYINGS LÍQUIDOS
# ===============================

UNDERLYINGS_BASE = [
    "PETR",
    "VALE",
    "ITUB",
    "BBDC",
    "BBAS",
    "ABEV",
    "WEGE",
    "B3SA",
    "SUZB",
]


# ===============================
# FUNÇÃO TERCEIRA SEXTA
# ===============================

def third_friday(year, month):

    d = datetime(year, month, 1)
    days_until_friday = (4 - d.weekday()) % 7
    first_friday = d + timedelta(days=days_until_friday)
    third = first_friday + timedelta(days=14)

    return third.date()


# ===============================
# CALCULA VENCIMENTOS
# ===============================

now = datetime.utcnow()

current_expiry_date = third_friday(now.year, now.month)

if now.month == 12:
    next_month = 1
    next_year = now.year + 1
else:
    next_month = now.month + 1
    next_year = now.year

next_expiry_date = third_friday(next_year, next_month)


# %%
# ===============================
# GERAR TICKERS
# ===============================

symbols = mt5.symbols_get() or []
tickers = []

SPOT_RE = re.compile(r"^[A-Z]{4}(3|4|11)$")

for s in symbols:

    name = s.name.upper()
    path = getattr(s, "path", "")

    if not path:
        continue

    if not any(name.startswith(u) for u in UNDERLYINGS_BASE):
        continue

    path_up = path.upper()

    # SPOT
    if "VISTA" in path_up:

        if SPOT_RE.match(name):
            tickers.append(s.name)

        continue

    # OPÇÕES
    if "OPCO" in path_up:

        info = mt5.symbol_info(s.name)

        if not info or not info.expiration_time or info.expiration_time <= 0:
            continue

        try:
            exp = datetime.fromtimestamp(info.expiration_time)
        except:
            continue

        exp_date = exp.date()

        if exp_date == current_expiry_date or exp_date == next_expiry_date:
            tickers.append(s.name)


print("Qtd de tickers selecionados:", len(tickers))
print("Primeiros 10:", tickers[:10])


# %%
# ===============================
# CONSTRUIR TABELA
# ===============================

rows = [build_row(t) for t in tickers]

df_ativos = pd.DataFrame(rows)

df_ativos.head(10)


# %%
# =========================================
# ENRIQUECIMENTO UNDERLYING
# =========================================

def infer_underlying(row):

    ticker = row["ticker"]
    asset_type = row["type"]
    desc = row["description"]

    base = ticker[:4]

    if asset_type == "SPOT":
        return ticker

    if asset_type in ("CALL", "PUT") and isinstance(desc, str):

        d = desc.upper()

        if re.search(r"\bPN\b", d):
            return f"{base}4"

        if re.search(r"\bON\b", d):
            return f"{base}3"

    return None


df_ativos["underlying"] = df_ativos.apply(infer_underlying, axis=1)


# %%
# =========================================
# UNIVERSO SPOT + CALL
# =========================================

df_spot = df_ativos[df_ativos["type"] == "SPOT"].copy()
df_calls = df_ativos[df_ativos["type"] == "CALL"].copy()


# %%
# =========================================
# PREÇO SPOT NAS CALLS
# =========================================

spot_price_map = dict(
    zip(df_spot["ticker"], df_spot["fechamento_d-1"])
)

df_calls["spot_price"] = df_calls["underlying"].map(spot_price_map)


# %%
# =========================================
# CALLs OTM
# =========================================

df_calls_valid = df_calls[
    (df_calls["strike"].notna()) &
    (df_calls["spot_price"].notna()) &
    (df_calls["strike"] > df_calls["spot_price"])
].copy()


# %%
# =========================================
# GERAR CALL SPREADS
# =========================================

spreads = []

group_cols = ["underlying", "expiration"]

for (underlying, expiration), g in df_calls_valid.groupby(group_cols):

    g = g.sort_values("strike")

    for _, sell in g.iterrows():
        for _, buy in g.iterrows():

            if buy["strike"] <= sell["strike"]:
                continue

            credit = sell["fechamento_d-1"] - buy["fechamento_d-1"]

            if credit <= 0:
                continue

            spreads.append({

                "underlying": underlying,
                "expiration": expiration,

                "sell_ticker": sell["ticker"],
                "buy_ticker": buy["ticker"],

                "sell_strike": sell["strike"],
                "buy_strike": buy["strike"],

                "spot_price": sell["spot_price"],

                "sell_premium": sell["fechamento_d-1"],
                "buy_premium": buy["fechamento_d-1"],
                "credit": credit,

                "sell_volume": sell["volume_d-1"],
                "buy_volume": buy["volume_d-1"],
            })

df_call_spreads = pd.DataFrame(spreads)


# %%
# =========================================
# FILTROS FINAIS
# =========================================

MIN_SELL_VOLUME = 5000
MIN_BUY_VOLUME = 1000
MIN_OTM_PCT = 2.0
MIN_RETURN_PCT = 30.0
MIN_CREDIT = 0.5

MIN_SPREAD_WIDTH = 1.0
MAX_SPREAD_WIDTH = 2.0


df = df_call_spreads.copy()

df["spread_width"] = df["buy_strike"] - df["sell_strike"]

df["otm_pct"] = (
    (df["sell_strike"] - df["spot_price"]) / df["spot_price"]
) * 100

df["max_loss"] = df["spread_width"] - df["credit"]

df["return_pct"] = (df["credit"] / df["max_loss"]) * 100


df_filt = df[
    (df["sell_volume"] >= MIN_SELL_VOLUME) &
    (df["buy_volume"] >= MIN_BUY_VOLUME) &
    (df["otm_pct"] >= MIN_OTM_PCT) &
    (df["return_pct"] >= MIN_RETURN_PCT) &
    (df["credit"] >= MIN_CREDIT) &
    (df["spread_width"] >= MIN_SPREAD_WIDTH) &
    (df["spread_width"] <= MAX_SPREAD_WIDTH) &
    (df["max_loss"] > 0)
].copy()


# %%
# CONTAGEM

df_count = (
    df_filt
    .groupby("underlying")
    .size()
    .reset_index(name="qtd_travas")
    .sort_values("qtd_travas", ascending=False)
)

print("Qtd de travas viáveis por ativo:")
display(df_count)


# %%
# TOP TRAVAS

df_top = df_filt.sort_values(
    ["return_pct", "otm_pct"],
    ascending=[False, False]
)

display(df_top.head(20))