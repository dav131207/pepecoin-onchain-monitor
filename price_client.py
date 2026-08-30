"""
Holt historische Preis-/Volumendaten für Pepecoin (PEP).

Primärquelle: CoinEx (öffentliche API, kein Key) — PEPUSDT ist dort seit
10.12.2024 gelistet, mit ECHTEN Tages-Kerzen (OHLCV), bestätigt am 30.08.2026
via /v2/spot/kline (629 Kerzen, lückenlos bis heute). Das ist die tiefste
verlässliche historische Reichweite, die wir gefunden haben — geprüft wurden
auch MEXC (PEPUSDT ab 17.02.2025) und Kraken (PEPUSD ab 11.02.2026, viel
kürzer). Vor dem 10.12.2024 (>10 Monate nach dem Chain-Launch 30.01.2024) war
PEP auf keiner der geprüften Börsen gelistet — dafür scheint schlicht kein
verlässlicher Marktpreis zu existieren, nicht nur ein API-Limit.

Sekundärquelle: CoinGecko (Coin-ID "pepecoin-network", verifiziert: aktueller
Preis stimmt exakt mit dem Live-Preis auf avivppblocks.realmasterkush.com
überein). Liefert nur noch `market_cap` (CoinEx hat keinen Supply-Bezug) sowie
einen Fallback für price/volume, falls CoinEx mal ausfällt. Kostenloser Tarif
deckelt historische Anfragen hart bei 365 Tagen zurück (getestet 30.08.2026:
days=1000/"max" -> HTTP 401, error_code 10012) — bei >30 Tagen liefert das
OHLC-Endpoint außerdem nur 4-Tage- statt Tages-Kerzen.

Bucketing erfolgt selbst auf UTC-Kalendertage statt sich auf Provider-eigene
Intervall-Parameter zu verlassen.
"""
import json
import os
from datetime import datetime, timezone

import requests

PRICE_HISTORY_FILE = "price_history.json"
OHLC_FILE = "price_ohlc.json"

COINEX_MARKET = "PEPUSDT"
COINEX_KLINE_URL = "https://api.coinex.com/v2/spot/kline"

COINGECKO_ID = "pepecoin-network"
COINGECKO_URL = f"https://api.coingecko.com/api/v3/coins/{COINGECKO_ID}/market_chart"
COINGECKO_OHLC_URL = f"https://api.coingecko.com/api/v3/coins/{COINGECKO_ID}/ohlc"
OHLC_DAYS = 365


def utc_day_key(ts_ms):
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def fetch_coinex_daily(timeout=20):
    """
    Holt alle verfügbaren Tages-Kerzen für PEPUSDT (aktuell 629, weit unter dem
    limit=1000 der API — d.h. das ist die vollständige Historie seit Listing,
    nicht durch das Limit gekappt). Gibt {tag: {open,high,low,close,volume}} zurück.
    """
    r = requests.get(COINEX_KLINE_URL, params={"market": COINEX_MARKET, "period": "1day", "limit": 1000},
                      timeout=timeout)
    r.raise_for_status()
    data = r.json()
    if data.get("code") not in (0, None):
        raise RuntimeError(f"CoinEx API-Fehler: {data.get('message', data)}")

    by_day = {}
    for c in data.get("data", []):
        day = utc_day_key(int(c["created_at"]))
        by_day[day] = {
            "open_usd": float(c["open"]),
            "high_usd": float(c["high"]),
            "low_usd": float(c["low"]),
            "price_usd": float(c["close"]),
            "volume_usd": float(c["value"]),  # "value" = Quote-Volumen (USDT), "volume" = Basis-Volumen (PEP)
            "source": "coinex",
        }
    return by_day


def fetch_price_history(days=90, timeout=20):
    """Holt Preis-/Volumen-/Marketcap-Historie von CoinGecko, gebucketed auf UTC-Tage."""
    r = requests.get(COINGECKO_URL, params={"vs_currency": "usd", "days": days}, timeout=timeout)
    r.raise_for_status()
    data = r.json()

    by_day = {}
    for ts_ms, price in data.get("prices", []):
        by_day.setdefault(utc_day_key(ts_ms), {})["price_usd"] = price
    for ts_ms, vol in data.get("total_volumes", []):
        by_day.setdefault(utc_day_key(ts_ms), {})["volume_usd"] = vol
    for ts_ms, mcap in data.get("market_caps", []):
        by_day.setdefault(utc_day_key(ts_ms), {})["market_cap_usd"] = mcap

    return by_day


def load_price_history():
    if os.path.exists(PRICE_HISTORY_FILE):
        with open(PRICE_HISTORY_FILE) as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                pass
    return {"coingecko_id": COINGECKO_ID, "coinex_market": COINEX_MARKET, "daily": {}}


def save_price_history(history):
    with open(PRICE_HISTORY_FILE, "w") as f:
        json.dump(history, f, indent=2, sort_keys=True)


def update_price_history(days=365):
    """
    Best-effort, zwei Quellen: CoinEx zuerst (tiefere, echte Tages-Historie geht
    vor), CoinGecko ergänzt nur `market_cap_usd` (das hat CoinEx nicht) und
    dient als Fallback für price/volume an Tagen, die CoinEx nicht liefert.
    Ein Fehlschlag einer Quelle blockiert die andere nicht; bestehende Tage
    bleiben bei Komplett-Fehlschlag unangetastet. Wirft nie.
    """
    history = load_price_history()
    daily = history.setdefault("daily", {})
    any_ok = False

    try:
        coinex_days = fetch_coinex_daily()
        for day, vals in coinex_days.items():
            daily.setdefault(day, {}).update(vals)
        print(f"Preis-Historie: {len(coinex_days)} Tage von CoinEx gemerged (führende Quelle).")
        any_ok = True
    except Exception as e:
        print(f"Preis-Historie: CoinEx-Abruf fehlgeschlagen ({e}).")

    try:
        gecko_days = fetch_price_history(days=days)
        for day, vals in gecko_days.items():
            existing = daily.setdefault(day, {})
            if "market_cap_usd" in vals:
                existing["market_cap_usd"] = vals["market_cap_usd"]
            # price/volume nur auffüllen, wenn CoinEx für diesen Tag nichts hatte
            if existing.get("source") != "coinex":
                for k in ("price_usd", "volume_usd"):
                    if k in vals:
                        existing[k] = vals[k]
                existing.setdefault("source", "coingecko")
        print(f"Preis-Historie: {len(gecko_days)} Tage von CoinGecko gemerged (Marketcap + Fallback).")
        any_ok = True
    except Exception as e:
        print(f"Preis-Historie: CoinGecko-Abruf fehlgeschlagen ({e}).")

    if not any_ok:
        print("Preis-Historie: beide Quellen fehlgeschlagen, bestehende Daten bleiben unverändert.")
        return False

    history["updated"] = datetime.now(timezone.utc).isoformat()
    save_price_history(history)
    print(f"Preis-Historie: {len(daily)} Tage insgesamt gespeichert.")
    return True


def fetch_ohlc_coingecko(days=OHLC_DAYS, timeout=20):
    """
    Fallback-OHLC von CoinGecko. Granularität ist serverseitig fix an `days`
    gekoppelt: 1-2 Tage -> 30-Min-Kerzen, 3-30 Tage -> 4h-Kerzen, >30 Tage ->
    4-Tage-Kerzen (bei OHLC_DAYS=365 also 4-Tage-Kerzen, kein echtes Tages-OHLC).
    """
    r = requests.get(COINGECKO_OHLC_URL, params={"vs_currency": "usd", "days": days}, timeout=timeout)
    r.raise_for_status()
    candles = r.json()
    return [
        {"timestamp": ts_ms // 1000, "open": o, "high": h, "low": l, "close": c}
        for ts_ms, o, h, l, c in candles
    ], (4 if days > 30 else None)


def update_ohlc():
    """
    Best-effort. CoinEx zuerst (echte Tages-Kerzen, seit 10.12.2024) — nur bei
    dessen Fehlschlag Rückfall auf CoinGecko (gröbere 4-Tage-Kerzen, kürzeres
    Fenster). Schlagen beide fehl, bleibt die vorhandene Datei unangetastet.
    """
    try:
        coinex_days = fetch_coinex_daily()
        candles = [
            {
                "date": day, "open": v["open_usd"], "high": v["high_usd"],
                "low": v["low_usd"], "close": v["price_usd"],
            }
            for day, v in sorted(coinex_days.items())
        ]
        with open(OHLC_FILE, "w") as f:
            json.dump({
                "source": "coinex", "market": COINEX_MARKET,
                "candle_days": 1,
                "updated": datetime.now(timezone.utc).isoformat(),
                "candles": candles,
            }, f, indent=2)
        print(f"OHLC aktualisiert: {len(candles)} echte Tages-Kerzen von CoinEx.")
        return True
    except Exception as e:
        print(f"OHLC: CoinEx-Abruf fehlgeschlagen ({e}), versuche CoinGecko-Fallback...")

    try:
        candles, candle_days = fetch_ohlc_coingecko()
    except Exception as e:
        print(f"OHLC: CoinGecko-Fallback ebenfalls fehlgeschlagen ({e}), bestehende Datei bleibt unverändert.")
        return False

    if not candles:
        print("OHLC: leere Antwort erhalten, bestehende Datei bleibt unverändert.")
        return False

    with open(OHLC_FILE, "w") as f:
        json.dump({
            "source": "coingecko", "coingecko_id": COINGECKO_ID,
            "candle_days": candle_days,
            "updated": datetime.now(timezone.utc).isoformat(),
            "candles": candles,
        }, f, indent=2)
    print(f"OHLC aktualisiert: {len(candles)} Kerzen von CoinGecko (Fallback, {candle_days}-Tage-Kerzen).")
    return True


if __name__ == "__main__":
    update_price_history()
    update_ohlc()
