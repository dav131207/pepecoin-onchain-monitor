"""
Holt historische Preis-/Volumendaten für Pepecoin (PEP) von der öffentlichen
CoinGecko-API (kein API-Key nötig). Coin-ID "pepecoin-network" wurde verifiziert:
der aktuelle Preis stimmt exakt mit dem Live-Preis auf avivppblocks.realmasterkush.com
überein, Beschreibung (Scrypt, Litecoin-mergemined) passt zur selben Chain.

Bucketing erfolgt selbst auf UTC-Kalendertage (letzter Datenpunkt je Tag) statt
sich auf den `interval`-Parameter zu verlassen, der je nach CoinGecko-Tarif
ignoriert werden kann.
"""
import json
import os
from datetime import datetime, timezone

import requests

PRICE_HISTORY_FILE = "price_history.json"
OHLC_FILE = "price_ohlc.json"
COINGECKO_ID = "pepecoin-network"
COINGECKO_URL = f"https://api.coingecko.com/api/v3/coins/{COINGECKO_ID}/market_chart"
COINGECKO_OHLC_URL = f"https://api.coingecko.com/api/v3/coins/{COINGECKO_ID}/ohlc"
# CoinGecko liefert bei days>30 automatisch 4-Tage-Kerzen (kein echtes Tages-OHLC
# auf dem kostenlosen Tarif) — wird unverändert so gespeichert und als
# candle_days=4 gekennzeichnet, damit niemand es fälschlich als Tagesdaten liest.
OHLC_DAYS = 365


def utc_day_key(ts_ms):
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def fetch_price_history(days=90, timeout=20):
    """Holt Preis-/Volumen-/Marketcap-Historie, gebucketed auf UTC-Tage."""
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
    return {"coingecko_id": COINGECKO_ID, "daily": {}}


def save_price_history(history):
    with open(PRICE_HISTORY_FILE, "w") as f:
        json.dump(history, f, indent=2, sort_keys=True)


def update_price_history(days=365):
    """
    Best-effort: holt frische Daten und merged sie in die lokale Historie
    (bestehende Tage bleiben bei Fehlschlag unangetastet). Gibt True/False zurück,
    wirft nie — der Aufrufer (monitor.py) soll dadurch nicht blockiert werden.

    days=365 ist das Maximum, das CoinGeckos kostenloser Tarif überhaupt noch
    zulässt (getestet 30.08.2026: days=1000/"max" -> HTTP 401, error_code 10012
    "Public API users are limited to a 365 day historical data view"). Echte
    Preis-Historie bis zum Launch (30.01.2024, >365 Tage zurück) ist über diese
    Quelle grundsätzlich NICHT erreichbar, egal welcher Wert hier steht — dafür
    bräuchte es einen bezahlten CoinGecko-Plan oder eine andere Quelle. Der bereits
    gesammelte lokale Verlauf (`history["daily"]`) bleibt aber über die Zeit
    kumulativ vollständig, solange der Job regelmäßig läuft und nie länger als
    365 Tage aussetzt.
    """
    history = load_price_history()
    try:
        by_day = fetch_price_history(days=days)
    except Exception as e:
        print(f"Preis-Historie: Abruf fehlgeschlagen ({e}), bestehende Daten bleiben unverändert.")
        return False

    if len(by_day) < days * 0.5:
        print(f"Preis-Historie: Nur {len(by_day)} Tage erhalten (erwartet ~{days}), wird trotzdem gemerged.")

    history.setdefault("daily", {}).update(by_day)
    history["updated"] = datetime.now(timezone.utc).isoformat()
    save_price_history(history)
    print(f"Preis-Historie aktualisiert: {len(by_day)} Tage von CoinGecko gemerged, "
          f"{len(history['daily'])} Tage insgesamt gespeichert.")
    return True


def fetch_ohlc(days=OHLC_DAYS, timeout=20):
    """
    Holt OHLC-Kerzen von CoinGecko. Granularität ist serverseitig fix an `days`
    gekoppelt (nicht wählbar): 1-2 Tage -> 30-Min-Kerzen, 3-30 Tage -> 4h-Kerzen,
    >30 Tage -> 4-Tage-Kerzen. Bei OHLC_DAYS=365 sind das also 4-Tage-Kerzen.
    """
    r = requests.get(COINGECKO_OHLC_URL, params={"vs_currency": "usd", "days": days}, timeout=timeout)
    r.raise_for_status()
    candles = r.json()
    return [
        {"timestamp": ts_ms // 1000, "open": o, "high": h, "low": l, "close": c}
        for ts_ms, o, h, l, c in candles
    ]


def update_ohlc(days=OHLC_DAYS):
    """
    Best-effort, ersetzt price_ohlc.json komplett bei jedem erfolgreichen Abruf
    (CoinGecko liefert immer das volle angeforderte Fenster neu, kein Merge nötig).
    Schlägt der Abruf fehl, bleibt die vorhandene Datei unangetastet. Wirft nie.
    """
    try:
        candles = fetch_ohlc(days=days)
    except Exception as e:
        print(f"OHLC: Abruf fehlgeschlagen ({e}), bestehende Datei bleibt unverändert.")
        return False

    if not candles:
        print("OHLC: leere Antwort erhalten, bestehende Datei bleibt unverändert.")
        return False

    with open(OHLC_FILE, "w") as f:
        json.dump({
            "coingecko_id": COINGECKO_ID,
            "requested_days": days,
            "candle_days": 4 if days > 30 else None,
            "updated": datetime.now(timezone.utc).isoformat(),
            "candles": candles,
        }, f, indent=2)
    print(f"OHLC aktualisiert: {len(candles)} Kerzen ({days} Tage Fenster).")
    return True


if __name__ == "__main__":
    update_price_history()
    update_ohlc()
