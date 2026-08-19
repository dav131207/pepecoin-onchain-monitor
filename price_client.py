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
COINGECKO_ID = "pepecoin-network"
COINGECKO_URL = f"https://api.coingecko.com/api/v3/coins/{COINGECKO_ID}/market_chart"


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


def update_price_history(days=90):
    """
    Best-effort: holt frische Daten und merged sie in die lokale Historie
    (bestehende Tage bleiben bei Fehlschlag unangetastet). Gibt True/False zurück,
    wirft nie — der Aufrufer (monitor.py) soll dadurch nicht blockiert werden.
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


if __name__ == "__main__":
    update_price_history()
