"""
Wöchentliche Verteilungsanalyse: holt die komplette Rich List (alle Adressen mit
Balance > 0, aktuell ~294k Adressen über /api/v3/addresses; die Seitengröße ist
serverseitig fix auf 100/Seite gedeckelt, unabhängig vom angeforderten `limit`)
und berechnet daraus Konzentrationskennzahlen: Gini-Koeffizient, Top-1%- und
Top-10%-Anteil am Gesamtbestand.

Die ~294k einzelnen Adress-/Balance-Paare werden NICHT dauerhaft gespeichert (zu
groß für einen wöchentlichen Snapshot, und für die Kennzahlen selbst nicht nötig)
— nur die daraus abgeleiteten Werte landen in holder_distribution.json, ein
Eintrag pro Kalendertag.
"""
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

from pep_client import RateLimiter, get_with_retry, API

OUTPUT_FILE = "holder_distribution.json"
MIN_REQUEST_INTERVAL = 0.15
WORKERS = 8
SAT = 100_000_000


def fetch_page(session, limiter, page, limit=100):
    r = get_with_retry(session, f"{API}/v3/addresses", limiter, params={"page": page, "limit": limit})
    return r.json()


def fetch_all_balances():
    session = requests.Session()
    limiter = RateLimiter(MIN_REQUEST_INTERVAL)

    first = fetch_page(session, limiter, 1)
    total_pages = first["paging"]["total_pages"]
    total_count = first["paging"]["total_count"]

    balances = [int(a["balance"]) / SAT for a in first["addresses"]]
    failed_pages = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(fetch_page, session, limiter, p): p for p in range(2, total_pages + 1)}
        for future in as_completed(futures):
            page = futures[future]
            try:
                data = future.result()
            except Exception as e:
                print(f"Seite {page}: Fehler {e} — wird ausgelassen.")
                failed_pages += 1
                continue
            balances.extend(int(a["balance"]) / SAT for a in data["addresses"])

    return balances, total_count, failed_pages


def gini(balances):
    """Standard-Gini-Koeffizient über nichtnegative Werte (0 = perfekte Gleichverteilung)."""
    if not balances:
        return None
    x = sorted(balances)
    n = len(x)
    total = sum(x)
    if total == 0:
        return 0.0
    weighted_sum = sum(i * v for i, v in enumerate(x, start=1))
    return (2 * weighted_sum) / (n * total) - (n + 1) / n


def top_share_pct(balances, pct):
    x = sorted(balances, reverse=True)
    n = len(x)
    total = sum(x)
    if total == 0 or n == 0:
        return 0.0
    k = max(1, round(n * pct / 100))
    return round(100 * sum(x[:k]) / total, 4)


def main():
    print("Holder-Verteilung: hole komplette Rich List...")
    t0 = time.time()
    balances, total_count, failed_pages = fetch_all_balances()
    elapsed = time.time() - t0
    print(f"{len(balances)}/{total_count} Adressen geladen in {elapsed:.0f}s "
          f"({failed_pages} Seiten fehlgeschlagen).")

    if len(balances) < total_count * 0.9:
        print("Weniger als 90% der Rich List erreicht — Lauf wird trotzdem gespeichert, "
              "aber als unvollständig markiert.")

    g = gini(balances)
    result = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "addresses_counted": len(balances),
        "addresses_total_reported": total_count,
        "complete": len(balances) >= total_count * 0.99,
        "gini": round(g, 5) if g is not None else None,
        "top_1pct_share_pct": top_share_pct(balances, 1),
        "top_10pct_share_pct": top_share_pct(balances, 10),
    }
    print(json.dumps(result, indent=2))

    history = {}
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE) as f:
            try:
                history = json.load(f)
            except json.JSONDecodeError:
                history = {}

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    history[date_str] = result
    with open(OUTPUT_FILE, "w") as f:
        json.dump(history, f, indent=2)
    print(f"Gespeichert unter {OUTPUT_FILE} (Datum {date_str}).")


if __name__ == "__main__":
    main()
