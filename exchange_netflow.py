"""
Aggregiert Ein-/Ausgänge zu vermuteten Exchange-Adressen (aus exchange_candidates.json)
zu einer täglichen Netflow-Zeitreihe — bisher gab es nur einzelne markierte
Großtransfers im Snapshot, keine aufsummierte Reihe. Rein lokal, keine API-Calls:
liest large_transfers/*.jsonl (bereits vorhanden) und exchange_candidates.json.

Netflow = Summe(Zuflüsse zu Exchange-Kandidaten) - Summe(Abflüsse von Exchange-
Kandidaten), pro Kalendertag. Positiv = mehr PEP floss auf vermutete Exchanges als
weg (potenzieller Verkaufsdruck), negativ = Nettoabfluss (eher Akkumulation/Self-
Custody).

ZWEI EINSCHRÄNKUNGEN, beide explizit in der Ausgabedatei vermerkt:
1. Nur Großtransfers >= 1M PEP fließen ein (large_transfers/ erfasst nur diese) —
   echte Netflows durch viele kleine Ein-/Auszahlungen fehlen. Sobald der laufende
   Vollscan (backfill_alltx.py) verfügbar ist, könnte dies auf ALLE Transaktionen
   erweitert werden.
2. exchange_candidates.json ist eine AKTUELLE Momentaufnahme (heutige Tx-Aktivität-
   Rangliste) — wird aber auf die GESAMTE Historie angewendet. Eine Adresse, die
   erst kürzlich exchange-artige Aktivität zeigt, wird auch für frühere Transfers
   als "Exchange" gewertet, obwohl sie es damals vielleicht noch nicht war (und
   umgekehrt: eine früher genutzte, mittlerweile inaktive Exchange-Adresse fehlt,
   wenn sie aus dem aktuellen Ranking gefallen ist).
"""
import json
import os
from datetime import datetime, timezone

from pep_client import LARGE_TRANSFERS_DIR, read_jsonl_dir

EXCHANGE_CANDIDATES_FILE = "exchange_candidates.json"
OUTPUT_FILE = "exchange_netflow.json"


def day_key(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def load_exchange_addresses(min_tx_count=200):
    """Dieselbe Auswahl wie monitor.py: auffällig aktive Adressen, die NICHT als
    Miner-Wallet erkannt sind (wiederholte Coinbase-Auszahlungen sähen sonst wie
    Exchange-Aktivität aus)."""
    if not os.path.exists(EXCHANGE_CANDIDATES_FILE):
        return set()
    with open(EXCHANGE_CANDIDATES_FILE) as f:
        data = json.load(f)
    return {
        c["address"] for c in data.get("candidates", [])
        if c.get("tx_count", 0) >= min_tx_count and not c.get("is_miner")
    }


def main():
    exchange_addrs = load_exchange_addresses()
    if not exchange_addrs:
        print("Exchange-Netflow: exchange_candidates.json fehlt oder leer — nichts zu tun.")
        return

    daily = {}  # day -> {"inflow": x, "outflow": y, "transfer_count": n}
    transfers_seen = 0
    matched = 0

    for t in read_jsonl_dir(LARGE_TRANSFERS_DIR):
        transfers_seen += 1
        day = day_key(t["time"])
        entry = daily.setdefault(day, {"inflow_pep": 0.0, "outflow_pep": 0.0, "transfer_count": 0})

        from_exchange = any(f["address"] in exchange_addrs for f in t.get("from", []))
        to_exchange_amount = sum(
            o["amount"] for o in t.get("to", []) if o.get("address") in exchange_addrs
        )
        # "from" trägt bereits den vollen Netto-Betrag der Absenderseite (siehe
        # extract_net_transfers) — für den Abfluss zählt der volle net_amount, wenn
        # irgendeine Absenderadresse ein Exchange-Kandidat ist.
        if from_exchange:
            daily[day]["outflow_pep"] += t["net_amount"]
            matched += 1
        if to_exchange_amount > 0:
            daily[day]["inflow_pep"] += to_exchange_amount
            matched += 1
        if from_exchange or to_exchange_amount > 0:
            daily[day]["transfer_count"] += 1

    result = {}
    for day, v in daily.items():
        if v["transfer_count"] == 0:
            continue
        result[day] = {
            "inflow_pep": round(v["inflow_pep"], 2),
            "outflow_pep": round(v["outflow_pep"], 2),
            "net_flow_pep": round(v["inflow_pep"] - v["outflow_pep"], 2),
            "transfer_count": v["transfer_count"],
        }

    output = {
        "updated": datetime.now(timezone.utc).isoformat(),
        "exchange_candidate_count": len(exchange_addrs),
        "caveat": "Nur Großtransfers >= 1M PEP (large_transfers/) gehen ein, kleinere "
                   "Ein-/Auszahlungen fehlen. exchange_candidates.json ist eine aktuelle "
                   "Momentaufnahme der Tx-Aktivität, wird aber rückwirkend auf die gesamte "
                   "Historie angewendet — Adressen können früher/später anders eingeordnet "
                   "gehört haben, als es diese Liste heute zeigt.",
        "unit": "PEP, net_flow_pep > 0 = Netto-Zufluss zu Exchange-Kandidaten "
                 "(potenzieller Verkaufsdruck), < 0 = Netto-Abfluss",
        "daily": result,
    }
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2, sort_keys=True)

    print(f"Exchange-Netflow: {transfers_seen} Großtransfers geprüft, {matched} mit Exchange-"
          f"Kandidaten-Beteiligung, {len(result)} Tage mit Aktivität. Gespeichert unter {OUTPUT_FILE}.")


if __name__ == "__main__":
    main()
