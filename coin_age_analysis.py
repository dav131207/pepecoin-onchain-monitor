"""
Coin-Age-Analyse aus all_transactions/*.jsonl (backfill_alltx.py), rein lokal,
keine API-Calls. Zwei Kennzahlen:

1. Coin Days Destroyed (CDD), pro Kalendertag: für jede ausgegebene Eingabe wird
   ihr Alter (Zeit zwischen Erzeugung der Ausgabe und diesem Spend) mit dem Betrag
   gewichtet. Hoher CDD-Ausschlag = viele alte Coins wurden bewegt (klassisches
   Signal für "smart money"/langfristige Halter, die verkaufen).
   Join: input["prev_txid"]/["prev_vout"] gegen den (txid, vout)-Schlüssel einer
   FRÜHER gesehenen Ausgabe (siehe pep_client.extract_full_transactions).

2. HODL-Waves (Supply nach Haltedauer, Momentaufnahme JETZT): Ausgaben, die nie
   als Eingabe einer anderen Tx auftauchen, gelten als (aus Sicht der bisher
   gescannten Daten) unverbraucht. Bucket nach Alter seit Erzeugung.

WICHTIGE EINSCHRÄNKUNG: Beide Kennzahlen sind nur so vollständig wie
all_transactions/*.jsonl selbst. Solange backfill_alltx.py nicht die gesamte
Chain abgedeckt hat, fehlen zwangsläufig Ursprungs- oder Spend-Einträge —
das macht CDD für frühe/noch ungescannte Zeiträume unterschätzt UND lässt
tatsächlich längst ausgegebene Coins fälschlich als "unspent" (HODL-Wave)
erscheinen, wenn ihr Spend in einem noch ungescannten Block liegt. Deshalb wird
covered_days explizit mitgeschrieben statt die Zahlen als vollständig auszugeben.

Speicherbedarf wächst mit der Zahl aller je erzeugten Ausgaben (nicht nur
unverbrauchte) — bei sehr großem Datensatz ggf. Umstieg auf eine echte
Datenbank/Streaming-Join nötig; für den aktuellen Umfang reicht ein In-Memory-Dict.
"""
import json
import os
from datetime import datetime, timezone

from pep_client import ALL_TRANSACTIONS_DIR, read_jsonl_dir

CDD_OUTPUT_FILE = "coin_days_destroyed.json"
HODL_OUTPUT_FILE = "hodl_waves.json"

HODL_BUCKETS_DAYS = [
    ("< 1 Tag", 0, 1),
    ("1-7 Tage", 1, 7),
    ("1-4 Wochen", 7, 30),
    ("1-3 Monate", 30, 90),
    ("3-6 Monate", 90, 180),
    ("6-12 Monate", 180, 365),
    ("1-2 Jahre", 365, 730),
    ("> 2 Jahre", 730, float("inf")),
]


def bucket_for_age(age_days):
    for label, lo, hi in HODL_BUCKETS_DAYS:
        if lo <= age_days < hi:
            return label
    return HODL_BUCKETS_DAYS[-1][0]


def main():
    if not os.path.exists(ALL_TRANSACTIONS_DIR):
        print(f"{ALL_TRANSACTIONS_DIR}/ existiert noch nicht (backfill_alltx.py noch nicht gelaufen) — nichts zu tun.")
        return

    outputs = {}   # (txid, vout) -> (address, value, time)
    spent = set()  # (prev_txid, prev_vout)
    cdd_by_day = {}
    tx_count = 0
    now = datetime.now(timezone.utc).timestamp()

    for tx in read_jsonl_dir(ALL_TRANSACTIONS_DIR):
        tx_count += 1
        txid = tx["txid"]
        ts = tx["time"]

        for o in tx.get("outputs", []):
            outputs[(txid, o["vout"])] = (o.get("address"), o.get("amount", 0.0), ts)

        for i in tx.get("inputs", []):
            key = (i.get("prev_txid"), i.get("prev_vout"))
            if key[0] is None:
                continue
            spent.add(key)
            origin = outputs.get(key)
            if origin is None:
                continue  # Ursprungs-Ausgabe (noch) nicht im gescannten Datensatz
            _, value, origin_ts = origin
            age_days = max(0.0, (ts - origin_ts) / 86400)
            day_key = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
            cdd_by_day[day_key] = cdd_by_day.get(day_key, 0.0) + value * age_days

    index_path = os.path.join(ALL_TRANSACTIONS_DIR, "index.json")
    covered_days = []
    if os.path.exists(index_path):
        with open(index_path) as f:
            covered_days = json.load(f)

    cdd_result = {
        "updated": datetime.now(timezone.utc).isoformat(),
        "unit": "PEP * Tage (Betrag mal Alter der ausgegebenen Coins bei diesem Spend)",
        "caveat": "Nur Spends, deren Ursprungs-Ausgabe ebenfalls im bislang gescannten Zeitraum "
                   "liegt, gehen ein — CDD für Tage außerhalb von covered_days ist unvollständig.",
        "transactions_scanned": tx_count,
        "covered_days": covered_days,
        "daily": cdd_by_day,
    }
    with open(CDD_OUTPUT_FILE, "w") as f:
        json.dump(cdd_result, f, indent=2, sort_keys=True)

    unspent = {k: v for k, v in outputs.items() if k not in spent}
    bucket_totals = {label: 0.0 for label, _, _ in HODL_BUCKETS_DAYS}
    bucket_counts = {label: 0 for label, _, _ in HODL_BUCKETS_DAYS}
    total_value = 0.0
    for (address, value, ts) in unspent.values():
        age_days = max(0.0, (now - ts) / 86400)
        label = bucket_for_age(age_days)
        bucket_totals[label] += value
        bucket_counts[label] += 1
        total_value += value

    hodl_result = {
        "updated": datetime.now(timezone.utc).isoformat(),
        "caveat": "Momentaufnahme NUR über die bislang von backfill_alltx.py gescannten Tage "
                   "(covered_days) — Ausgaben aus noch ungescannten Blöcken fehlen komplett, "
                   "und tatsächlich schon ausgegebene Coins können fälschlich als unverbraucht "
                   "erscheinen, wenn ihr Spend in einem noch ungescannten Block liegt. Erst mit "
                   "vollständigem all_transactions/-Backfill (Block 1 bis aktueller Stand) belastbar.",
        "transactions_scanned": tx_count,
        "covered_days": covered_days,
        "unspent_outputs_tracked": len(unspent),
        "total_value_pep": round(total_value, 2),
        "buckets": [
            {
                "label": label,
                "value_pep": round(bucket_totals[label], 2),
                "share_pct": round(100 * bucket_totals[label] / total_value, 2) if total_value else 0.0,
                "output_count": bucket_counts[label],
            }
            for label, _, _ in HODL_BUCKETS_DAYS
        ],
    }
    with open(HODL_OUTPUT_FILE, "w") as f:
        json.dump(hodl_result, f, indent=2)

    print(f"Coin-Age-Analyse: {tx_count} Tx, {len(unspent)}/{len(outputs)} Ausgaben (im gescannten "
          f"Datensatz) unverbraucht. CDD -> {CDD_OUTPUT_FILE}, HODL-Waves -> {HODL_OUTPUT_FILE}.")


if __name__ == "__main__":
    main()
