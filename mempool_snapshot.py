"""
Stündlicher Mempool-Schnappschuss: Mempool ist naturgemäß nicht rückwirkend
erfassbar (nur der aktuelle Zustand existiert), daher eigener, häufiger Cron
statt Teil eines Backfills. Läuft in einer eigenen Concurrency-Gruppe (siehe
mempool-snapshot.yml), damit stündliche Läufe nicht hinter den 5h-Backfills
verhungern — schreibt außerdem in eigene Dateien, kein Konfliktrisiko mit den
Backfill-Läufen.

Speichert Zusammenfassung (Anzahl, Gesamtgebühr, Gebühr/Byte-Verteilung) IMMER,
die rohen Einzeleinträge nur bis MAX_RAW_ENTRIES (Schutz vor einem pathologischen
Congestion-Event, das eine einzelne Tagesdatei aufblähen würde).
"""
import json
import os
from datetime import datetime, timezone

import requests

from pep_client import API

OUTPUT_DIR = "mempool_history"
MAX_RAW_ENTRIES = 2000


def percentile(sorted_values, pct):
    if not sorted_values:
        return None
    idx = min(len(sorted_values) - 1, max(0, round(pct / 100 * (len(sorted_values) - 1))))
    return sorted_values[idx]


def main():
    now = datetime.now(timezone.utc)
    try:
        r = requests.get(f"{API}/getrawmempool", params={"verbose": 1}, timeout=20)
        r.raise_for_status()
        mempool = r.json()
    except Exception as e:
        print(f"Mempool-Snapshot: Abruf fehlgeschlagen ({e}), kein Eintrag geschrieben.")
        return

    entries = []
    fee_rates = []
    total_fee = 0.0
    total_size = 0
    for txid, info in mempool.items():
        size = info.get("size", 0)
        fee = info.get("fee", 0.0)
        fee_rate = (fee / size) if size else None
        if fee_rate is not None:
            fee_rates.append(fee_rate)
        total_fee += fee
        total_size += size
        entries.append({
            "txid": txid, "size": size, "fee": fee, "fee_rate_pep_per_byte": fee_rate,
            "time": info.get("time"), "height": info.get("height"),
        })

    fee_rates.sort()
    summary = {
        "timestamp": now.isoformat(),
        "pending_count": len(entries),
        "total_fee_pep": round(total_fee, 8),
        "total_size_bytes": total_size,
        "fee_rate_pep_per_byte": {
            "p10": percentile(fee_rates, 10),
            "p50": percentile(fee_rates, 50),
            "p90": percentile(fee_rates, 90),
            "max": fee_rates[-1] if fee_rates else None,
        },
        "raw_truncated": len(entries) > MAX_RAW_ENTRIES,
        "entries": entries[:MAX_RAW_ENTRIES],
    }

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    day_key = now.strftime("%Y-%m-%d")
    with open(os.path.join(OUTPUT_DIR, f"{day_key}.jsonl"), "a") as f:
        f.write(json.dumps(summary) + "\n")

    print(f"Mempool-Snapshot: {summary['pending_count']} ausstehende Tx, "
          f"{summary['total_fee_pep']} PEP Gesamtgebühr, gespeichert unter {OUTPUT_DIR}/{day_key}.jsonl.")


if __name__ == "__main__":
    main()
