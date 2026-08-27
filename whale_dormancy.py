"""
Dormancy-Analyse für bekannte Wal-Adressen (known_whales.json): wie lange liegt
die letzte große Auszahlung (>= 1M PEP, aus large_transfers/*.jsonl) einer
Wal-Adresse zurück? Rein lokale Auswertung bereits gesammelter Dateien, keine
zusätzlichen API-Calls.

Wichtige Einschränkung: "keine Auszahlung gefunden" bedeutet NICHT zwangsläufig
"seit Genesis inaktiv" — es bedeutet nur "keine Auszahlung >= 1M PEP innerhalb
des bislang lückenlos gescannten Zeitraums" (siehe covered_days unten). Solange
der Genesis-Backfill nicht abgeschlossen ist, kann eine Adresse fälschlich als
"dormant" erscheinen, deren letzte Bewegung in einer noch ungescannten Lücke
liegt — daher wird der abgedeckte Zeitraum explizit mitgeschrieben.
"""
import json
import os
from datetime import datetime, timezone, timedelta

from pep_client import LARGE_TRANSFERS_DIR, read_jsonl_dir

KNOWN_WHALES_FILE = "known_whales.json"
HEAD_BACKFILL_STATE_FILE = "backfill_state.json"
GENESIS_BACKFILL_STATE_FILE = "backfill_genesis_state.json"
OUTPUT_FILE = "whale_dormancy.json"
DORMANT_THRESHOLD_DAYS = 90


def _covered_day_range(state_file):
    if not os.path.exists(state_file):
        return None, None
    with open(state_file) as f:
        try:
            state = json.load(f)
        except json.JSONDecodeError:
            return None, None
    start = state.get("contiguous_covered_start_day")
    end = state.get("contiguous_covered_end_day")
    if not start or not end:
        return None, None
    return start, end


def _overall_coverage():
    """Grober Gesamt-Zeitraum (frühester Start, spätestes Ende) über beide Backfill-Läufe.
    Sagt nichts über etwaige Lücken dazwischen aus — nur eine grobe untere/obere Grenze
    für die Dormancy-Interpretation."""
    ranges = [r for r in (
        _covered_day_range(GENESIS_BACKFILL_STATE_FILE),
        _covered_day_range(HEAD_BACKFILL_STATE_FILE),
    ) if r[0] is not None]
    if not ranges:
        return None, None
    starts = [r[0] for r in ranges]
    ends = [r[1] for r in ranges]
    return min(starts), max(ends)


def main():
    if not os.path.exists(KNOWN_WHALES_FILE):
        print(f"{KNOWN_WHALES_FILE} nicht gefunden — nichts zu tun.")
        return

    with open(KNOWN_WHALES_FILE) as f:
        known_whales = json.load(f)

    last_outflow = {}
    for t in read_jsonl_dir(LARGE_TRANSFERS_DIR):
        ts = t.get("time")
        if ts is None:
            continue
        for src in t.get("from", []):
            addr = src.get("address")
            if not addr:
                continue
            if addr not in last_outflow or ts > last_outflow[addr]:
                last_outflow[addr] = ts

    now = datetime.now(timezone.utc)
    cov_start, cov_end = _overall_coverage()

    entries = {}
    dormant_count = 0
    active_count = 0
    unknown_count = 0
    for addr in known_whales:
        ts = last_outflow.get(addr)
        if ts is None:
            entries[addr] = {
                "last_large_outflow": None,
                "days_since_last_large_outflow": None,
                "status": "keine Auszahlung >= 1M PEP im gescannten Zeitraum gefunden",
            }
            unknown_count += 1
            continue
        last_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        days_since = (now - last_dt).total_seconds() / 86400
        status = "dormant" if days_since >= DORMANT_THRESHOLD_DAYS else "active"
        if status == "dormant":
            dormant_count += 1
        else:
            active_count += 1
        entries[addr] = {
            "last_large_outflow": last_dt.isoformat(),
            "days_since_last_large_outflow": round(days_since, 1),
            "status": status,
        }

    result = {
        "updated": now.isoformat(),
        "dormant_threshold_days": DORMANT_THRESHOLD_DAYS,
        "coverage_note": (
            f"Basierend auf Großtransfers >= 1M PEP, grob abgedeckter Zeitraum "
            f"{cov_start} bis {cov_end} (kann Lücken enthalten, siehe README)."
            if cov_start else "Kein Backfill-Fortschritt gefunden — Auswertung ohne Zeitbezug."
        ),
        "summary": {
            "active": active_count,
            "dormant": dormant_count,
            "no_outflow_found": unknown_count,
            "total_whales": len(known_whales),
        },
        "whales": entries,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(result, f, indent=2)
    print(f"Dormancy: {active_count} aktiv, {dormant_count} dormant, "
          f"{unknown_count} ohne gefundene Auszahlung. Gespeichert unter {OUTPUT_FILE}.")


if __name__ == "__main__":
    main()
