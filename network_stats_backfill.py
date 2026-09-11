"""
Rekonstruiert Hashrate/Difficulty rückwirkend bis zum Genesis-Block aus den
bereits gescannten miner_rewards/*.jsonl (jeder Eintrag trägt schon `difficulty`
+ `time` + `block`) — keine neuen API-Calls nötig, reine lokale Auswertung.

Hashrate-Schätzung folgt der Standard-Difficulty-1-Formel (gilt unabhängig vom
Hash-Algorithmus, da Difficulty relativ zum Max-Target definiert ist — scrypt-
PoW-Chains wie Pepecoin übernehmen dasselbe Schema wie Bitcoin/Litecoin):

    hashrate = avg_difficulty * 2^32 / avg_blockzeit

Pro Kalendertag wird die BEOBACHTETE durchschnittliche Blockzeit aus den
tatsächlichen Zeitstempeln der an diesem Tag gesehenen Blöcke verwendet
(derselbe Fenster-Ansatz wie `getnetworkhashps`, nur mit einem 1-Tages-Fenster
statt eines festen Blockfensters).

Validiert gegen die 4 bereits live abgerufenen Tage (`monitor.py`/getdifficulty
+getnetworkhashps, 19.08.–07.09.2026, siehe network_stats.json): Größenordnung
stimmt, Differenz bis zu ~30% an einzelnen Tagen — der Tages-Durchschnitt kann
eine oder mehrere Difficulty-Anpassungen innerhalb des Tages verwischen, während
die Live-Werte eine Momentaufnahme sind. Deshalb: NIE einen bereits vorhandenen
(echten, live abgerufenen) Tag überschreiben, und jeder rekonstruierte Tag trägt
"source": "backfilled_from_miner_rewards" zur klaren Unterscheidung von "live".
"""
import json
import os
from datetime import datetime, timezone

from pep_client import MINER_REWARDS_DIR, read_jsonl_dir

NETWORK_STATS_FILE = "network_stats.json"
DIFF1_TARGET = 2 ** 32


def day_key(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def load_history():
    if os.path.exists(NETWORK_STATS_FILE):
        with open(NETWORK_STATS_FILE) as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                pass
    return {}


def main():
    by_day = {}
    for r in read_jsonl_dir(MINER_REWARDS_DIR):
        diff = r.get("difficulty")
        if diff is None:
            continue
        by_day.setdefault(day_key(r["time"]), []).append((r["block"], r["time"], diff))

    history = load_history()
    added = 0
    skipped_existing = 0
    skipped_too_few = 0

    for day, entries in by_day.items():
        if day in history:
            skipped_existing += 1
            continue
        entries.sort()
        if len(entries) < 2:
            skipped_too_few += 1
            continue  # keine Blockzeit-Differenz berechenbar

        times = [e[1] for e in entries]
        diffs = [e[2] for e in entries]
        span = times[-1] - times[0]
        if span <= 0:
            skipped_too_few += 1
            continue
        avg_block_time = span / (len(entries) - 1)
        avg_difficulty = sum(diffs) / len(diffs)
        hashrate = avg_difficulty * DIFF1_TARGET / avg_block_time

        history[day] = {
            "hashrate": hashrate,
            "difficulty": avg_difficulty,
            "source": "backfilled_from_miner_rewards",
            "caveat": "Tages-Durchschnitt aus beobachteten Blockzeiten/Difficulty-Werten, "
                       "keine Node-Live-Abfrage — kann bei Difficulty-Anpassungen innerhalb "
                       "des Tages bis zu ~30% von einer Momentaufnahme abweichen.",
            "blocks_used": len(entries),
            "avg_block_time_sec": round(avg_block_time, 2),
        }
        added += 1

    with open(NETWORK_STATS_FILE, "w") as f:
        json.dump(history, f, indent=2, sort_keys=True)

    print(f"Netzwerk-Stats-Backfill: {added} Tage ergänzt, {skipped_existing} bereits vorhanden "
          f"(live, unangetastet), {skipped_too_few} mit zu wenig Blöcken übersprungen. "
          f"Insgesamt jetzt {len(history)} Tage.")


if __name__ == "__main__":
    main()
