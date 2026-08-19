"""
Einmaliger Nachtrag: Der Backfill lief anfangs ohne Coinbase-/Miner-Erfassung
(die Funktion kam erst nach ~52% Fortschritt dazu). Ein volles Nach-Scannen
aller bereits gescannten Blöcke nur für die Coinbase-Adresse wäre so teuer wie
der ursprüngliche Backfill selbst — für Pool-Identifikation und eine
Difficulty-Tageshistorie reicht aber ein Sample: jeder 10. Block liefert
über den gesamten bereits abgedeckten Zeitraum verlässliche Häufigkeiten
(Pools zahlen sich alle paar Minuten aus, ein Sample alle ~10 Minuten trifft
jeden aktiven Pool zigfach) bei ~1/10 der Kosten.

Ergänzt NICHT die exakte Lückenlos-Abdeckung (die bleibt bei backfill_history.py's
eigener Coinbase-Erfassung ab dem Umstellungszeitpunkt) — dient nur der
Anreicherung von miner_rewards.jsonl für Ranking/Difficulty-Chart, nicht als
Grundlage für irgendeine Tag-für-Tag-Korrelation.
"""
import json
import time

import requests

from pep_client import RateLimiter, fetch_block, extract_coinbase_reward

STATE_FILE = "backfill_state.json"
MINER_OUTPUT_FILE = "miner_rewards.jsonl"
SAMPLE_STRIDE = 10
# Etwas konservativer als der Haupt-Backfill, da beide Prozesse parallel laufen
# und die Server-Last sich addiert (getrennte RateLimiter-Instanzen pro Prozess).
MIN_REQUEST_INTERVAL = 0.5


def contiguous_frontier_height(state):
    chunk_times = state.get("chunk_times", {})
    h = state["target_start_height"]
    chunk_size = 500
    frontier = h
    while str(h) in chunk_times:
        frontier = min(h + chunk_size - 1, state["target_end_height"])
        h += chunk_size
    return frontier


def main():
    state = json.load(open(STATE_FILE))
    start_height = state["target_start_height"]
    frontier_height = contiguous_frontier_height(state)

    heights = list(range(start_height, frontier_height + 1, SAMPLE_STRIDE))
    print(f"Sample-Nachtrag: {len(heights)} Blöcke (jeder {SAMPLE_STRIDE}.) "
          f"von {start_height} bis {frontier_height}.")

    session = requests.Session()
    limiter = RateLimiter(MIN_REQUEST_INTERVAL)
    out_f = open(MINER_OUTPUT_FILE, "a")

    start_time = time.time()
    written = 0
    for i, height in enumerate(heights, 1):
        try:
            block_meta, txs = fetch_block(session, limiter, height)
        except Exception as e:
            print(f"Block {height}: übersprungen ({e})")
            continue
        reward = extract_coinbase_reward(block_meta, txs)
        if reward:
            reward["sampled"] = True
            out_f.write(json.dumps(reward) + "\n")
            out_f.flush()
            written += 1

        if i % 200 == 0:
            elapsed = time.time() - start_time
            rate = i / elapsed
            eta_min = (len(heights) - i) / rate / 60 if rate > 0 else float("inf")
            print(f"{i}/{len(heights)} Sample-Blöcke ({elapsed/60:.1f} min, ETA ~{eta_min:.0f} min)")

    out_f.close()
    print(f"\nFertig. {written} Miner-Reward-Einträge aus Sample ergänzt.")


if __name__ == "__main__":
    main()
