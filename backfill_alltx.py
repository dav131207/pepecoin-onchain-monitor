"""
Dritter, unabhängiger Backfill-Pass: erfasst JEDE Transaktion (nicht nur Netto-
Transfers >= 1M PEP wie backfill_history.py/backfill_genesis.py) von Block 1 bis
zum aktuellen Blockstand, in all_transactions/YYYY-MM-DD.jsonl (siehe
pep_client.extract_full_transactions für das Schema und den Coin-Age-Join-Ansatz).

Eigene Zustandsdatei (STATE_FILE), eigener, EIN durchgehender Bereich (keine
Aufteilung in Genesis-/Kopf-Fenster wie bei den anderen beiden Läufen — hier
gibt es keinen Grund für "aktuelle Daten zuerst", da bereits die anderen beiden
Läufe genau das liefern). auto_extend_end=True: folgt bei jedem Resume dem
aktuellen Blockstand.

Rührt NICHT an backfill_state.json oder backfill_genesis_state.json — läuft
komplett unabhängig neben den beiden bestehenden Läufen (gleiche Blöcke werden
also dreifach von der API geholt, das ist bewusst in Kauf genommen statt die
bereits laufenden ~51% Fortschritt zu riskieren).

Fortsetzbar: Fortschritt in 500-Block-Chunks (STATE_FILE). Abbruch verliert
höchstens den aktuell offenen Chunk.
"""
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

from pep_client import RateLimiter, fetch_block, extract_full_transactions, API, ALL_TRANSACTIONS_DIR, append_jsonl_by_day

STATE_FILE = "backfill_alltx_state.json"
OUTPUT_DIR = ALL_TRANSACTIONS_DIR
INCOMPLETE_LOG = "blocks_incomplete_alltx.txt"

CHUNK_SIZE = 500
WORKERS = 16
# Wie backfill_genesis.py: dedizierter, für höheres Tempo freigegebener Endpoint
# (Burst-getestet 19.8.: 20 gleichzeitige Requests ~28 Blöcke/s, 0 Fehler). Läuft
# im selben "pep-data-pipeline"-Concurrency-Slot wie die anderen beiden schweren
# Backfills (siehe .github/workflows/), nie gleichzeitig mit ihnen — sonst würden
# sich drei Prozesse dasselbe getestete Rate-Budget streitig machen.
MIN_REQUEST_INTERVAL = 0.02


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return None


def save_state(state):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, STATE_FILE)


def chunk_starts(start_height, end_height, chunk_size):
    h = start_height
    while h <= end_height:
        yield h
        h += chunk_size


def process_chunk(session, limiter, chunk_start, chunk_end):
    transactions = []
    incomplete = []
    for height in range(chunk_start, chunk_end + 1):
        try:
            block_meta, txs = fetch_block(session, limiter, height)
        except Exception as e:
            incomplete.append((height, str(e)))
            continue
        transactions.extend(extract_full_transactions(block_meta, txs))
    return transactions, incomplete


def main():
    session = requests.Session()
    limiter = RateLimiter(MIN_REQUEST_INTERVAL)

    r = session.get(f"{API}/getblockcount", timeout=10)
    r.raise_for_status()
    current_height = int(r.text)

    state = load_state()
    if state is None:
        state = {"target_start_height": 1, "target_end_height": current_height, "completed_chunks": []}
        save_state(state)
        print(f"Alltx-Backfill: Kein vorheriger Status. Zielbereich 1 bis {current_height}.")
    else:
        if current_height > state["target_end_height"]:
            state["target_end_height"] = current_height
        print(f"Alltx-Backfill: Setze fort — Block {state['target_start_height']} bis {state['target_end_height']}.")

    start_height = state["target_start_height"]
    end_height = state["target_end_height"]
    completed = set(state["completed_chunks"])
    all_chunks = list(chunk_starts(start_height, end_height, CHUNK_SIZE))
    pending_chunks = [c for c in all_chunks if c not in completed]

    print(f"Alltx-Backfill: {len(all_chunks)} Chunks insgesamt, {len(pending_chunks)} noch offen "
          f"({WORKERS} parallele Worker, ~{1/MIN_REQUEST_INTERVAL:.0f} req/s Takt).")

    if not pending_chunks:
        print("Alltx-Backfill: bereits vollständig.")
        return

    incomplete_f = open(INCOMPLETE_LOG, "a")
    done_count = 0
    tx_count = 0
    start_time = time.time()

    def submit(chunk_start):
        chunk_end = min(chunk_start + CHUNK_SIZE - 1, end_height)
        sess = requests.Session()
        return chunk_start, process_chunk(sess, limiter, chunk_start, chunk_end)

    try:
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = {pool.submit(submit, c): c for c in pending_chunks}
            for future in as_completed(futures):
                chunk_start = futures[future]
                try:
                    _, (transactions, incomplete) = future.result()
                except Exception as e:
                    print(f"Alltx-Backfill: Chunk {chunk_start}: FEHLER {e} — wird beim nächsten Lauf erneut versucht.")
                    continue

                append_jsonl_by_day(transactions, OUTPUT_DIR)
                tx_count += len(transactions)

                for height, err in incomplete:
                    incomplete_f.write(f"{height}\t{err}\n")
                incomplete_f.flush()

                state["completed_chunks"].append(chunk_start)
                save_state(state)

                done_count += 1
                if done_count % 5 == 0 or done_count == len(pending_chunks):
                    elapsed = time.time() - start_time
                    rate = done_count / elapsed
                    remaining = len(pending_chunks) - done_count
                    eta_min = (remaining / rate / 60) if rate > 0 else float("inf")
                    print(f"Alltx-Backfill: {done_count}/{len(pending_chunks)} Chunks fertig "
                          f"({elapsed/60:.1f} min, ETA ~{eta_min:.0f} min, {tx_count} Tx erfasst)")
    finally:
        incomplete_f.close()

    print(f"\nAlltx-Backfill: abgeschlossen. {tx_count} Transaktionen in {OUTPUT_DIR}/.")


if __name__ == "__main__":
    main()
