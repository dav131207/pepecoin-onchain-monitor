"""
Fortsetzbarer Backfill der letzten ~90 Tage Blockchain-Historie (das laufende
"Kopf"-Fenster, folgt dem aktuellen Blockstand): findet alle Netto-Transfers
>= THRESHOLD_PEP und die Coinbase-Belohnung jedes Blocks, speichert beides als
JSONL an.

Für den älteren Bereich (vor diesem 90-Tage-Fenster, z.B. bis zurück zum
Launch-Block) siehe backfill_genesis.py — nutzt dieselbe Kernlogik (run_backfill)
mit einem eigenen, unabhängigen Zustand, damit dieses laufende 90-Tage-Fenster
nicht durcheinandergebracht wird.

Fortsetzbar: der Fortschritt wird in 500-Block-Chunks je Zustandsdatei
festgehalten. Ein Abbruch (Ctrl-C, Absturz, Netzwerkfehler) verliert höchstens
den aktuell offenen Chunk — beim nächsten Start werden nur fehlende Chunks
erneut geholt.

Nutzt bewusst wenige parallele Worker mit Mindestabstand zwischen Requests,
um den Server (Chef-Mirror) nicht zu überlasten.
"""
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

from pep_client import RateLimiter, fetch_block, extract_net_transfers, extract_coinbase_reward, API

THRESHOLD_PEP = 1_000_000
BACKFILL_DAYS = 90
CHUNK_SIZE = 500
WORKERS = 6
# Empirisch getestet (Burst-Test 19.8., dedizierter Endpoint): 20 gleichzeitige
# Requests ~28 Blöcke/s bei 0 Fehlern und ~0.6s Latenz; bei 40 stieg die Latenz
# stark ohne Mehrdurchsatz. Dieses Skript teilt sich das Budget mit
# backfill_genesis.py, daher hier nur ein Teil der getesteten Kapazität.
MIN_REQUEST_INTERVAL = 0.03

STATE_FILE = "backfill_state.json"
OUTPUT_FILE = "large_transfers_3m.jsonl"
MINER_OUTPUT_FILE = "miner_rewards.jsonl"
INCOMPLETE_LOG = "blocks_incomplete.txt"


def get_current_height(session):
    r = session.get(f"{API}/getblockcount", timeout=10)
    r.raise_for_status()
    return int(r.text)


def get_block_time(session, height):
    bhash = session.get(f"{API}/getblockhash", params={"index": height}, timeout=10).text.strip()
    block = session.get(f"{API}/getblock", params={"hash": bhash, "verbosity": 1}, timeout=10).json()
    return block["time"]


def find_start_height(session, current_height, cutoff_time):
    """Binärsuche über die offizielle /api-Route (billig) für den exakten Blockindex ab cutoff_time."""
    lo, hi = 0, current_height
    while lo < hi:
        mid = (lo + hi) // 2
        t = get_block_time(session, mid)
        if t < cutoff_time:
            lo = mid + 1
        else:
            hi = mid
        time.sleep(0.1)
    return lo


def load_state(state_file):
    if os.path.exists(state_file):
        with open(state_file) as f:
            return json.load(f)
    return None


def save_state(state_file, state):
    tmp = state_file + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, state_file)


def chunk_starts(start_height, end_height, chunk_size):
    h = start_height
    while h <= end_height:
        yield h
        h += chunk_size


def process_chunk(session, limiter, chunk_start, chunk_end):
    """
    Scannt [chunk_start, chunk_end] und gibt (transfers, miner_rewards, incomplete_heights,
    first_ts, last_ts) zurück. first_ts/last_ts sind die Zeitstempel des ersten/letzten
    erfolgreich gelesenen Blocks im Chunk — daraus leitet das Dashboard ab, welcher
    Kalendertag-Bereich WIRKLICH vollständig gescannt ist (nicht nur "irgendwo ein Transfer
    gefunden"). miner_rewards enthält die Coinbase-Belohnung jedes Blocks (ein Eintrag pro
    Block) — daraus lassen sich sowohl Mining-Wallets als auch eine kostenlose
    Difficulty-Historie ableiten, ohne zusätzliche API-Calls.
    """
    transfers = []
    miner_rewards = []
    incomplete = []
    first_ts, last_ts = None, None
    for height in range(chunk_start, chunk_end + 1):
        try:
            block_meta, txs = fetch_block(session, limiter, height)
        except Exception as e:
            incomplete.append((height, str(e)))
            continue
        if first_ts is None:
            first_ts = block_meta["timestamp"]
        last_ts = block_meta["timestamp"]
        transfers.extend(extract_net_transfers(block_meta, txs, THRESHOLD_PEP))
        reward = extract_coinbase_reward(block_meta, txs)
        if reward:
            miner_rewards.append(reward)
    return transfers, miner_rewards, incomplete, first_ts, last_ts


def utc_day(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def update_contiguous_coverage(state):
    """
    Läuft die Chunks ab target_start_height in Reihenfolge ab und bestimmt, bis wohin
    LÜCKENLOS gescannt wurde (auch wenn spätere Chunks dank paralleler Worker schon
    fertig sind). Nur dieser lückenlose Bereich gilt als "abgedeckt" — nicht die
    Vereinigung aller bisher fertigen Chunks.
    """
    chunk_times = state.get("chunk_times", {})
    h = state["target_start_height"]
    covered_end_ts = None
    covered_start_ts = None
    while str(h) in chunk_times:
        rng = chunk_times[str(h)]
        if rng["start_ts"] is None:
            break
        if covered_start_ts is None:
            covered_start_ts = rng["start_ts"]
        covered_end_ts = rng["end_ts"]
        h += CHUNK_SIZE
    state["contiguous_covered_start_day"] = utc_day(covered_start_ts) if covered_start_ts else None
    state["contiguous_covered_end_day"] = utc_day(covered_end_ts) if covered_end_ts else None


def run_backfill(state_file, output_file, miner_file_path, incomplete_log,
                  determine_initial_range, auto_extend_end=False, label="Backfill",
                  min_request_interval=None, workers=None):
    """
    Gemeinsamer, fortsetzbarer Scan-Kern für einen Blockbereich — von mehreren
    unabhängigen Skripten mit jeweils eigener Zustandsdatei nutzbar (z.B. das
    laufende 90-Tage-Fenster UND ein separater Lauf für ältere Historie), damit
    sie sich nie gegenseitig durcheinanderbringen.

    determine_initial_range(session, current_height) -> (start_height, end_height)
    wird nur beim allerersten Start (keine Zustandsdatei vorhanden) aufgerufen.

    auto_extend_end=True: das Zielende folgt bei jedem Resume dem aktuellen
    Blockstand (für ein "Kopf"-Fenster, das mit der Chain mitwächst).
    auto_extend_end=False: das Zielende bleibt fix (für einen abgegrenzten
    historischen Bereich, der nicht in den Bereich eines anderen laufenden
    Jobs hineinwachsen soll).
    """
    session = requests.Session()

    state = load_state(state_file)
    current_height = get_current_height(session)

    if state is None:
        print(f"{label}: Kein vorheriger Status gefunden. Bestimme Zielbereich...")
        start_height, target_end_height = determine_initial_range(session, current_height)
        state = {
            "target_start_height": start_height,
            "target_end_height": target_end_height,
            "completed_chunks": [],
            "threshold_pep": THRESHOLD_PEP,
        }
        save_state(state_file, state)
        print(f"{label}: Zielbereich Block {start_height} bis {target_end_height} "
              f"({target_end_height - start_height + 1} Blöcke).")
    else:
        start_height = state["target_start_height"]
        if auto_extend_end and current_height > state["target_end_height"]:
            state["target_end_height"] = current_height
        print(f"{label}: Setze fort — Block {start_height} bis {state['target_end_height']}.")

    end_height = state["target_end_height"]
    completed = set(state["completed_chunks"])
    all_chunks = list(chunk_starts(start_height, end_height, CHUNK_SIZE))
    pending_chunks = [c for c in all_chunks if c not in completed]

    effective_interval = min_request_interval if min_request_interval is not None else MIN_REQUEST_INTERVAL
    effective_workers = workers if workers is not None else WORKERS

    print(f"{label}: {len(all_chunks)} Chunks insgesamt, {len(pending_chunks)} noch offen "
          f"(Chunk-Größe {CHUNK_SIZE}, {effective_workers} parallele Worker, ~{1/effective_interval:.1f} req/s Takt).")

    if not pending_chunks:
        print(f"{label}: bereits vollständig.")
        return

    limiter = RateLimiter(effective_interval)
    out_f = open(output_file, "a")
    miner_f = open(miner_file_path, "a")
    incomplete_f = open(incomplete_log, "a")

    done_count = 0
    start_time = time.time()

    def submit(chunk_start):
        chunk_end = min(chunk_start + CHUNK_SIZE - 1, end_height)
        sess = requests.Session()
        return chunk_start, process_chunk(sess, limiter, chunk_start, chunk_end)

    state.setdefault("chunk_times", {})
    state.setdefault("miner_data_chunks", [])

    try:
        with ThreadPoolExecutor(max_workers=effective_workers) as pool:
            futures = {pool.submit(submit, c): c for c in pending_chunks}
            for future in as_completed(futures):
                chunk_start = futures[future]
                try:
                    _, (transfers, miner_rewards, incomplete, first_ts, last_ts) = future.result()
                except Exception as e:
                    print(f"{label}: Chunk {chunk_start}: FEHLER {e} — wird beim nächsten Lauf erneut versucht.")
                    continue

                for r in miner_rewards:
                    miner_f.write(json.dumps(r) + "\n")
                miner_f.flush()
                state["miner_data_chunks"].append(chunk_start)

                for t in transfers:
                    out_f.write(json.dumps(t) + "\n")
                out_f.flush()

                for height, err in incomplete:
                    incomplete_f.write(f"{height}\t{err}\n")
                incomplete_f.flush()

                state["completed_chunks"].append(chunk_start)
                state["chunk_times"][str(chunk_start)] = {"start_ts": first_ts, "end_ts": last_ts}
                update_contiguous_coverage(state)
                save_state(state_file, state)

                done_count += 1
                if done_count % 5 == 0 or done_count == len(pending_chunks):
                    elapsed = time.time() - start_time
                    rate = done_count / elapsed
                    remaining = len(pending_chunks) - done_count
                    eta_min = (remaining / rate / 60) if rate > 0 else float("inf")
                    print(f"{label}: {done_count}/{len(pending_chunks)} Chunks fertig "
                          f"({elapsed/60:.1f} min, ETA ~{eta_min:.0f} min)"
                          + (f" — {len(incomplete)} unvollständige Blöcke in diesem Chunk" if incomplete else ""))
    finally:
        out_f.close()
        miner_f.close()
        incomplete_f.close()

    print(f"\n{label}: abgeschlossen. Transfers in {output_file}, Miner-Rewards in {miner_file_path}, "
          f"unvollständige Blöcke (falls vorhanden) in {incomplete_log}.")


def determine_initial_range_90d(session, current_height):
    cutoff_time = int(time.time()) - BACKFILL_DAYS * 86400
    start_height = find_start_height(session, current_height, cutoff_time)
    return start_height, current_height


def main():
    run_backfill(
        state_file=STATE_FILE,
        output_file=OUTPUT_FILE,
        miner_file_path=MINER_OUTPUT_FILE,
        incomplete_log=INCOMPLETE_LOG,
        determine_initial_range=determine_initial_range_90d,
        auto_extend_end=True,
        label="90-Tage-Backfill",
    )


if __name__ == "__main__":
    main()
