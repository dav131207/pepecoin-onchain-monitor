"""
Dritter, unabhängiger Backfill-Pass: scannt JEDE Transaktion (nicht nur Netto-
Transfers >= 1M PEP wie backfill_history.py/backfill_genesis.py) von Block 1 bis
zum aktuellen Blockstand — ABER speichert keine Rohtransaktionen mehr. Stattdessen
werden vier kompakte Reduktionen direkt beim Scan berechnet und gepflegt:

  - address_clusters.json       (Common-Input-Ownership-Clustering)
  - coin_days_destroyed.json    (CDD, täglich)
  - hodl_waves.json             (Supply nach Haltedauer, Momentaufnahme)
  - fee_stats.json              (Gebühren-Statistik, täglich)

Frühere Version schrieb jede Transaktion roh nach all_transactions/YYYY-MM-DD.jsonl.
Das riss am 31.08.2026 GitHubs 100-MB-Datei-Limit erneut (2024-11-19.jsonl: 311 MB
an einem einzigen dicht bespielten Tag) — Tages-Bucketing konzentriert Last exakt
dort, wo sie am größten ist. Jeder Downstream-Konsument (Clustering, Coin-Age,
Fees) braucht aber nie die Rohdaten selbst, nur diese vier Aggregate — also werden
sie direkt beim Scan gebildet statt in separaten Skripten aus Rohdaten nachträglich
berechnet. address_clustering.py und coin_age_analysis.py entfallen dadurch.

Einzige verbleibende Rohspeicherung ist ein kompakter UTXO-Index
(utxo_index/{shard}.{outputs,spent}.jsonl, siehe pep_client.load_utxo_index/
append_utxo_shard) — nötig, weil eine Spend-Transaktion ihre Ursprungs-Ausgabe
(für Coin-Age) und Clustering-Zuordnung über beliebig viele Läufe/Zeiträume
hinweg wiederfinden muss. Nach Txid-Präfix gesharded (nicht nach Zeit) und pro
UTXO nur ~1 kompakte Zeile — bleibt auch bei sehr dichten Perioden weit unter
dem 100-MB-Limit (siehe pep_client.utxo_shard).

Korrektheit hängt daran, dass jede Transaktion in STRIKTER Blockhöhen-Reihenfolge
verarbeitet wird: eine Ausgabe kann laut Blockchain-Konstruktion nur VOR ihrem
Spend existieren, also ist ihr Ursprungs-Eintrag garantiert schon im Index, wenn
der Spend verarbeitet wird — vorausgesetzt, wir verarbeiten nie einen späteren
Block, bevor ein früherer fertig ist. Das Fetchen bleibt parallel (16 Worker),
aber ein Reorder-Buffer sorgt dafür, dass Chunks nur in aufsteigender Reihenfolge
in den Aggregator einfließen, auch wenn sie out-of-order fertig werden (siehe
main()). Für den seltenen Fall, dass eine Ursprungs-Ausgabe trotzdem fehlt (z.B.
Datenlücke durch einen übersprungenen Block), landet der Spend in
alltx_orphan_spends.json und wird bei jedem künftigen Lauf erneut versucht.

Fortsetzbar über backfill_alltx_state.json (next_height-Cursor + covered_days).
Zwischenstände (Index-Shards + alle vier Aggregat-Dateien) werden alle
CHECKPOINT_EVERY_CHUNKS Chunks UND am Lauf-Ende geschrieben, damit ein Timeout
höchstens den offenen Checkpoint-Zeitraum kostet.

Rührt NICHT an backfill_state.json oder backfill_genesis_state.json — läuft
unabhängig neben den beiden bestehenden Läufen (dieselben Blöcke werden also
bewusst dreifach von der API geholt, statt deren Fortschritt zu riskieren).
"""
import json
import os
import signal
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

from pep_client import (
    RateLimiter, fetch_block, extract_full_transactions, API,
    UTXO_INDEX_DIR, load_utxo_index, append_utxo_shard, utxo_shard, day_key,
)

STATE_FILE = "backfill_alltx_state.json"
INCOMPLETE_LOG = "blocks_incomplete_alltx.txt"

CLUSTERS_STATE_FILE = "address_clusters_state.json"
CLUSTERS_OUTPUT_FILE = "address_clusters.json"
CDD_OUTPUT_FILE = "coin_days_destroyed.json"
FEE_OUTPUT_FILE = "fee_stats.json"
HODL_OUTPUT_FILE = "hodl_waves.json"
ORPHAN_FILE = "alltx_orphan_spends.json"

CHUNK_SIZE = 500
WORKERS = 16
MIN_REQUEST_INTERVAL = 0.02
CHECKPOINT_EVERY_CHUNKS = 20  # ~10.000 Blöcke zwischen Zwischenständen
MIN_CLUSTER_SIZE = 2

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


def load_json(path, default):
    if os.path.exists(path):
        with open(path) as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                pass
    return default


def save_json_atomic(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)
    os.replace(tmp, path)


def load_state():
    return load_json(STATE_FILE, None)


def save_state(state):
    save_json_atomic(STATE_FILE, state)


class UnionFind:
    def __init__(self, parent=None):
        self.parent = dict(parent) if parent else {}

    def find(self, x):
        if x not in self.parent:
            self.parent[x] = x
            return x
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != root:
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[ra] = rb


def bucket_for_age(age_days):
    for label, lo, hi in HODL_BUCKETS_DAYS:
        if lo <= age_days < hi:
            return label
    return HODL_BUCKETS_DAYS[-1][0]


def percentile(sorted_vals, pct):
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, int(len(sorted_vals) * pct))
    return sorted_vals[idx]


def chunk_starts(start_height, end_height, chunk_size):
    h = start_height
    while h <= end_height:
        yield h
        h += chunk_size


def fetch_chunk(chunk_start, chunk_end, limiter):
    session = requests.Session()
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


class Aggregator:
    """
    Hält den kompletten laufenden Zustand für einen Backfill-Lauf: UTXO-Index
    (Ursprung + Spent, für Coin-Age/HODL), Fee-Tagesstatistik, Adress-Clustering.
    process_tx() MUSS strikt in Blockhöhen-Reihenfolge aufgerufen werden (siehe
    Modul-Docstring) — sonst kann ein Spend seine Ursprungs-Ausgabe fälschlich
    als "noch nicht gescannt" einstufen.
    """

    def __init__(self):
        self.outputs, self.spent = load_utxo_index()
        self.uf = UnionFind(load_json(CLUSTERS_STATE_FILE, {}))
        self.cdd_by_day = load_json(CDD_OUTPUT_FILE, {}).get("daily", {})
        self.fee_by_day = load_json(FEE_OUTPUT_FILE, {}).get("daily", {})
        self.orphans = load_json(ORPHAN_FILE, [])  # [[prev_txid, prev_vout, spend_time], ...]

        self._new_outputs_by_shard = defaultdict(list)
        self._new_spent_by_shard = defaultdict(list)
        self._open_day = None
        self._open_day_fees = []
        self.tx_count = 0

        self._resolve_orphans()

    def _resolve_orphans(self):
        still_open = []
        resolved = 0
        for prev_txid, prev_vout, spend_time in self.orphans:
            origin = self.outputs.get((prev_txid, prev_vout))
            if origin is None:
                still_open.append([prev_txid, prev_vout, spend_time])
                continue
            _, value, origin_ts = origin
            self._add_cdd(spend_time, value, origin_ts)
            resolved += 1
        if resolved:
            print(f"Alltx-Backfill: {resolved} Orphan-Spends aus vorherigen Läufen aufgelöst.")
        self.orphans = still_open

    def _add_cdd(self, spend_time, value, origin_ts):
        age_days = max(0.0, (spend_time - origin_ts) / 86400)
        day = day_key(spend_time)
        self.cdd_by_day[day] = self.cdd_by_day.get(day, 0.0) + value * age_days

    def _flush_open_day(self):
        """Schreibt den aktuell offenen Tag final in fee_by_day und leert den
        Rohwerte-Puffer. Darf NUR aufgerufen werden, wenn feststeht, dass keine
        weitere Transaktion dieses Tages mehr kommt (Tageswechsel oder Lauf-Ende)
        — sonst würde ein Zwischenstand den Tag vorzeitig als vollständig markieren."""
        if self._open_day is None or not self._open_day_fees:
            return
        vals = sorted(self._open_day_fees)
        n = len(vals)
        self.fee_by_day[self._open_day] = {
            "tx_count_with_fee": n,
            "total_fee_pep": round(sum(vals), 8),
            "avg_fee_pep": round(sum(vals) / n, 8),
            "median_fee_pep": round(percentile(vals, 0.5), 8),
            "p90_fee_pep": round(percentile(vals, 0.9), 8),
        }
        self._open_day_fees = []

    def _fee_snapshot(self):
        """Wie fee_by_day, aber inkl. eines VORLÄUFIGEN (nicht finalisierten)
        Eintrags für den gerade offenen Tag — nur für Checkpoint-Schreibvorgänge,
        mutiert nichts. Der Tag wird bei Fertigstellung (Tageswechsel/Lauf-Ende)
        über _flush_open_day() final geschrieben."""
        snapshot = dict(self.fee_by_day)
        if self._open_day and self._open_day_fees:
            vals = sorted(self._open_day_fees)
            n = len(vals)
            snapshot[self._open_day] = {
                "tx_count_with_fee": n,
                "total_fee_pep": round(sum(vals), 8),
                "avg_fee_pep": round(sum(vals) / n, 8),
                "median_fee_pep": round(percentile(vals, 0.5), 8),
                "p90_fee_pep": round(percentile(vals, 0.9), 8),
            }
        return snapshot

    def process_tx(self, tx):
        self.tx_count += 1
        txid = tx["txid"]
        ts = tx["time"]
        day = day_key(ts)

        if day != self._open_day:
            self._flush_open_day()
            self._open_day = day

        for o in tx.get("outputs", []):
            key = (txid, o["vout"])
            addr = o.get("address")
            value = o.get("amount", 0.0)
            self.outputs[key] = (addr, value, ts)
            self._new_outputs_by_shard[utxo_shard(txid)].append({
                "txid": txid, "vout": o["vout"], "address": addr, "value": value, "time": ts,
            })

        input_addrs = set()
        for i in tx.get("inputs", []):
            prev_txid = i.get("prev_txid")
            prev_vout = i.get("prev_vout")
            addr = i.get("address")
            if addr:
                input_addrs.add(addr)
            if prev_txid is None:
                continue
            key = (prev_txid, prev_vout)
            self.spent.add(key)
            self._new_spent_by_shard[utxo_shard(prev_txid)].append({"txid": prev_txid, "vout": prev_vout})

            origin = self.outputs.get(key)
            if origin is None:
                self.orphans.append([prev_txid, prev_vout, ts])
                continue
            _, value, origin_ts = origin
            self._add_cdd(ts, value, origin_ts)

        if tx.get("fee") is not None:
            self._open_day_fees.append(tx["fee"])

        if len(input_addrs) >= 2:
            addrs = list(input_addrs)
            first = addrs[0]
            for a in addrs[1:]:
                self.uf.union(first, a)

    def checkpoint(self, covered_days, target_end_height, next_height):
        for shard, lines in self._new_outputs_by_shard.items():
            append_utxo_shard(UTXO_INDEX_DIR, shard, "outputs", lines)
        for shard, lines in self._new_spent_by_shard.items():
            append_utxo_shard(UTXO_INDEX_DIR, shard, "spent", lines)
        self._new_outputs_by_shard.clear()
        self._new_spent_by_shard.clear()

        save_json_atomic(CLUSTERS_STATE_FILE, self.uf.parent)
        save_json_atomic(ORPHAN_FILE, self.orphans)

        clusters = {}
        for addr in self.uf.parent:
            root = self.uf.find(addr)
            clusters.setdefault(root, []).append(addr)
        real_clusters = [m for m in clusters.values() if len(m) >= MIN_CLUSTER_SIZE]
        real_clusters.sort(key=len, reverse=True)
        save_json_atomic(CLUSTERS_OUTPUT_FILE, {
            "updated": datetime.now(timezone.utc).isoformat(),
            "method": "common-input-ownership (Union-Find über Tx-Eingabe-Adressgruppen)",
            "caveat": "Näherungsheuristik, nicht validiert. CoinJoin-artige Tx würden fälschlich "
                       "zusammengeführt. Deckt nur covered_days ab.",
            "covered_days": covered_days,
            "transactions_scanned": self.tx_count,
            "addresses_in_clusters": sum(len(m) for m in real_clusters),
            "cluster_count": len(real_clusters),
            "clusters": [
                {"cluster_id": i, "size": len(m), "addresses": sorted(m)}
                for i, m in enumerate(real_clusters)
            ],
        })

        save_json_atomic(CDD_OUTPUT_FILE, {
            "updated": datetime.now(timezone.utc).isoformat(),
            "unit": "PEP * Tage (Betrag mal Alter der ausgegebenen Coins bei diesem Spend)",
            "caveat": "Nur Spends, deren Ursprungs-Ausgabe ebenfalls im bislang gescannten Bereich "
                       "liegt (siehe covered_days), gehen ein.",
            "covered_days": covered_days,
            "daily": self.cdd_by_day,
        })

        save_json_atomic(FEE_OUTPUT_FILE, {
            "updated": datetime.now(timezone.utc).isoformat(),
            "unit": "PEP",
            "caveat": "Gebühr = Summe Eingaben minus Summe Ausgaben je Tx (nur reguläre Tx, keine "
                       "Coinbase). Nur Tage in covered_days sind vollständig.",
            "covered_days": covered_days,
            "daily": self._fee_snapshot(),
        })

        now = datetime.now(timezone.utc).timestamp()
        unspent = {k: v for k, v in self.outputs.items() if k not in self.spent}
        bucket_totals = {label: 0.0 for label, _, _ in HODL_BUCKETS_DAYS}
        bucket_counts = {label: 0 for label, _, _ in HODL_BUCKETS_DAYS}
        total_value = 0.0
        for (_, value, ts) in unspent.values():
            age_days = max(0.0, (now - ts) / 86400)
            label = bucket_for_age(age_days)
            bucket_totals[label] += value
            bucket_counts[label] += 1
            total_value += value
        save_json_atomic(HODL_OUTPUT_FILE, {
            "updated": datetime.now(timezone.utc).isoformat(),
            "caveat": "Momentaufnahme über covered_days — solange next_height < aktueller Blockstand "
                       "können Coins fälschlich als unverbraucht erscheinen, wenn ihr Spend in einem "
                       "noch ungescannten Block liegt.",
            "covered_days": covered_days,
            "next_height": next_height,
            "target_end_height": target_end_height,
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
        })


class GracefulShutdown(Exception):
    """Ausgelöst durch SIGTERM (z.B. `timeout 5h ...` im Workflow) — erlaubt einen
    sauberen letzten Checkpoint statt eines abrupten Abbruchs mitten im Chunk-Fluss."""


def _handle_sigterm(signum, frame):
    raise GracefulShutdown()


def main():
    signal.signal(signal.SIGTERM, _handle_sigterm)

    session = requests.Session()
    limiter = RateLimiter(MIN_REQUEST_INTERVAL)

    r = session.get(f"{API}/getblockcount", timeout=10)
    r.raise_for_status()
    current_height = int(r.text)

    state = load_state()
    if state is None:
        state = {"next_height": 1, "target_end_height": current_height, "covered_days": []}
        save_state(state)
        print(f"Alltx-Backfill: Kein vorheriger Status. Ziel: Block 1 bis {current_height}.")
    else:
        if current_height > state["target_end_height"]:
            state["target_end_height"] = current_height
        state.setdefault("covered_days", [])
        print(f"Alltx-Backfill: Setze fort ab Block {state['next_height']} (Ziel {state['target_end_height']}).")

    target_end_height = state["target_end_height"]
    next_height = state["next_height"]

    if next_height > target_end_height:
        print("Alltx-Backfill: bereits vollständig.")
        return

    chunks = list(chunk_starts(next_height, target_end_height, CHUNK_SIZE))
    print(f"Alltx-Backfill: {len(chunks)} Chunks zu je {CHUNK_SIZE} Blöcken "
          f"({WORKERS} parallele Fetch-Worker, sequentielle Verarbeitung in Blockreihenfolge).")

    covered_days = set(state["covered_days"])
    agg = Aggregator()

    incomplete_f = open(INCOMPLETE_LOG, "a")
    start_time = time.time()
    chunks_drained = 0

    pool = ThreadPoolExecutor(max_workers=WORKERS)
    try:
        futures = {pool.submit(fetch_chunk, c, min(c + CHUNK_SIZE - 1, target_end_height), limiter): c
                   for c in chunks}
        pending_results = {}
        expected = chunks[0]

        for future in as_completed(futures):
            chunk_start = futures[future]
            try:
                transactions, incomplete = future.result()
            except Exception as e:
                print(f"Alltx-Backfill: Chunk {chunk_start}: FEHLER {e} — erneuter Versuch beim nächsten Lauf.")
                continue
            pending_results[chunk_start] = (transactions, incomplete)

            while expected in pending_results:
                transactions, incomplete = pending_results.pop(expected)
                for tx in transactions:
                    agg.process_tx(tx)
                    covered_days.add(day_key(tx["time"]))
                for height, err in incomplete:
                    incomplete_f.write(f"{height}\t{err}\n")
                incomplete_f.flush()

                next_height = expected + CHUNK_SIZE
                expected = next_height
                chunks_drained += 1

                if chunks_drained % CHECKPOINT_EVERY_CHUNKS == 0:
                    # Checkpoint IMMER vor dem State-Save: bricht der Prozess dazwischen
                    # ab, zeigt backfill_alltx_state.json noch die ALTE next_height, und
                    # der nächste Lauf verarbeitet den Bereich einfach erneut (sicher dank
                    # append-only/idempotenter Shards). Umgekehrt (State zuerst) hätte
                    # next_height Fortschritt behauptet, den die Aggregate nie bekommen
                    # haben — genau das erzeugte beim ersten Testlauf 227 nie auflösbare
                    # Orphan-Spends.
                    covered_days_sorted = sorted(covered_days)
                    agg.checkpoint(covered_days_sorted, target_end_height, next_height)
                    state["next_height"] = next_height
                    state["covered_days"] = covered_days_sorted
                    save_state(state)
                    elapsed = time.time() - start_time
                    rate = chunks_drained / elapsed
                    remaining = len(chunks) - chunks_drained
                    eta_min = (remaining / rate / 60) if rate > 0 else float("inf")
                    print(f"Alltx-Backfill: Checkpoint bei Block {next_height} "
                          f"({chunks_drained}/{len(chunks)} Chunks, {elapsed/60:.1f} min, "
                          f"ETA ~{eta_min:.0f} min, {agg.tx_count} Tx verarbeitet, "
                          f"{len(agg.orphans)} offene Orphan-Spends)")
    except GracefulShutdown:
        print("Alltx-Backfill: SIGTERM erhalten (Timeout) — breche offene Chunks ab, speichere Zwischenstand...")
    finally:
        # cancel_futures verwirft noch nicht gestartete Chunks sofort, statt (wie das
        # `with`-Statement es täte) auf ALLE ~tausend vorab eingereihten Futures zu
        # warten — sonst hätte ein SIGTERM nie rechtzeitig zu einem sauberen Checkpoint
        # geführt, bevor der Runner ohnehin hart gekillt wird.
        pool.shutdown(wait=True, cancel_futures=True)
        incomplete_f.close()
        agg._flush_open_day()
        covered_days_sorted = sorted(covered_days)
        agg.checkpoint(covered_days_sorted, target_end_height, next_height)
        state["next_height"] = next_height
        state["covered_days"] = covered_days_sorted
        save_state(state)

    print(f"\nAlltx-Backfill: Lauf beendet bei Block {next_height}/{target_end_height}. "
          f"{agg.tx_count} Tx verarbeitet, {len(agg.orphans)} offene Orphan-Spends.")


if __name__ == "__main__":
    main()
