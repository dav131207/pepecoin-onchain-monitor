"""
Gemeinsamer Client für die SvelteKit-SSR-Datenrouten des Pepeblocks-Explorer-Spiegels
(avivppblocks.realmasterkush.com). Diese Routen sind nicht offiziell dokumentiert
(interne Ladefunktionen des Frontends), liefern aber pro Block/Adresse bereits
aufgelöste Esplora-artige Daten inkl. Absenderadressen (vin[].prevout) — das spart
gegenüber dem dokumentierten /api-Pfad die separate Auflösung jeder Eingabe.

Validiert gegen /api/getblockhash + /api/getblock(verbosity=2) + /api/getrawtransaction
auf 20 Stichproben-Blöcken (u.a. ein 449-Tx-Block) — 0 Abweichungen.

Werte auf dieser Route sind 1e8-skaliert (wie Satoshi) und werden von diesem Modul
bereits in PEP umgerechnet zurückgegeben.
"""
import glob
import json
import os
import random
import threading
import time
from datetime import datetime, timezone

import requests

HOST = "https://avivppblocks.realmasterkush.com"
API = f"{HOST}/api"

SAT = 100_000_000

LARGE_TRANSFERS_DIR = "large_transfers"
MINER_REWARDS_DIR = "miner_rewards"
ALL_TRANSACTIONS_DIR = "all_transactions"


def month_key(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m")


def day_key(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def _update_index(dir_path, new_keys):
    """Pflegt dir_path/index.json (sortierte Liste der Datei-Keys ohne .jsonl) — das
    Dashboard (statisches HTML, kein Server-Verzeichnislisting) braucht diese
    Liste, um zu wissen, welche Dateien es per fetch() laden soll."""
    index_path = os.path.join(dir_path, "index.json")
    existing = set()
    if os.path.exists(index_path):
        with open(index_path) as f:
            try:
                existing = set(json.load(f))
            except json.JSONDecodeError:
                pass
    updated = existing | new_keys
    if updated != existing:
        with open(index_path, "w") as f:
            json.dump(sorted(updated), f)


def _append_jsonl_bucketed(records, dir_path, key_fn):
    if not records:
        return
    os.makedirs(dir_path, exist_ok=True)
    by_key = {}
    for r in records:
        by_key.setdefault(key_fn(r["time"]), []).append(r)
    for key, items in by_key.items():
        with open(os.path.join(dir_path, f"{key}.jsonl"), "a") as f:
            for r in items:
                f.write(json.dumps(r) + "\n")
    _update_index(dir_path, set(by_key.keys()))


def append_jsonl_by_month(records, dir_path):
    """
    Verteilt Records auf monatliche Dateien (dir_path/YYYY-MM.jsonl), damit keine
    einzelne Datei je in Richtung GitHubs 100-MB-Hard-Limit wächst (miner_rewards.jsonl
    riss das am 26.8. bei ~119 MB — jeder Push seitdem wurde hart abgelehnt, siehe
    README). Jeder Record braucht ein "time"-Feld (Unix-Sekunden). Passend für Datensätze
    mit niedriger Dichte (ein Eintrag pro Block oder seltener).
    """
    _append_jsonl_bucketed(records, dir_path, month_key)


def append_jsonl_by_day(records, dir_path):
    """
    Wie append_jsonl_by_month, aber pro Tag statt pro Monat — für dichtere Datensätze
    (z.B. alle Transaktionen statt nur Großtransfers/Coinbase), bei denen ein Monat
    zu groß würde (siehe README, all_transactions/).
    """
    _append_jsonl_bucketed(records, dir_path, day_key)


def read_jsonl_dir(dir_path):
    """Generator über alle Records in allen dir_path/*.jsonl-Dateien, Monatsdateien aufsteigend sortiert."""
    for path in sorted(glob.glob(os.path.join(dir_path, "*.jsonl"))):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue


class RateLimiter:
    """
    Erzwingt einen globalen Mindestabstand zwischen Request-*Starts* über alle
    Worker hinweg (echter Token-Bucket-Takt, kein Multiplikator pro Worker).
    Die Slot-Reservierung passiert kurz unter Lock, das eigentliche Warten
    danach außerhalb — so können mehrere Worker parallel auf ihren jeweils
    reservierten Slot warten, ohne dass sich die Requests gegenseitig blockieren.
    """

    def __init__(self, min_interval):
        self.min_interval = min_interval
        self._next_slot = 0.0
        self._lock = threading.Lock()

    def wait(self):
        with self._lock:
            now = time.monotonic()
            target = max(self._next_slot, now)
            self._next_slot = target + self.min_interval
        delay = target - time.monotonic() + random.uniform(0, self.min_interval * 0.3)
        if delay > 0:
            time.sleep(delay)


def get_with_retry(session, url, limiter, params=None, max_retries=5, timeout=20):
    for attempt in range(max_retries):
        limiter.wait()
        try:
            r = session.get(url, params=params, timeout=timeout)
        except requests.RequestException as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)
            continue
        if r.status_code == 200:
            return r
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep((2 ** attempt) + random.uniform(0, 1))
            continue
        r.raise_for_status()
    raise RuntimeError(f"Max retries exceeded for {url}")


def resolve(arr, idx, cache=None):
    """Löst SvelteKit's 'devalue'-artiges, referenzbasiertes JSON-Format auf."""
    if cache is None:
        cache = {}
    if idx in cache:
        return cache[idx]
    v = arr[idx]
    if isinstance(v, dict):
        out = {}
        cache[idx] = out
        for k, ref in v.items():
            out[k] = resolve(arr, ref, cache)
        return out
    elif isinstance(v, list):
        out = []
        cache[idx] = out
        for ref in v:
            out.append(resolve(arr, ref, cache))
        return out
    else:
        return v


def fetch_block(session, limiter, height):
    """
    Holt einen vollständigen Block (alle Transaktionen, inkl. aufgelöster
    Absenderadressen aus vin[].prevout), paginiert bei Bedarf.
    Wirft AssertionError, wenn die Anzahl gelesener Transaktionen nicht mit
    dem gemeldeten tx_count übereinstimmt (z.B. bei einem API-Fehlverhalten).
    """
    txs = {}
    page = 1
    total_pages = 1
    block_meta = None
    while page <= total_pages:
        r = get_with_retry(session, f"{HOST}/block/{height}/__data.json", limiter, params={"page": page})
        arr = r.json()["nodes"][2]["data"]
        root = resolve(arr, 0)
        block_meta = root["block"]
        total_pages = root.get("totalPages", 1)
        for tx in root["txs"]:
            txs[tx["txid"]] = tx
        page += 1

    if len(txs) != block_meta["tx_count"]:
        raise AssertionError(
            f"Block {height}: {len(txs)} Transaktionen gelesen, {block_meta['tx_count']} erwartet"
        )

    return block_meta, list(txs.values())


def fetch_address_stats(session, limiter, address):
    """Holt Kontostand + Tx-Statistik einer Adresse (Werte bereits in PEP)."""
    r = get_with_retry(session, f"{HOST}/address/{address}/__data.json", limiter)
    arr = r.json()["nodes"][2]["data"]
    root = resolve(arr, 0)
    info = root["addressData"]["info"]
    chain = info["chain_stats"]
    return {
        "address": info["address"],
        "balance": root["addressData"]["balance"] / SAT,
        "tx_count": chain["tx_count"],
        "funded_txo_count": chain["funded_txo_count"],
        "funded_txo_sum": float(chain["funded_txo_sum"]) / SAT,
        "spent_txo_count": chain["spent_txo_count"],
        "spent_txo_sum": float(chain["spent_txo_sum"]) / SAT,
    }


def is_coinbase_tx(tx):
    """Gemeinsames Kriterium für Coinbase-Erkennung — von extract_net_transfers (zum
    Ausschließen) UND extract_coinbase_reward (zum gezielten Erfassen) genutzt, damit
    beide niemals unterschiedlich entscheiden, was eine Coinbase-Tx ist."""
    return any(v.get("is_coinbase") for v in tx.get("vin", []))


def extract_coinbase_reward(block_meta, txs):
    """
    Erfasst die Block-Belohnung (Coinbase-Auszahlung) eines Blocks — die Adresse(n),
    die den Mining-Reward erhalten. Wiederholte Treffer derselben Adresse über viele
    Blöcke hinweg sind ein sehr starkes Signal für eine Mining-Pool-/Solo-Miner-Wallet
    (im Gegensatz zu Exchange-Wallets, die nur an Tx-Aktivität erkennbar sind).
    Manche Pools splitten die Belohnung auf mehrere Adressen (z.B. Pool + Spende) —
    daher eine Liste statt einer einzelnen Adresse.
    """
    for tx in txs:
        if not is_coinbase_tx(tx):
            continue
        payouts = []
        for vout in tx.get("vout", []):
            addr = vout.get("scriptpubkey_address")
            if not addr:
                continue
            payouts.append({"address": addr, "amount": round(vout["value"] / SAT, 8)})
        if payouts:
            return {
                "block": block_meta["height"],
                "time": block_meta["timestamp"],
                "difficulty": block_meta.get("difficulty"),
                "tx_count": block_meta.get("tx_count"),
                "payouts": payouts,
            }
    return None


def extract_net_transfers(block_meta, txs, threshold_pep):
    """
    Filtert Transaktionen eines Blocks auf Netto-Transfers >= threshold_pep,
    d.h. Wechselgeld an dieselbe Eingabe-Adresse wird herausgerechnet.
    Coinbase-Transaktionen werden übersprungen (kein echter Transfer).
    """
    results = []
    for tx in txs:
        if is_coinbase_tx(tx):
            continue

        input_addrs = {}
        for v in tx.get("vin", []):
            prevout = v.get("prevout")
            if not prevout:
                continue
            addr = prevout.get("scriptpubkey_address")
            if not addr:
                continue
            input_addrs[addr] = input_addrs.get(addr, 0) + prevout["value"] / SAT

        outputs = []
        net_total = 0.0
        for vout in tx.get("vout", []):
            addr = vout.get("scriptpubkey_address")
            value = vout["value"] / SAT
            outputs.append({"address": addr, "amount": value})
            if addr not in input_addrs:
                net_total += value

        if net_total >= threshold_pep:
            results.append({
                "txid": tx["txid"],
                "block": block_meta["height"],
                "time": block_meta["timestamp"],
                "net_amount": round(net_total, 8),
                "from": [{"address": a, "amount": round(amt, 8)} for a, amt in input_addrs.items()],
                "to": [o for o in outputs if o["address"] not in input_addrs],
            })
    return results


def extract_full_transactions(block_meta, txs):
    """
    JEDE Transaktion eines Blocks (nicht nur Netto-Transfers >= Schwelle), roh
    (keine Wechselgeld-Nettoberechnung) — Grundlage für Analysen, die die
    bestehenden ≥1M-PEP-Großtransfers nicht abdecken können: echtes Tx-Volumen,
    Gebühren, und per Offline-Join über (txid, vout) Coin-Age/Coin-Days-Destroyed/
    HODL-Waves sowie Common-Input-Adress-Clustering.

    Jede Ausgabe (auch von Coinbase-Tx) wird mit ihrem eigenen "vout"-Index
    zurückgegeben — das ist der spätere Join-Schlüssel: eine spätere Transaktion,
    die diese Ausgabe ausgibt, referenziert sie exakt über
    input["prev_txid"] == diese "txid" und input["prev_vout"] == dieser "vout".
    Coinbase-Ausgaben MÜSSEN mit erfasst werden, sonst hat jede Münze, die noch
    in ihrer ursprünglichen Mining-Auszahlung steckt, beim Spend keinen
    Ursprungs-Eintrag zum Joinen (Coin-Age wäre für sie nicht berechenbar).

    "inputs" bleibt pro Transaktion gruppiert (nicht über den Block aggregiert),
    weil Adress-Clustering (Common-Input-Ownership-Heuristik: alle Eingabe-
    Adressen einer Tx gehören vermutlich derselben Entität) genau diese Gruppierung
    braucht.
    """
    results = []
    for tx in txs:
        coinbase = is_coinbase_tx(tx)

        outputs = []
        output_total = 0.0
        for i, vout in enumerate(tx.get("vout", [])):
            addr = vout.get("scriptpubkey_address")
            value = vout["value"] / SAT
            outputs.append({"vout": i, "address": addr, "amount": round(value, 8)})
            output_total += value

        if coinbase:
            results.append({
                "txid": tx["txid"],
                "block": block_meta["height"],
                "time": block_meta["timestamp"],
                "is_coinbase": True,
                "fee": None,
                "inputs": [],
                "outputs": outputs,
            })
            continue

        inputs = []
        input_total = 0.0
        for v in tx.get("vin", []):
            prevout = v.get("prevout")
            if not prevout:
                continue
            value = prevout["value"] / SAT
            input_total += value
            inputs.append({
                "prev_txid": v.get("txid"),
                "prev_vout": v.get("vout"),
                "address": prevout.get("scriptpubkey_address"),
                "amount": round(value, 8),
            })

        results.append({
            "txid": tx["txid"],
            "block": block_meta["height"],
            "time": block_meta["timestamp"],
            "is_coinbase": False,
            "fee": round(input_total - output_total, 8) if inputs else None,
            "inputs": inputs,
            "outputs": outputs,
        })
    return results
