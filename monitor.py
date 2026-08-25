import os
import json
import time
from datetime import datetime, timezone, timedelta
import requests

from pep_client import RateLimiter, fetch_block, extract_net_transfers, extract_coinbase_reward, API
from price_client import update_price_history, update_ohlc

DATA_DIR = "snapshots"
KNOWN_WHALES_FILE = "known_whales.json"
STATE_FILE = "last_scanned_block.txt"
EXCHANGE_CANDIDATES_FILE = "exchange_candidates.json"
MINER_REWARDS_FILE = "miner_rewards.jsonl"
NETWORK_STATS_FILE = "network_stats.json"
BASE_URL = API

THRESHOLD_PEP = 1_000_000  # Mindest-Nettotransfer, der als "große Transaktion" gilt
MAX_BLOCKS_PER_RUN = 20000  # Sicherheitsventil, deckelt nur das Ende des Fensters
MIN_REQUEST_INTERVAL = 0.35  # Höflicher Takt gegen den SSR-Endpoint des Mirrors

def load_known_whales():
    if os.path.exists(KNOWN_WHALES_FILE):
        with open(KNOWN_WHALES_FILE, "r") as f:
            try:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
            except json.JSONDecodeError:
                return {}
    return {}

def save_known_whales(whales_dict):
    with open(KNOWN_WHALES_FILE, "w") as f:
        json.dump(whales_dict, f, indent=4)

def load_exchange_candidates(min_tx_count=200):
    """
    Adressen mit auffällig hoher Tx-Aktivität aus dem letzten Ranking-Lauf
    (rank_exchange_candidates.py) — schließt erkannte Miner-Wallets aus, da
    wiederholte Coinbase-Auszahlungen ebenfalls hohe Tx-Aktivität erzeugen,
    aber keine Exchange-Bewegung sind.
    """
    if not os.path.exists(EXCHANGE_CANDIDATES_FILE):
        return set()
    with open(EXCHANGE_CANDIDATES_FILE) as f:
        data = json.load(f)
    return {
        c["address"] for c in data.get("candidates", [])
        if c.get("tx_count", 0) >= min_tx_count and not c.get("is_miner")
    }

def update_network_stats(session):
    """Best-effort: aktuelle Hashrate/Difficulty, in Tages-Historie gemerged (günstig: 2 Calls/Lauf)."""
    try:
        hashrate = float(session.get(f"{BASE_URL}/getnetworkhashps", timeout=10).text)
        difficulty = float(session.get(f"{BASE_URL}/getdifficulty", timeout=10).text)
    except Exception as e:
        print(f"Netzwerk-Stats: Abruf fehlgeschlagen ({e})")
        return

    history = {}
    if os.path.exists(NETWORK_STATS_FILE):
        with open(NETWORK_STATS_FILE) as f:
            try:
                history = json.load(f)
            except json.JSONDecodeError:
                history = {}

    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    history[day] = {"hashrate": hashrate, "difficulty": difficulty, "updated": datetime.now().isoformat()}
    with open(NETWORK_STATS_FILE, "w") as f:
        json.dump(history, f, indent=2, sort_keys=True)
    print(f"Netzwerk-Stats aktualisiert: Hashrate={hashrate:.3e} H/s, Difficulty={difficulty:.0f}")

def get_last_scanned_block(current_height):
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            try:
                return int(f.read().strip())
            except:
                pass
    # Default: Scanne die letzten 2880 Blöcke (ca. 48h), falls kein Status existiert
    return max(0, current_height - 2880)

def set_last_scanned_block(height):
    with open(STATE_FILE, "w") as f:
        f.write(str(height))

HEAD_BACKFILL_STATE_FILE = "backfill_state.json"
GENESIS_BACKFILL_STATE_FILE = "backfill_genesis_state.json"
MINER_REWARDS_FOR_TX_COUNTS = "miner_rewards.jsonl"


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
    return (datetime.strptime(start, "%Y-%m-%d").date(),
            datetime.strptime(end, "%Y-%m-%d").date())


def _covered_days_set():
    """
    Vereinigt die lückenlos gescannten Tagesbereiche aus beiden Backfill-Läufen
    (Genesis- und 90-Tage-Kopf-Fenster). Nur Tage in dieser Menge dürfen für
    transaction_counts als vollständig gezählt gelten — alles andere (inkl. der
    von sample_miner_history.py nur alle 10 Blöcke gesampelten Bereiche) fehlt
    hier bewusst, damit keine Untererfassung als "0 Transaktionen" missverstanden
    werden kann.
    """
    covered = set()
    for state_file in (GENESIS_BACKFILL_STATE_FILE, HEAD_BACKFILL_STATE_FILE):
        start, end = _covered_day_range(state_file)
        if start is None:
            continue
        d = start
        while d <= end:
            covered.add(d.isoformat())
            d += timedelta(days=1)
    return covered


def compute_transaction_counts():
    """
    Summiert tx_count je Block (aus miner_rewards.jsonl) für die letzten 7/30/365
    Tage. Ein Tag zählt nur dann als "abgedeckt", wenn er (a) im lückenlos
    gescannten Bereich beider Backfill-Läufe liegt UND (b) JEDER an diesem Tag
    gesehene Block ein tx_count-Feld trägt.

    (b) ist nötig, weil tx_count erst nachträglich ergänzt wurde: ältere, bereits
    abgeschlossene Chunks haben Einträge ohne dieses Feld und werden nicht erneut
    gescannt. Ohne diese Prüfung würde ein historischer, block-vollständig
    gescannter aber tx_count-loser Tag fälschlich als "abgedeckt, 0 Tx" gezählt
    statt als "noch keine Tx-Daten" — Untererfassung, die als Fakt aussieht, ist
    schlimmer als ein ehrliches Coverage-Loch.
    """
    block_scanned_days = _covered_days_set()

    by_day_count = {}
    day_has_null = set()
    seen_blocks = set()
    if os.path.exists(MINER_REWARDS_FOR_TX_COUNTS):
        with open(MINER_REWARDS_FOR_TX_COUNTS) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                block = entry.get("block")
                ts = entry.get("time")
                if block is None or ts is None or block in seen_blocks:
                    continue
                seen_blocks.add(block)
                day = datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
                tx_count = entry.get("tx_count")
                if tx_count is None:
                    day_has_null.add(day)
                    continue
                by_day_count[day] = by_day_count.get(day, 0) + tx_count

    tx_count_covered_days = {
        d for d in block_scanned_days
        if d in by_day_count and d not in day_has_null
    }

    today = datetime.now(timezone.utc).date()
    windows = {"7_days": 7, "30_days": 30, "365_days": 365}
    result = {}
    for key, n_days in windows.items():
        window_days = [(today - timedelta(days=i)).isoformat() for i in range(n_days)]
        covered_in_window = [d for d in window_days if d in tx_count_covered_days]
        count = sum(by_day_count.get(d, 0) for d in covered_in_window)
        result[key] = {
            "count": count,
            "coverage_pct": round(100 * len(covered_in_window) / n_days, 1),
            "days_covered": len(covered_in_window),
            "days_total": n_days,
        }
    return result


def scan_large_transactions(session, exchange_candidates):
    large_txs = []
    print(f"Suche nach großen Netto-Transfers (>= {THRESHOLD_PEP:,.0f} PEP)...")
    limiter = RateLimiter(MIN_REQUEST_INTERVAL)
    try:
        r = session.get(f"{BASE_URL}/getblockcount", timeout=10)
        if r.status_code != 200:
            print("Fehler beim Abrufen der Blockhöhe.")
            return []

        current_height = int(r.text)
        last_scanned = get_last_scanned_block(current_height)

        start_height = last_scanned + 1

        if start_height > current_height:
            print("Keine neuen Blöcke zum Scannen.")
            return []

        # Deckelt NUR das Ende des Fensters (nie den Anfang), damit kein Block
        # stillschweigend übersprungen wird, falls last_scanned weit zurückliegt.
        end_height = min(current_height, start_height + MAX_BLOCKS_PER_RUN - 1)
        if end_height < current_height:
            print(f"WARNUNG: Scan-Fenster auf {MAX_BLOCKS_PER_RUN} Blöcke gedeckelt — "
                  f"{current_height - end_height} Blöcke bleiben für den nächsten Lauf offen (kein Datenverlust, nur verzögert).")

        print(f"Scanne Blöcke von {start_height} bis {end_height}...")

        miner_f = open(MINER_REWARDS_FILE, "a")

        for height in range(start_height, end_height + 1):
            if height % 200 == 0:
                print(f"Scanne Block {height}/{end_height}...")

            try:
                block_meta, txs = fetch_block(session, limiter, height)
            except Exception as e:
                print(f"Block {height}: übersprungen ({e})")
                continue

            for t in extract_net_transfers(block_meta, txs, THRESHOLD_PEP):
                t["from_is_candidate_exchange"] = any(
                    f["address"] in exchange_candidates for f in t["from"]
                )
                t["to_is_candidate_exchange"] = any(
                    o["address"] in exchange_candidates for o in t["to"] if o["address"]
                )
                large_txs.append(t)

            reward = extract_coinbase_reward(block_meta, txs)
            if reward:
                miner_f.write(json.dumps(reward) + "\n")

            # Zustand speichern
            set_last_scanned_block(height)

        miner_f.close()

    except Exception as e:
        print(f"Fehler beim Block-Scan: {e}")

    return large_txs

def generate_snapshot():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    session = requests.Session()

    current_whales = []
    known_whales = load_known_whales()
    exchange_candidates = load_exchange_candidates()
    address_counts = {
        "100k": 0, "500k": 0, "1M": 0, "2M": 0, "5M": 0, "10M": 0, "20M": 0,
        "50M": 0, "100M": 0, "500M": 0, "1B": 0, "2B": 0, "3B": 0
    }

    status = "ok"

    print("Aktualisiere Preis-/Volumenhistorie (CoinGecko)...")
    update_price_history()

    print("Aktualisiere OHLC-Kerzen (CoinGecko)...")
    update_ohlc()

    print("Aktualisiere Netzwerk-Stats (Hashrate/Difficulty)...")
    update_network_stats(session)

    print("Hole Rich List über die echte API...")

    page = 1
    limit = 1000
    keep_fetching = True

    while keep_fetching:
        url = f"{BASE_URL}/v3/addresses?page={page}&limit={limit}"

        try:
            response = session.get(url, timeout=30)
            if response.status_code != 200:
                print(f"Fehler bei API: Status {response.status_code}")
                if page == 1: status = "fehlgeschlagen"
                else: status = "teilweise"
                break

            data = response.json()
            addresses = data.get("addresses", [])

            if not addresses:
                break

            for item in addresses:
                address = item.get("address")
                raw_balance_str = item.get("balance", "0")
                balance = float(raw_balance_str) / 100000000.0

                if balance < 100_000:
                    keep_fetching = False
                    break

                if balance >= 100_000: address_counts["100k"] += 1
                if balance >= 500_000: address_counts["500k"] += 1
                if balance >= 1_000_000: address_counts["1M"] += 1
                if balance >= 2_000_000: address_counts["2M"] += 1
                if balance >= 5_000_000: address_counts["5M"] += 1
                if balance >= 10_000_000: address_counts["10M"] += 1
                if balance >= 20_000_000: address_counts["20M"] += 1
                if balance >= 50_000_000: address_counts["50M"] += 1
                if balance >= 100_000_000: address_counts["100M"] += 1
                if balance >= 500_000_000: address_counts["500M"] += 1
                if balance >= 1_000_000_000: address_counts["1B"] += 1
                if balance >= 2_000_000_000: address_counts["2B"] += 1
                if balance >= 3_000_000_000: address_counts["3B"] += 1

                if balance >= 10_000_000:
                    current_whales.append({"address": address, "balance": balance})
                    if address not in known_whales:
                        known_whales[address] = {"first_seen": datetime.now().isoformat()}

            page += 1
            time.sleep(1)

        except Exception as e:
            print(f"Fehler beim Abrufen der API: {e}")
            if page == 1: status = "fehlgeschlagen"
            else: status = "teilweise"
            break

    print(f"Extrahiert: {len(current_whales)} Wale und {address_counts['100k']} Adressen über 100k PEP.")

    # 2. Schritt: Große Netto-Transfers scannen
    large_txs = scan_large_transactions(session, exchange_candidates)
    print(f"{len(large_txs)} große Netto-Transfers (>= {THRESHOLD_PEP:,.0f} PEP) im überprüften Zeitraum gefunden.")

    save_known_whales(known_whales)

    snapshot = {
        "timestamp": datetime.now().isoformat(),
        "status": status,
        "metrics": {
            "address_counts": address_counts
        },
        "whales": current_whales,
        "large_transfers": large_txs,
        "transaction_counts": compute_transaction_counts()
    }

    date_str = datetime.now().strftime("%Y-%m-%d")
    filepath = os.path.join(DATA_DIR, f"{date_str}.json")

    with open(filepath, "w") as f:
        json.dump(snapshot, f, indent=4)

    print(f"Snapshot gespeichert unter: {filepath}")

if __name__ == "__main__":
    print("Starte On-Chain Monitor Run...")
    generate_snapshot()
