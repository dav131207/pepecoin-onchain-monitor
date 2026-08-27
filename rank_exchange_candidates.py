"""
Rankt bekannte Wal-Adressen (known_whales.json) nach Transaktionsaktivität, um
Kandidaten für Exchange-Hot-Wallets zu identifizieren. Keine Namen echter
Börsen — es gibt keine öffentliche Zuordnungsliste für Pepecoin-Adressen, daher
nur verhaltensbasierte Einordnung (hohe tx_count / viele Ein- und Auszahlungen).

Zwei rein lokale Anreicherungsschritte (keine zusätzlichen API-Calls, nutzen nur
bereits gesammelte Dateien):

1. Grad-Zentralität aus large_transfers/*.jsonl: Anzahl unterschiedlicher
   Gegenparteien je Adresse — aber NUR innerhalb des ≥1M-PEP-Transfer-Graphen,
   das ist eine Untergrenze, kein echter Adress-Grad (viele Exchange-Einzahlungen
   liegen darunter). Wächst mit fortschreitendem Backfill.
2. Miner-Erkennung aus miner_rewards/*.jsonl (Coinbase-Auszahlungen): Adressen, die
   wiederholt Block-Belohnungen erhalten, sind Mining-Pool-/Solo-Miner-Wallets,
   keine Exchange-Wallets — auch wenn sie (viele Auszahlungen an Miner) hohe
   Tx-Aktivität zeigen und sonst als "Exchange-Hot-Wallet" fehlklassifiziert
   würden. Ergebnis reklassifiziert betroffene Kandidaten UND wird zusätzlich
   als eigenständiges miner_wallets.json geschrieben (auch für Adressen, die
   keine bekannten Wale sind).
"""
import json

import requests

from pep_client import RateLimiter, fetch_address_stats, LARGE_TRANSFERS_DIR, MINER_REWARDS_DIR, read_jsonl_dir

KNOWN_WHALES_FILE = "known_whales.json"
OUTPUT_FILE = "exchange_candidates.json"
MINER_OUTPUT_FILE = "miner_wallets.json"
MIN_REQUEST_INTERVAL = 0.25  # ~4 req/s, konservativ gegenüber dem SSR-Endpoint
MIN_BLOCKS_FOR_MINER_TAG = 3  # so oft mind. als Coinbase-Empfänger gesehen, um als Miner zu gelten


def classify(stats):
    tx_count = stats["tx_count"]
    if tx_count >= 1000:
        return "sehr wahrscheinlich Exchange/Dienstleister-Wallet (sehr hohe Tx-Aktivität)"
    if tx_count >= 200:
        return "möglicherweise Exchange-Hot-Wallet oder Pool (hohe Tx-Aktivität)"
    if tx_count >= 50:
        return "erhöhte Aktivität, evtl. Dienstleister oder aktiver Trader"
    return "normale Wal-Aktivität (eher Hold-Wallet)"


def load_miner_registry():
    """address -> {blocks, total_reward, first_block, last_block, sampled_only}"""
    registry = {}
    for r in read_jsonl_dir(MINER_REWARDS_DIR):
        for p in r.get("payouts", []):
            addr = p.get("address")
            if not addr:
                continue
            entry = registry.setdefault(addr, {
                "blocks": 0, "total_reward": 0.0,
                "first_block": r["block"], "last_block": r["block"],
                "sampled_only": True,
            })
            entry["blocks"] += 1
            entry["total_reward"] += p["amount"]
            entry["first_block"] = min(entry["first_block"], r["block"])
            entry["last_block"] = max(entry["last_block"], r["block"])
            if not r.get("sampled"):
                entry["sampled_only"] = False
    return registry


def compute_degree_centrality(addresses_of_interest):
    """Grad-Zentralität NUR im ≥1M-PEP-Transfer-Graphen (Untergrenze, siehe Docstring oben)."""
    incoming = {a: set() for a in addresses_of_interest}
    outgoing = {a: set() for a in addresses_of_interest}
    involved_count = {a: 0 for a in addresses_of_interest}
    for t in read_jsonl_dir(LARGE_TRANSFERS_DIR):
        from_addrs = {x["address"] for x in t.get("from", []) if x.get("address")}
        to_addrs = {x["address"] for x in t.get("to", []) if x.get("address")}
        for a in from_addrs:
            if a in addresses_of_interest:
                outgoing[a].update(to_addrs)
                involved_count[a] += 1
        for a in to_addrs:
            if a in addresses_of_interest:
                incoming[a].update(from_addrs)
                involved_count[a] += 1
    return incoming, outgoing, involved_count


def write_miner_wallets_file(miner_registry):
    total_blocks_seen = sum(1 for _ in read_jsonl_dir(MINER_REWARDS_DIR))
    miners = []
    for addr, entry in miner_registry.items():
        if entry["blocks"] < MIN_BLOCKS_FOR_MINER_TAG:
            continue
        miners.append({
            "address": addr,
            "blocks_mined": entry["blocks"],
            "total_reward_pep": round(entry["total_reward"], 2),
            "share_pct_of_seen_blocks": round(entry["blocks"] / total_blocks_seen * 100, 2) if total_blocks_seen else None,
            "first_block": entry["first_block"],
            "last_block": entry["last_block"],
            "includes_sampled_blocks": entry["sampled_only"] or entry["blocks"] > 0,
        })
    miners.sort(key=lambda m: m["blocks_mined"], reverse=True)
    with open(MINER_OUTPUT_FILE, "w") as f:
        json.dump({
            "note": "Verhaltensbasiert (wiederholte Coinbase-Auszahlung) — keine bestätigten Pool-Namen. "
                    "'share_pct_of_seen_blocks' bezieht sich nur auf die bislang gescannten/gesampleten Blöcke, "
                    "nicht auf die gesamte Chain-Historie.",
            "total_blocks_seen": total_blocks_seen,
            "min_blocks_for_tag": MIN_BLOCKS_FOR_MINER_TAG,
            "miners": miners,
        }, f, indent=2)
    return miners


def main():
    with open(KNOWN_WHALES_FILE) as f:
        whales = json.load(f)

    addresses = list(whales.keys())
    print(f"Prüfe {len(addresses)} bekannte Wal-Adressen auf Aktivitätsmuster...")

    session = requests.Session()
    limiter = RateLimiter(MIN_REQUEST_INTERVAL)

    results = []
    errors = 0
    for i, addr in enumerate(addresses, 1):
        try:
            stats = fetch_address_stats(session, limiter, addr)
        except Exception as e:
            errors += 1
            continue
        stats["classification"] = classify(stats)
        results.append(stats)
        if i % 100 == 0:
            print(f"{i}/{len(addresses)} geprüft ({errors} Fehler bisher)...")

    print("\nBerechne Grad-Zentralität aus bereits gesammelten Groß-Transfers (lokal, keine API-Calls)...")
    addr_set = {r["address"] for r in results}
    incoming, outgoing, involved = compute_degree_centrality(addr_set)

    print("Lade Miner-Registry aus Coinbase-Auszahlungen (lokal, keine API-Calls)...")
    miner_registry = load_miner_registry()
    miners_written = write_miner_wallets_file(miner_registry)
    print(f"{len(miners_written)} Adressen als Miner erkannt (>= {MIN_BLOCKS_FOR_MINER_TAG} Blöcke), "
          f"gespeichert unter {MINER_OUTPUT_FILE}.")

    reclassified = 0
    for r in results:
        addr = r["address"]
        r["distinct_senders_ge1m"] = len(incoming.get(addr, set()))
        r["distinct_receivers_ge1m"] = len(outgoing.get(addr, set()))
        r["large_transfer_count"] = involved.get(addr, 0)

        miner = miner_registry.get(addr)
        if miner and miner["blocks"] >= MIN_BLOCKS_FOR_MINER_TAG:
            r["is_miner"] = True
            r["miner_blocks"] = miner["blocks"]
            r["classification"] = f"Mining-Pool-/Miner-Auszahlungsadresse ({miner['blocks']} Blöcke gesehen, keine Exchange)"
            reclassified += 1
        else:
            r["is_miner"] = False

    results.sort(key=lambda s: s["tx_count"], reverse=True)

    with open(OUTPUT_FILE, "w") as f:
        json.dump({
            "checked": len(addresses),
            "errors": errors,
            "candidates": results,
        }, f, indent=2)

    print(f"\nFertig. {len(results)} Adressen ausgewertet, {errors} Fehler, "
          f"{reclassified} davon als Miner statt Exchange reklassifiziert.")
    print(f"Ergebnis gespeichert unter: {OUTPUT_FILE}")
    print("\nTop 15 nach Tx-Anzahl:")
    for s in results[:15]:
        tag = " [MINER]" if s["is_miner"] else ""
        print(f"  {s['address']}  tx_count={s['tx_count']:<6} balance={s['balance']:.0f} PEP{tag}  — {s['classification']}")


if __name__ == "__main__":
    main()
