"""
Adress-Clustering per Common-Input-Ownership-Heuristik: Alle Eingabe-Adressen
einer einzelnen Transaktion müssen von deren jeweiligen Inhabern gemeinsam
signiert worden sein, gelten also vermutlich als dieselbe Entität (Standard-
Heuristik der On-Chain-Analyse, siehe z.B. Meiklejohn et al. 2013). Reine
Nährungs­heuristik: CoinJoin-artige Transaktionen (nicht bekannt, ob Pepecoin-
Wallets sowas nutzen) würden fälschlich Adressen verschiedener Personen
zusammenwürfeln — nicht weiter validiert.

Rein lokale Auswertung von all_transactions/*.jsonl (aus backfill_alltx.py),
keine API-Calls. Union-Find über alle Adressen, die in mind. einer Tx gemeinsam
als Input auftauchen.
"""
import glob
import json
import os
from datetime import datetime, timezone

from pep_client import ALL_TRANSACTIONS_DIR, read_jsonl_dir

OUTPUT_FILE = "address_clusters.json"
MIN_CLUSTER_SIZE = 2  # nur tatsächlich zusammengeführte Cluster sind interessant


class UnionFind:
    def __init__(self):
        self.parent = {}

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


def main():
    if not os.path.exists(ALL_TRANSACTIONS_DIR):
        print(f"{ALL_TRANSACTIONS_DIR}/ existiert noch nicht (backfill_alltx.py noch nicht gelaufen) — nichts zu tun.")
        return

    uf = UnionFind()
    tx_count = 0
    for tx in read_jsonl_dir(ALL_TRANSACTIONS_DIR):
        tx_count += 1
        addrs = {i["address"] for i in tx.get("inputs", []) if i.get("address")}
        if len(addrs) < 2:
            continue
        addrs = list(addrs)
        first = addrs[0]
        for a in addrs[1:]:
            uf.union(first, a)

    clusters = {}
    for addr in uf.parent:
        root = uf.find(addr)
        clusters.setdefault(root, []).append(addr)

    real_clusters = [members for members in clusters.values() if len(members) >= MIN_CLUSTER_SIZE]
    real_clusters.sort(key=len, reverse=True)

    index_path = os.path.join(ALL_TRANSACTIONS_DIR, "index.json")
    covered_days = []
    if os.path.exists(index_path):
        with open(index_path) as f:
            covered_days = json.load(f)

    result = {
        "updated": datetime.now(timezone.utc).isoformat(),
        "method": "common-input-ownership (Union-Find über Tx-Eingabe-Adressgruppen)",
        "caveat": "Nährungsheuristik, nicht validiert. Basiert nur auf bislang von backfill_alltx.py "
                   "gescannten Tagen (siehe covered_days) — mit fortschreitendem Backfill können sich "
                   "Cluster noch vergrößern/verändern.",
        "transactions_scanned": tx_count,
        "covered_days": covered_days,
        "addresses_in_clusters": sum(len(m) for m in real_clusters),
        "cluster_count": len(real_clusters),
        "clusters": [
            {"cluster_id": i, "size": len(members), "addresses": sorted(members)}
            for i, members in enumerate(real_clusters)
        ],
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(result, f, indent=2)
    print(f"Adress-Clustering: {tx_count} Tx ausgewertet, {len(real_clusters)} Cluster "
          f"({result['addresses_in_clusters']} Adressen) gefunden. Gespeichert unter {OUTPUT_FILE}.")


if __name__ == "__main__":
    main()
