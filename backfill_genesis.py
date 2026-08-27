"""
Zweiter, unabhängiger Backfill-Lauf: füllt die Historie vom Launch-Block (Block 1,
30.1.2024) bis zum Startpunkt des laufenden 90-Tage-Fensters (backfill_history.py).

Eigene Zustandsdatei (GENESIS_STATE_FILE), eigenes fixes Zielende (nicht der
aktuelle Blockstand) — läuft komplett unabhängig neben backfill_history.py,
ohne dessen Fortschritt zu berühren. Beide schreiben in dieselben monatlich
aufgeteilten Verzeichnisse (large_transfers/, miner_rewards/ — siehe pep_client.py,
append_jsonl_by_month), das Dashboard dedupliziert Transfers ohnehin per txid.

Umfang: ~1,17 Mio. Blöcke (fast die gesamte Chain) — bei aktuellem Tempo
mehrere Tage durchgehender Scan. Läuft im Hintergrund weiter, auch über
mehrere Sitzungen hinweg (Fortschritt ist jederzeit fortsetzbar).
"""
import json
import os

from backfill_history import run_backfill, STATE_FILE as HEAD_STATE_FILE
from pep_client import LARGE_TRANSFERS_DIR, MINER_REWARDS_DIR

GENESIS_STATE_FILE = "backfill_genesis_state.json"
OUTPUT_DIR = LARGE_TRANSFERS_DIR
MINER_OUTPUT_DIR = MINER_REWARDS_DIR
INCOMPLETE_LOG = "blocks_incomplete_genesis.txt"

GENESIS_HEIGHT = 1  # Block 1 = 30.1.2024, siehe README


def determine_initial_range_genesis(session, current_height):
    """
    Zielbereich: Block 1 bis (Startpunkt des laufenden 90-Tage-Fensters - 1).
    Wird nur beim allerersten Start dieses Skripts gelesen — falls das
    90-Tage-Fenster inzwischen weitergewandert ist, bleibt das hier einmal
    festgelegte Ziel unverändert (auto_extend_end=False), damit kein
    Überlapp/Chaos mit dem anderen Lauf entsteht.
    """
    if not os.path.exists(HEAD_STATE_FILE):
        raise RuntimeError(
            f"{HEAD_STATE_FILE} nicht gefunden — backfill_history.py muss mindestens einmal "
            "gelaufen sein, damit der Grenzpunkt zwischen beiden Läufen feststeht."
        )
    with open(HEAD_STATE_FILE) as f:
        head_state = json.load(f)
    boundary = head_state["target_start_height"] - 1
    return GENESIS_HEIGHT, boundary


def main():
    run_backfill(
        state_file=GENESIS_STATE_FILE,
        output_dir=OUTPUT_DIR,
        miner_dir=MINER_OUTPUT_DIR,
        incomplete_log=INCOMPLETE_LOG,
        determine_initial_range=determine_initial_range_genesis,
        auto_extend_end=False,
        label="Genesis-Backfill",
        # Dedizierter Endpoint, laut Nutzer für höheres Tempo freigegeben.
        # Burst-getestet am 19.8.: 20 gleichzeitige Requests ~28 Blöcke/s, 0 Fehler.
        # Läuft parallel zum 90-Tage-Kopf-Fenster (backfill_history.py) — bekommt hier
        # den größeren Anteil des getesteten Budgets, da hier der Großteil der Arbeit liegt.
        min_request_interval=0.02,
        workers=16,
    )


if __name__ == "__main__":
    main()
