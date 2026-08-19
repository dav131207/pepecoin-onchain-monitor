# Pepecoin On-Chain Monitor

Datenspeicher für den wöchentlichen Pepecoin (PEP) On-Chain-Report-Agenten.

## Struktur

- `snapshots/YYYY-MM-DD.json` — ein JSON-Snapshot pro Lauf (`monitor.py`) mit:
  - Adressanzahl je Balance-Schwelle (100k, 500k, 1M, 2M, 5M, 10M, 20M PEP, ...)
  - Liste aktueller Wal-Adressen (>= 10M PEP) mit Balance
  - Große Netto-Transfers seit dem letzten Lauf (>= 1M PEP, Wechselgeld herausgerechnet) mit `from`/`to`/`net_amount`/`txid`, plus Flags `from_is_candidate_exchange` / `to_is_candidate_exchange`
  - Zeitstempel des Laufs, Datenquelle-Status (ok / teilweise / fehlgeschlagen)
- `known_whales.json` — laufend gepflegte Liste aller jemals gesehenen Wal-Adressen (>= 10M PEP), inkl. `first_seen`-Datum, um "neue Wale" pro Lauf zu erkennen.
- `exchange_candidates.json` — Ranking der bekannten Wal-Adressen nach Tx-Aktivität (`rank_exchange_candidates.py`), verhaltensbasierte Einordnung als vermutliche Exchange-/Hub-Wallets. Keine bestätigten Börsen-Namen — dafür gibt es keine öffentliche Adressliste für Pepecoin.
- `large_transfers_3m.jsonl` — Ergebnis des einmaligen 90-Tage-Backfills (`backfill_history.py`), ein JSON-Objekt pro Zeile, gleiches Format wie `large_transfers` im Snapshot.
- `price_history.json` — täglicher USD-Preis/Volumen/Marketcap von CoinGecko (Coin-ID `pepecoin-network`), gepflegt von `price_client.py`, bei jedem `monitor.py`-Lauf gemerged.
- `backfill_state.json` (git-ignoriert) — Backfill-Fortschritt inkl. `contiguous_covered_start_day`/`contiguous_covered_end_day`: das vom Backfill bereits **lückenlos** gescannte Kalendertag-Fenster (Grundlage dafür, welche Tage im Dashboard als "echte 0" statt "fehlend" in Korrelationen eingehen dürfen).
- `dashboard.html` — Analyse-Dashboard (Coinglass-artig: KPI-Kacheln, Preis-/Volumen-Charts, Adressverteilung, Groß-Transfer-Volumen, Exchange-Netflow, Korrelationsmatrix, Lag-Analyse, Aktivitäts-Heatmap, Shill/Noise-Postentwürfe). Liest alle obigen Dateien per `fetch()` (muss über einen lokalen Webserver geöffnet werden, nicht per `file://`). Siehe Abschnitte "Darstellung & Robustheit" und "Shill/Noise" unten.

## Datenquelle

Primär: https://avivppblocks.realmasterkush.com (Spiegel des Pepeblocks-Explorers ohne Cloudflare-Blockierung, vom Chef bereitgestellt). Zwei Zugriffswege werden genutzt:

- `/api/*` (dokumentierte JSON-RPC-artige Routen: `getblockcount`, `getblockhash`, `getblock`, `getrawtransaction`, `v3/addresses`) — stabil, für die Rich-List und einfache Lookups.
- `/block/<height>/__data.json` und `/address/<addr>/__data.json` (interne SvelteKit-SSR-Datenrouten, nicht offiziell dokumentiert) — liefern pro Block/Adresse bereits aufgelöste Esplora-artige Daten **inklusive Absenderadressen** (`vin[].prevout`), wodurch keine zusätzlichen Requests zur Auflösung der Eingaben nötig sind. Werte dort sind 1e8-skaliert (wie Satoshi); `pep_client.py` rechnet das bereits in PEP um. Format wurde gegen den dokumentierten `/api`-Pfad auf 20 Stichproben-Blöcken cross-validiert (0 Abweichungen, siehe Git-Historie). Da diese Route SSR-gerendert (teurer für den Server) und undokumentiert ist, läuft aller Zugriff über einen gemeinsamen, konservativen Rate-Limiter (`pep_client.RateLimiter`) mit Backoff bei 429/5xx.

Zugriff ist dennoch Best-Effort und kann fehlschlagen — in diesem Fall im Snapshot und Dashboard transparent vermerken, keine Daten erfinden.

## Skripte

- `monitor.py` — der wöchentliche Hauptlauf: Rich-List-Snapshot + inkrementeller Scan neuer Blöcke seit `last_scanned_block.txt` auf Netto-Transfers >= 1M PEP. Deckelt ein einzelnes Fenster auf max. 20.000 Blöcke (Sicherheitsventil, kein Datenverlust — der Rest folgt beim nächsten Lauf).
- `pep_client.py` — gemeinsamer Client (Devalue-Parser für die SvelteKit-Routen, Rate-Limiter, Block-/Adress-Fetcher, Netto-Transfer-Filter). Wird von `monitor.py`, `backfill_history.py` und `rank_exchange_candidates.py` genutzt.
- `backfill_history.py` — einmaliger, fortsetzbarer Backfill der letzten ~90 Tage. Fortschritt in 500-Block-Chunks (`backfill_state.json`, git-ignoriert), Ergebnisse werden laufend an `large_transfers_3m.jsonl` angehängt. Bei Abbruch: einfach erneut starten, es wird nur der fehlende Rest geholt.
- `rank_exchange_candidates.py` — prüft alle Adressen aus `known_whales.json` auf Tx-Aktivität und schreibt ein Ranking nach `exchange_candidates.json`.
- `price_client.py` — holt USD-Preis-/Volumenhistorie von CoinGecko (öffentliche API, kein Key), gebucketed auf UTC-Kalendertage, merged in `price_history.json`. Wird von `monitor.py` bei jedem Lauf aufgerufen (best-effort, blockiert den Snapshot nicht bei Fehlschlag).

## Korrelationen & Heatmaps (Dashboard)

Die Korrelationsmatrix und Lag-Analyse im Dashboard zeigen erst ab **n ≥ 30** gemeinsamen Tagen einen Zahlenwert — darunter nur die Stichprobengröße "n" auf neutralem Grund, um keine Scheinsicherheit vorzutäuschen (aktuell, solange der 90-Tage-Backfill noch läuft, greift das fast überall). On-Chain-Metriken zählen dabei nur innerhalb des von `backfill_history.py` bestätigten lückenlosen Zeitfensters (`contiguous_covered_*_day` in `backfill_state.json`) — nicht einfach zwischen ältestem und neuestem gefundenem Transfer, da sonst die noch ungescannte Lücke zwischen Backfill-Fenster und heutigem Live-Rand fälschlich als "abgedeckt (= 0 Transfers)" gewertet würde. Preis- und Handelsvolumen-Reihen gehen als Tag-über-Tag-Änderung ein, nicht als Niveau (sonst Scheinkorrelation durch gemeinsamen Trend).

## Darstellung & Robustheit (Dashboard)

Das Dashboard ist responsiv bis ~360px Breite: Breakpoints bei 900px und 560px, Tabellen und Charts scrollen **innerhalb ihrer Karte** (`.table-scroll`, `overflow-x` auf `.chart-wrap`), damit die Seite selbst nie horizontal wegläuft und SVG-Beschriftungen auf dem Handy nicht auf unlesbare ~4px zusammengestaucht werden. Der Tooltip wird an den Viewport geklemmt.

Drei Fehlerquellen, die vorher nach jedem Skript-Lauf die komplette Seite leer lassen konnten, sind abgestellt:

- **Fehlender Tages-Snapshot**: Lief `monitor.py` heute (noch) nicht durch, wird bis zu 14 Tage zurück der jüngste vorhandene Snapshot geladen und als veraltet markiert. Gibt es gar keinen, rendern Preis-, Difficulty- und Transfer-Karten trotzdem.
- **Ein kaputter Renderer riss alle folgenden mit**: `render()` ruft jede Karte über `safeRender()` auf — ein Fehler bleibt in seiner Karte sichtbar stehen, der Rest rendert normal weiter. Gilt auch beim Umschalten des Zeitraum-Filters.
- **`Math.min(...array)` auf der Transfer-Liste**: Ab ca. 125.000 Einträgen wirft der Spread einen `RangeError` (Stack), womit das gesamte Laden abbrach — der laufende Genesis-Backfill überschreitet diese Grenze zwangsläufig. Ersetzt durch schleifenbasierte `arrayMin`/`arrayMax` (verifiziert mit 224k Transfers).

## Shill/Noise

Die Karte "Shill/Noise" erzeugt fertige, kopierbare Post-Entwürfe für X/Twitter — Preis-Momentum, Holder-Basis, Groß-Transfer-Aktivität, PoW/Mining, Fluss-Verhalten. Regeln, damit die Reichweite nicht auf Kosten der Datenglaubwürdigkeit geht:

- Jede Zahl wird zur Laufzeit aus den geladenen Daten berechnet; fehlt die Grundlage, entfällt der Post statt eine Zahl zu raten.
- Keine Kursziele, keine Kaufaufrufe, keine Allzeithoch-Behauptungen (der aktuelle Kurs liegt unter dem Mai-Niveau, "ATH" wäre schlicht falsch), keine Börsennamen — Hub-Wallets bleiben verhaltensbasiert beschrieben.
- Hashtags zentral als `HASHTAGS` definiert (`#Pepecoin`, `#PEP` fest + je Post genau ein Kontext-Tag aus `#Memecoin`/`#OnChain`/`#PoW`).
- Kein Auto-Posting: Das Dashboard kopiert nur in die Zwischenablage, gepostet wird von Hand.

## Automatisierung

Aktuell **manuell**: `monitor.py` wird bei Bedarf von Hand gestartet (z.B. wöchentlich). Eine automatische Zeitsteuerung (lokaler cron-Job oder Cloud-Schedule) wurde bewusst noch nicht eingerichtet — das kann später nachgezogen werden, sobald klar ist, auf welchem Weg (lokal vs. Cloud) automatisiert werden soll.
