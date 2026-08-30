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
- `large_transfers/YYYY-MM.jsonl` / `miner_rewards/YYYY-MM.jsonl` — monatlich aufgeteilte Ausgabe von `backfill_history.py`/`backfill_genesis.py`/`monitor.py` (ein JSON-Objekt pro Zeile, gleiches Format wie `large_transfers` im Snapshot bzw. Coinbase-Reward inkl. `tx_count`). Monatlich aufgeteilt statt einer einzigen Datei, weil `miner_rewards.jsonl` am 26.8.2026 GitHubs 100-MB-Hard-Limit pro Datei riss (bei ~119 MB) — seither wurde jeder Push mit "Large files detected" hart abgelehnt und mehrere 5h-Backfill-Läufe gingen komplett verloren, bevor die Ursache gefunden wurde. Lesend über `pep_client.read_jsonl_dir(dir)` (iteriert alle Monatsdateien sortiert), schreibend über `pep_client.append_jsonl_by_month(records, dir)`.
- `price_history.json` — täglicher USD-Preis/Volumen/Marketcap von CoinGecko (Coin-ID `pepecoin-network`), gepflegt von `price_client.py`, bei jedem `monitor.py`-Lauf gemerged.
- `price_ohlc.json` — OHLC-Kerzen von CoinGecko (`price_client.py`, `update_ohlc()`), bei jedem Lauf komplett neu geschrieben (kein Merge nötig, CoinGecko liefert das volle Fenster). Bei einem Anforderungsfenster >30 Tage liefert CoinGeckos kostenloser Tarif serverseitig **4-Tage-Kerzen**, kein echtes Tages-OHLC — steht explizit als `candle_days` in der Datei, um Verwechslung zu vermeiden.
- `holder_distribution.json` — Konzentrationskennzahlen (Gini-Koeffizient, Top-1%/Top-10%-Anteil) aus der kompletten Rich List, ein Eintrag pro Kalendertag (`holder_distribution.py`). Die ~294k einzelnen Adress-/Balance-Paare selbst werden nicht gespeichert, nur die abgeleiteten Kennzahlen.
- `whale_dormancy.json` — pro bekannter Wal-Adresse: Zeitpunkt der letzten Auszahlung >= 1M PEP (aus `large_transfers/*.jsonl`) und Tage seit dieser Auszahlung (`whale_dormancy.py`, rein lokal, keine API-Calls). "Keine Auszahlung gefunden" heißt nur "nicht im bislang gescannten Zeitraum", nicht zwingend "seit Genesis inaktiv" — der abgedeckte Zeitraum steht mit in der Datei.
- `backfill_state.json` / `backfill_genesis_state.json` / `backfill_alltx_state.json` / `last_scanned_block.txt` — bewusst **nicht** git-ignoriert: GitHub Actions checkt bei jedem Lauf frisch aus, diese Fortschrittsdateien müssen im Repo liegen, damit die Skripte exakt fortsetzen statt bei jedem Lauf neu zu starten. Enthalten u.a. `contiguous_covered_start_day`/`contiguous_covered_end_day`: das vom jeweiligen Backfill bereits **lückenlos** gescannte Kalendertag-Fenster (Grundlage dafür, welche Tage als "echte 0" statt "fehlend" in Korrelationen/Transaktionszählungen eingehen dürfen). `backfill_genesis_state.json` ist seit dem 30.08.2026 bei 100% (Block 1 bis zum Start des 90-Tage-Kopf-Fensters lückenlos gescannt).
- `all_transactions/YYYY-MM-DD.jsonl` — **jede** Transaktion (nicht nur Großtransfers ≥1M PEP), von `backfill_alltx.py` (dritter, unabhängiger Backfill-Pass, Block 1 bis aktueller Stand). Pro Tag statt pro Monat gebucketet (dichter als `large_transfers`/`miner_rewards` — siehe Warnung zum 100-MB-Limit oben, hier wäre selbst ein Monat zu groß). Schema pro Transaktion: `txid`, `block`, `time`, `is_coinbase`, `fee`, `inputs[]` (`prev_txid`/`prev_vout`/`address`/`amount` — **gruppiert pro Tx**, nicht blockweise aggregiert, wichtig für Adress-Clustering), `outputs[]` (`vout`/`address`/`amount`). Coinbase-Ausgaben sind **enthalten** (mit `inputs: []`), sonst hätte jede noch nie bewegte Mining-Belohnung beim späteren Spend keinen Ursprungs-Eintrag für den Coin-Age-Join. Grundlage für `address_clustering.py` und `coin_age_analysis.py`.
- `mempool_history/YYYY-MM-DD.jsonl` — stündliche Mempool-Schnappschüsse (`mempool_snapshot.py`): Anzahl ausstehender Tx, Gesamtgebühr, Gebühr/Byte-Perzentile (p10/p50/p90/max), plus die Rohdaten je ausstehender Tx (bis `MAX_RAW_ENTRIES`, Schutz vor einem Congestion-Event). Mempool ist nicht rückwirkend erfassbar — diese Reihe wächst nur ab dem Zeitpunkt, an dem der Workflow läuft.
- `address_clusters.json` — Adress-Cluster per Common-Input-Ownership-Heuristik (`address_clustering.py`) aus `all_transactions/`. Näherung, nicht validiert — siehe Caveat in der Datei.
- `coin_days_destroyed.json` / `hodl_waves.json` — Coin-Age-Kennzahlen aus `all_transactions/` (`coin_age_analysis.py`): CDD (Betrag × Alter bei Spend, pro Tag) bzw. HODL-Waves (Supply-Anteil nach Haltedauer-Bucket, Momentaufnahme). Beide nur so vollständig wie der `all_transactions/`-Backfill selbst — `covered_days` in der Datei zeigt den tatsächlich abgedeckten Zeitraum, siehe Caveat in den Dateien.
- `dashboard.html` — Analyse-Dashboard (Coinglass-artig: KPI-Kacheln, Preis-/Volumen-Charts, Adressverteilung, Groß-Transfer-Volumen, Exchange-Netflow, Korrelationsmatrix, Lag-Analyse, Aktivitäts-Heatmap, Shill/Noise-Postentwürfe). Liest alle obigen Dateien per `fetch()` (muss über einen lokalen Webserver geöffnet werden, nicht per `file://`). Liest `large_transfers/`/`miner_rewards/` über je ein `index.json` (Liste der Monatsdateien, von `pep_client.append_jsonl_by_month`/`append_jsonl_by_day` gepflegt) statt eine feste Datei zu erwarten — Verzeichnislisting gibt es bei einem statischen Server nicht. Die neueren Dateien (`price_ohlc.json`, `holder_distribution.json`, `whale_dormancy.json`, `address_clusters.json`, `coin_days_destroyed.json`, `hodl_waves.json`) sind noch **nicht** in die UI eingebunden, nur die Rohdaten werden gesammelt. Siehe Abschnitte "Darstellung & Robustheit" und "Shill/Noise" unten.

## Datenquelle

Primär: https://avivppblocks.realmasterkush.com (Spiegel des Pepeblocks-Explorers ohne Cloudflare-Blockierung, vom Chef bereitgestellt). Zwei Zugriffswege werden genutzt:

- `/api/*` (dokumentierte JSON-RPC-artige Routen: `getblockcount`, `getblockhash`, `getblock`, `getrawtransaction`, `v3/addresses`) — stabil, für die Rich-List und einfache Lookups.
- `/block/<height>/__data.json` und `/address/<addr>/__data.json` (interne SvelteKit-SSR-Datenrouten, nicht offiziell dokumentiert) — liefern pro Block/Adresse bereits aufgelöste Esplora-artige Daten **inklusive Absenderadressen** (`vin[].prevout`), wodurch keine zusätzlichen Requests zur Auflösung der Eingaben nötig sind. Werte dort sind 1e8-skaliert (wie Satoshi); `pep_client.py` rechnet das bereits in PEP um. Format wurde gegen den dokumentierten `/api`-Pfad auf 20 Stichproben-Blöcken cross-validiert (0 Abweichungen, siehe Git-Historie). Da diese Route SSR-gerendert (teurer für den Server) und undokumentiert ist, läuft aller Zugriff über einen gemeinsamen, konservativen Rate-Limiter (`pep_client.RateLimiter`) mit Backoff bei 429/5xx.

Zugriff ist dennoch Best-Effort und kann fehlschlagen — in diesem Fall im Snapshot und Dashboard transparent vermerken, keine Daten erfinden.

## Skripte

- `monitor.py` — der wöchentliche Hauptlauf: Rich-List-Snapshot + inkrementeller Scan neuer Blöcke seit `last_scanned_block.txt` auf Netto-Transfers >= 1M PEP, plus Preis-/OHLC-Update und `transaction_counts` (7/30/365 Tage, siehe unten). Deckelt ein einzelnes Fenster auf max. 20.000 Blöcke (Sicherheitsventil, kein Datenverlust — der Rest folgt beim nächsten Lauf).
- `pep_client.py` — gemeinsamer Client (Devalue-Parser für die SvelteKit-Routen, Rate-Limiter, Block-/Adress-Fetcher, Netto-Transfer-Filter, Coinbase-Reward-Extraktion inkl. `tx_count` je Block). Wird von `monitor.py`, `backfill_history.py`, `backfill_genesis.py`, `sample_miner_history.py` und `rank_exchange_candidates.py` genutzt.
- `backfill_history.py` / `backfill_genesis.py` — fortsetzbarer Backfill (90-Tage-Kopf-Fenster bzw. komplette Historie seit Genesis-Block). Fortschritt in 500-Block-Chunks (`backfill_state.json` / `backfill_genesis_state.json`), Ergebnisse werden laufend an `large_transfers/YYYY-MM.jsonl` und `miner_rewards/YYYY-MM.jsonl` angehängt. Bei Abbruch: einfach erneut starten, es wird nur der fehlende Rest geholt.
- `rank_exchange_candidates.py` — prüft alle Adressen aus `known_whales.json` auf Tx-Aktivität und schreibt ein Ranking nach `exchange_candidates.json`.
- `price_client.py` — holt USD-Preis-/Volumenhistorie und OHLC-Kerzen von CoinGecko (öffentliche API, kein Key). Preis-Historie wird auf UTC-Kalendertage gebucketed und in `price_history.json` gemerged; OHLC wird komplett neu geschrieben in `price_ohlc.json`. Wird von `monitor.py` bei jedem Lauf aufgerufen (best-effort, blockiert den Snapshot nicht bei Fehlschlag).
- `holder_distribution.py` — holt die komplette Rich List und berechnet Gini-Koeffizient sowie Top-1%/Top-10%-Anteil, schreibt nach `holder_distribution.json`.
- `whale_dormancy.py` — rein lokale Auswertung von `known_whales.json` + `large_transfers/*.jsonl`: Tage seit der letzten Auszahlung >= 1M PEP je Wal-Adresse, schreibt nach `whale_dormancy.json`.
- `backfill_alltx.py` — dritter, unabhängiger Backfill-Pass (eigene `backfill_alltx_state.json`, Block 1 bis aktueller Stand, kein Genesis-/Kopf-Split wie bei den anderen beiden): erfasst JEDE Transaktion (nicht nur Großtransfers) nach `all_transactions/YYYY-MM-DD.jsonl`. Rührt die anderen beiden Backfill-Zustände nicht an — dieselben Blöcke werden also bewusst dreifach von der API geholt, statt den bestehenden Fortschritt zu riskieren.
- `mempool_snapshot.py` — ein Schnappschuss des aktuellen Mempools nach `mempool_history/`, siehe oben.
- `address_clustering.py` — Union-Find über `all_transactions/*.jsonl` (Common-Input-Ownership-Heuristik), schreibt `address_clusters.json`.
- `coin_age_analysis.py` — Coin-Age-Join über `all_transactions/*.jsonl` (Ursprungs-Ausgabe via `(prev_txid, prev_vout)` nachschlagen), schreibt `coin_days_destroyed.json` und `hodl_waves.json`. Hält aktuell alle je gesehenen Ausgaben in einem In-Memory-Dict — bei sehr großem Datensatz ggf. später auf eine echte Datenbank umsteigen.

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

**GitHub Actions** (`.github/workflows/`), nicht die Claude-Cloud-Routine — der Sandbox der Claude-Cloud-Routine blockiert per Netzwerk-Policy ausgehende Requests an die Datenquellen (bestätigt am 23.08.2026: alle vier versuchten Quellen inkl. CoinGecko wurden dort geblockt, während pip/GitHub normal funktionierten). GitHub-Actions-Runner haben uneingeschränkten Netzwerkzugriff und committen die Ergebnisse direkt zurück ins Repo:

- **`genesis-backfill.yml`** — alle 6h, 5h Zeitbudget pro Lauf, treibt `backfill_genesis.py` voran. Seit 30.08.2026 bei 100% — läuft der Cron trotzdem weiter, das Skript erkennt "bereits vollständig" und beendet sich dann in ~30s statt 5h.
- **`alltx-backfill.yml`** — 4×/Tag (versetzt zu genesis-backfill, um sich die Warteschlange in derselben Concurrency-Gruppe fair zu teilen), 5h Zeitbudget, treibt `backfill_alltx.py` voran (Block 1 bis aktueller Stand, läuft noch).
- **`weekly-monitor.yml`** — sonntags 21:59 UTC, fährt `backfill_history.py` (90-Tage-Fenster nachziehen), `rank_exchange_candidates.py`, `monitor.py`, `holder_distribution.py`, `whale_dormancy.py`, `address_clustering.py` und `coin_age_analysis.py` hintereinander, committet alle Ergebnisdateien.
- **`mempool-snapshot.yml`** — stündlich, `mempool_snapshot.py`. **Eigene** Concurrency-Gruppe (`mempool-snapshot`, nicht `pep-data-pipeline`) — sonst würde ein stündlicher Lauf hinter einem 5h-Backfill verhungern. Unproblematisch, weil er in eigene Dateien schreibt (kein Konfliktrisiko mit den anderen drei Workflows).

`genesis-backfill.yml`, `alltx-backfill.yml` und `weekly-monitor.yml` teilen sich die Concurrency-Gruppe `pep-data-pipeline` aus zwei Gründen: (1) sie schreiben teils an dieselben Dateien (`large_transfers/`, `miner_rewards/`) — paralleler Zugriff führte am 25.08.2026 zu einem Merge-Konflikt beim Push, der einen kompletten 5h-Lauf kostete; (2) alle drei nutzen denselben, vom Chef freigegebenen Endpoint mit demselben getesteten Rate-Budget (~28 Blöcke/s bei 20 gleichzeitigen Requests) — gleichzeitig gefahren würden sie sich dieses Budget streitig machen.

Alle Workflows lassen sich manuell per `workflow_dispatch` anstoßen (GitHub-UI oder `gh workflow run <name>.yml`). Das Repo ist bewusst **öffentlich** gestellt, damit GitHub-Actions-Minuten unbegrenzt und kostenlos sind (enthält nur öffentliche Blockchain-Daten und Skripte, keine Secrets).

Die Claude-Cloud-Routine (falls wieder aktiviert) eignet sich stattdessen für das, was tatsächlich funktioniert hat: aus den bereits im Repo liegenden Daten das Artifact-Dashboard bauen und veröffentlichen (siehe Git-Historie, Lauf vom 23.08.2026 — Datenabruf schlug fehl, das Dashboard aus vorhandenen Daten wurde aber erfolgreich gebaut und published).
