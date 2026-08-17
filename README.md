# Pepecoin On-Chain Monitor

Datenspeicher für den wöchentlichen Pepecoin (PEP) On-Chain-Report-Agenten.

## Struktur

- `snapshots/YYYY-MM-DD.json` — ein JSON-Snapshot pro Lauf (Sonntagabend) mit:
  - Adressanzahl je Balance-Schwelle (100k, 500k, 1M, 2M, 5M, 10M, 20M PEP)
  - Liste bekannter Wal-Adressen (>= 10M PEP) mit Balance und "first_seen"
  - Große Transfers der letzten 7 Tage (> 10M PEP) mit from/to/amount/tx_hash
  - Transaktionsanzahl (7 Tage / 30 Tage / 365 Tage)
  - Zeitstempel des Laufs, Datenquelle-Status (ok / teilweise / fehlgeschlagen)
- `dashboard_url.txt` — die stabile Artifact-URL des Dashboards (nach dem ersten erfolgreichen Publish hier eintragen und danach bei jedem Lauf wiederverwenden, damit der Link stabil bleibt).
- `known_whales.json` — laufend gepflegte Liste aller jemals gesehenen Wal-Adressen (>= 10M PEP), inkl. `first_seen`-Datum, um "neue Wale" pro Lauf zu erkennen (Adressen, die seit dem letzten Snapshot neu hinzukamen).

## Datenquelle

Primär: https://pepeblocks.com/api (Cloudflare-geschützt, Zugriff ist Best-Effort und kann fehlschlagen — in diesem Fall im Snapshot und Dashboard transparent vermerken, keine Daten erfinden).
