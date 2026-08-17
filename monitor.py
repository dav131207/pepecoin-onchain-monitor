import os
import json
import time
from datetime import datetime
import cloudscraper

DATA_DIR = "snapshots"
KNOWN_WHALES_FILE = "known_whales.json"
STATE_FILE = "last_scanned_block.txt"
BASE_URL = "https://pepeblocks.com/api"

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

def scan_large_transactions(scraper):
    large_txs = []
    print("Suche nach großen Transaktionen (> 500k PEP)...")
    try:
        r = scraper.get(f"{BASE_URL}/getblockcount", timeout=10)
        if r.status_code != 200:
            print("Fehler beim Abrufen der Blockhöhe.")
            return []
            
        current_height = int(r.text)
        last_scanned = get_last_scanned_block(current_height)
        
        # Max 2880 Blöcke (48 Stunden) pro Lauf, damit das Skript nicht ewig hängt
        start_height = max(last_scanned + 1, current_height - 2880)
        
        if start_height > current_height:
            print("Keine neuen Blöcke zum Scannen.")
            return []
            
        print(f"Scanne Blöcke von {start_height} bis {current_height}...")
        
        for height in range(start_height, current_height + 1):
            if height % 50 == 0:
                print(f"Scanne Block {height}/{current_height}...")
                
            r_hash = scraper.get(f"{BASE_URL}/getblockhash?index={height}", timeout=10)
            if r_hash.status_code != 200: continue
            bhash = r_hash.text.strip()
            
            r_block = scraper.get(f"{BASE_URL}/getblock?hash={bhash}&verbosity=2", timeout=10)
            if r_block.status_code != 200: continue
            block = r_block.json()
            
            block_time = block.get("time")
            
            for tx in block.get("tx", []):
                txid = tx.get("txid")
                # Summiere alle Ausgänge der Transaktion
                total_out = sum(vout.get("value", 0) for vout in tx.get("vout", []))
                
                if total_out >= 500000: # 500k PEP
                    large_txs.append({
                        "txid": txid,
                        "amount": total_out,
                        "time": block_time,
                        "block": height
                    })
            
            # Zustand speichern
            set_last_scanned_block(height)
            
    except Exception as e:
        print(f"Fehler beim Block-Scan: {e}")
        
    return large_txs

def generate_snapshot():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        
    scraper = cloudscraper.create_scraper()
    
    current_whales = []
    known_whales = load_known_whales()
    address_counts = {
        "100k": 0, "500k": 0, "1M": 0, "2M": 0, "5M": 0, "10M": 0, "20M": 0,
        "50M": 0, "100M": 0, "500M": 0, "1B": 0, "2B": 0, "3B": 0
    }
    
    status = "ok"
    print("Hole Rich List über die echte API...")
    
    page = 1
    limit = 1000
    keep_fetching = True
    
    while keep_fetching:
        url = f"{BASE_URL}/v3/addresses?page={page}&limit={limit}"
        
        try:
            response = scraper.get(url, timeout=30)
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
    
    # 2. Schritt: Große Transaktionen scannen
    large_txs = scan_large_transactions(scraper)
    print(f"{len(large_txs)} große Transaktionen (> 500k PEP) im überprüften Zeitraum gefunden.")
    
    save_known_whales(known_whales)
    
    snapshot = {
        "timestamp": datetime.now().isoformat(),
        "status": status,
        "metrics": {
            "address_counts": address_counts
        },
        "whales": current_whales,
        "large_transfers": large_txs,
        "transaction_counts": {
            "7_days": 0,
            "30_days": 0,
            "365_days": 0
        }
    }
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    filepath = os.path.join(DATA_DIR, f"{date_str}.json")
    
    with open(filepath, "w") as f:
        json.dump(snapshot, f, indent=4)
        
    print(f"Snapshot gespeichert unter: {filepath}")

if __name__ == "__main__":
    print("Starte On-Chain Monitor Run...")
    generate_snapshot()
