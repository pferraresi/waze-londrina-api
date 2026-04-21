import requests
import json
import time
import sqlite3
from datetime import datetime
from pathlib import Path

WAZE_FEED_URL = "https://www.waze.com/row-partnerhub-api/partners/11989759594/waze-feeds/416872e9-afd0-41fb-9c5f-ce4d1bc84706?format=1"
BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "waze_londrina.db"
SNAPSHOT_DIR = BASE_DIR / "snapshots"

def fetch_data():
    response = requests.get(WAZE_FEED_URL, timeout=30)
    response.raise_for_status()
    return response.json()

def save_snapshot(data):
    SNAPSHOT_DIR.mkdir(exist_ok=True)

    now = datetime.now().isoformat().replace(":", "-").replace(".", "-")
    arquivo = SNAPSHOT_DIR / f"data_{now}.json"

    with open(arquivo, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Snapshot salvo: {arquivo.name}")

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            uuid TEXT,
            country TEXT,
            city TEXT,
            type TEXT,
            subtype TEXT,
            street TEXT,
            road_type INTEGER,
            report_rating INTEGER,
            reliability INTEGER,
            confidence INTEGER,
            magvar INTEGER,
            report_description TEXT,
            location_x REAL,
            location_y REAL,
            pub_millis INTEGER,
            collected_at TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS jams (
            id INTEGER,
            uuid INTEGER,
            country TEXT,
            city TEXT,
            street TEXT,
            level INTEGER,
            speed_kmh REAL,
            length INTEGER,
            delay INTEGER,
            road_type INTEGER,
            turn_type TEXT,
            blocking_alert_uuid TEXT,
            line_json TEXT,
            pub_millis INTEGER,
            collected_at TEXT,
            PRIMARY KEY (id, collected_at)
        )
    """)

    conn.commit()
    conn.close()

def save_alerts_to_db(data, collected_at):
    alerts = data.get("alerts", [])
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    for alert in alerts:
        location = alert.get("location", {})

        cur.execute("""
            INSERT INTO alerts (
                uuid, country, city, type, subtype, street, road_type,
                report_rating, reliability, confidence, magvar,
                report_description, location_x, location_y, pub_millis, collected_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            alert.get("uuid"),
            alert.get("country"),
            alert.get("city"),
            alert.get("type"),
            alert.get("subtype"),
            alert.get("street"),
            alert.get("roadType"),
            alert.get("reportRating"),
            alert.get("reliability"),
            alert.get("confidence"),
            alert.get("magvar"),
            alert.get("reportDescription"),
            location.get("x"),
            location.get("y"),
            alert.get("pubMillis"),
            collected_at
        ))

    conn.commit()
    conn.close()

def save_jams_to_db(data, collected_at):
    jams = data.get("jams", [])
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    for jam in jams:
        cur.execute("""
            INSERT OR REPLACE INTO jams (
                id, uuid, country, city, street, level, speed_kmh, length,
                delay, road_type, turn_type, blocking_alert_uuid,
                line_json, pub_millis, collected_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            jam.get("id"),
            jam.get("uuid"),
            jam.get("country"),
            jam.get("city"),
            jam.get("street"),
            jam.get("level"),
            jam.get("speedKMH"),
            jam.get("length"),
            jam.get("delay"),
            jam.get("roadType"),
            jam.get("turnType"),
            jam.get("blockingAlertUuid"),
            json.dumps(jam.get("line", []), ensure_ascii=False),
            jam.get("pubMillis"),
            collected_at
        ))

    conn.commit()
    conn.close()

def run_once():
    data = fetch_data()
    collected_at = datetime.now().isoformat()

    save_snapshot(data)
    save_alerts_to_db(data, collected_at)
    save_jams_to_db(data, collected_at)

    print(f"Banco atualizado em: {collected_at}")
    print(f"Jams: {len(data.get('jams', []))} | Alerts: {len(data.get('alerts', []))}")
    print("-" * 60)

def run_loop(intervalo_segundos=120):
    init_db()

    while True:
        try:
            run_once()
        except Exception as e:
            print(f"Erro na coleta: {e}")
            print("-" * 60)

        time.sleep(intervalo_segundos)

if __name__ == "__main__":
    run_loop(120)