import os
import sqlite3
from pathlib import Path

import psycopg2

BASE_DIR = Path(__file__).parent
SQLITE_PATH = BASE_DIR / "waze_londrina.db"
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL não definida")

print("Iniciando migração...")
print(f"SQLite em: {SQLITE_PATH}")

print("Conectando ao SQLite...")
sqlite_conn = sqlite3.connect(SQLITE_PATH)
sqlite_conn.row_factory = sqlite3.Row
sqlite_cur = sqlite_conn.cursor()
print("Conectado ao SQLite.")

print("Conectando ao PostgreSQL...")
pg_conn = psycopg2.connect(DATABASE_URL)
pg_cur = pg_conn.cursor()
print("Conectado ao PostgreSQL.")

print("Lendo alerts do SQLite...")
sqlite_cur.execute("SELECT * FROM alerts")
alerts = sqlite_cur.fetchall()
print(f"Total de alerts encontrados: {len(alerts)}")

for i, row in enumerate(alerts, start=1):
    pg_cur.execute("""
        INSERT INTO alerts (
            uuid, country, city, type, subtype, street, road_type,
            report_rating, reliability, confidence, magvar,
            report_description, location_x, location_y, pub_millis, collected_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        row["uuid"],
        row["country"],
        row["city"],
        row["type"],
        row["subtype"],
        row["street"],
        row["road_type"],
        row["report_rating"],
        row["reliability"],
        row["confidence"],
        row["magvar"],
        row["report_description"],
        row["location_x"],
        row["location_y"],
        row["pub_millis"],
        row["collected_at"],
    ))
    if i % 100 == 0:
        print(f"Alerts migrados: {i}")

print("Lendo jams do SQLite...")
sqlite_cur.execute("SELECT * FROM jams")
jams = sqlite_cur.fetchall()
print(f"Total de jams encontrados: {len(jams)}")

for i, row in enumerate(jams, start=1):
    pg_cur.execute("""
        INSERT INTO jams (
            id, uuid, country, city, street, level, speed_kmh, length,
            delay, road_type, turn_type, blocking_alert_uuid,
            line_json, pub_millis, collected_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        row["id"],
        row["uuid"],
        row["country"],
        row["city"],
        row["street"],
        row["level"],
        row["speed_kmh"],
        row["length"],
        row["delay"],
        row["road_type"],
        row["turn_type"],
        row["blocking_alert_uuid"],
        row["line_json"],
        row["pub_millis"],
        row["collected_at"],
    ))
    if i % 100 == 0:
        print(f"Jams migrados: {i}")

print("Efetuando commit...")
pg_conn.commit()

sqlite_conn.close()
pg_conn.close()

print("Migração concluída com sucesso.")