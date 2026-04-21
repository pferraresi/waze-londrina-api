import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "waze_londrina.db"

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

print("Total de alerts:")
cur.execute("SELECT COUNT(*) FROM alerts")
print(cur.fetchone()[0])

print("Total de jams:")
cur.execute("SELECT COUNT(*) FROM jams")
print(cur.fetchone()[0])

print("\nÚltimos 5 jams:")
cur.execute("""
    SELECT street, level, speed_kmh, length, delay, collected_at
    FROM jams
    ORDER BY collected_at DESC
    LIMIT 5
""")
for row in cur.fetchall():
    print(row)

conn.close()