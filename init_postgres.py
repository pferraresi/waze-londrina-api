import os
import psycopg2

conn = psycopg2.connect(os.getenv("DATABASE_URL"))
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS alerts (
    id SERIAL PRIMARY KEY,
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
    location_x DOUBLE PRECISION,
    location_y DOUBLE PRECISION,
    pub_millis BIGINT,
    collected_at TIMESTAMP
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS jams (
    pk_id SERIAL PRIMARY KEY,
    id BIGINT,
    uuid BIGINT,
    country TEXT,
    city TEXT,
    street TEXT,
    level INTEGER,
    speed_kmh DOUBLE PRECISION,
    length INTEGER,
    delay INTEGER,
    road_type INTEGER,
    turn_type TEXT,
    blocking_alert_uuid TEXT,
    line_json TEXT,
    pub_millis BIGINT,
    collected_at TIMESTAMP
)
""")

conn.commit()
conn.close()

print("Tabelas criadas")