import os
import json
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="API Waze Londrina")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_connection():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL não definida")
    return psycopg2.connect(database_url)

@app.get("/")
def home():
    return {"message": "API Waze Londrina está no ar"}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/alerts")
def listar_alerts(limit: int = Query(20, le=200)):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT uuid, city, street, type, subtype, location_x, location_y, pub_millis, collected_at
        FROM alerts
        ORDER BY collected_at DESC
        LIMIT %s
    """, (limit,))

    rows = cur.fetchall()
    conn.close()
    return rows

@app.get("/jams")
def listar_jams(limit: int = Query(20, le=200)):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT id, uuid, city, street, level, speed_kmh, length, delay, blocking_alert_uuid, collected_at
        FROM jams
        ORDER BY collected_at DESC
        LIMIT %s
    """, (limit,))

    rows = cur.fetchall()
    conn.close()
    return rows

@app.get("/jams/por-rua")
def jams_por_rua(rua: str, limit: int = Query(50, le=200)):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT id, uuid, city, street, level, speed_kmh, length, delay, collected_at
        FROM jams
        WHERE street ILIKE %s
        ORDER BY collected_at DESC
        LIMIT %s
    """, (f"%{rua}%", limit))

    rows = cur.fetchall()
    conn.close()
    return rows

@app.get("/stats")
def stats():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM alerts")
    total_alerts = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM jams")
    total_jams = cur.fetchone()[0]

    conn.close()

    return {
        "total_alerts": total_alerts,
        "total_jams": total_jams
    }

@app.get("/map/jams")
def map_jams(limit: int = 20):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT street, level, speed_kmh, length, delay, line_json, collected_at
        FROM jams
        WHERE line_json IS NOT NULL AND line_json != ''
        ORDER BY collected_at DESC
        LIMIT %s
    """, (limit,))

    features = []

    for row in cur.fetchall():
        try:
            line = json.loads(row["line_json"])

            if not line or len(line) < 2:
                continue

            coordinates = []
            for point in line:
                if "x" in point and "y" in point:
                    coordinates.append([point["x"], point["y"]])

            if len(coordinates) < 2:
                continue

            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": coordinates
                },
                "properties": {
                    "street": row["street"],
                    "level": row["level"],
                    "speed": row["speed_kmh"],
                    "length": row["length"],
                    "delay": row["delay"],
                    "collected_at": row["collected_at"].isoformat() if row["collected_at"] else None
                }
            })
        except Exception as e:
            print(f"Erro ao processar jam: {e}")
            continue

    conn.close()

    return {
        "type": "FeatureCollection",
        "features": features
    }