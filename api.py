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

@app.get("/debug/env")
def debug_env():
    database_url = os.getenv("DATABASE_URL")
    return {
        "database_url_exists": bool(database_url),
        "uses_internal_host": "railway.internal" in database_url if database_url else False
    }

@app.get("/debug/db")
def debug_db():
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT current_database(), current_user")
        row = cur.fetchone()
        conn.close()
        return {
            "ok": True,
            "database": row[0],
            "user": row[1]
        }
    except Exception as e:
        return {
            "ok": False,
            "error_type": type(e).__name__,
            "error": str(e)
        }

@app.get("/debug/tables")
def debug_tables():
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        rows = cur.fetchall()
        conn.close()
        return {
            "ok": True,
            "tables": [r[0] for r in rows]
        }
    except Exception as e:
        return {
            "ok": False,
            "error_type": type(e).__name__,
            "error": str(e)
        }

@app.get("/debug/latest")
def debug_latest():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT MAX(collected_at) FROM alerts")
    latest_alert = cur.fetchone()[0]

    cur.execute("SELECT MAX(collected_at) FROM jams")
    latest_jam = cur.fetchone()[0]

    conn.close()

    return {
        "latest_alert_collected_at": latest_alert.isoformat() if latest_alert else None,
        "latest_jam_collected_at": latest_jam.isoformat() if latest_jam else None
    }

@app.get("/analytics/jams-by-level")
def jams_by_level(limit: int = Query(10000, le=50000)):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT level, COUNT(*) AS total
        FROM (
            SELECT level
            FROM jams
            ORDER BY collected_at DESC
            LIMIT %s
        ) sub
        GROUP BY level
        ORDER BY level
    """, (limit,))

    rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/analytics/top-streets-jams")
def top_streets_jams(limit: int = Query(10, le=100), sample_size: int = Query(10000, le=50000)):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT street, COUNT(*) AS total
        FROM (
            SELECT street
            FROM jams
            WHERE street IS NOT NULL AND street != ''
            ORDER BY collected_at DESC
            LIMIT %s
        ) sub
        GROUP BY street
        ORDER BY total DESC, street ASC
        LIMIT %s
    """, (sample_size, limit))

    rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/analytics/alerts-by-type")
def alerts_by_type(limit: int = Query(10000, le=50000)):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT type, COUNT(*) AS total
        FROM (
            SELECT type
            FROM alerts
            ORDER BY collected_at DESC
            LIMIT %s
        ) sub
        GROUP BY type
        ORDER BY total DESC, type ASC
    """, (limit,))

    rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/analytics/jams-timeseries-hourly")
def jams_timeseries_hourly(hours: int = Query(24, le=168)):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT
            to_char(date_trunc('hour', collected_at), 'YYYY-MM-DD HH24:00:00') AS hour_bucket,
            COUNT(*) AS total
        FROM jams
        WHERE collected_at >= NOW() - (%s || ' hours')::interval
        GROUP BY date_trunc('hour', collected_at)
        ORDER BY date_trunc('hour', collected_at)
    """, (hours,))

    rows = cur.fetchall()
    conn.close()
    return rows

@app.get("/analytics/jams-by-level-hour")
def jams_by_level_hour(hours: int = Query(24, le=168)):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT
            EXTRACT(HOUR FROM collected_at) AS hour_of_day,
            level,
            COUNT(*) AS total
        FROM jams
        WHERE collected_at >= NOW() - (%s || ' hours')::interval
        GROUP BY EXTRACT(HOUR FROM collected_at), level
        ORDER BY hour_of_day, level
    """, (hours,))

    rows = cur.fetchall()
    conn.close()
    return rows

@app.get("/analytics/jams-by-weekday-hour")
def jams_by_weekday_hour(days: int = Query(7, le=30)):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT
            EXTRACT(DOW FROM collected_at) AS weekday,
            EXTRACT(HOUR FROM collected_at) AS hour,
            level,
            COUNT(*) AS total
        FROM jams
        WHERE collected_at >= NOW() - (%s || ' days')::interval
        GROUP BY weekday, hour, level
        ORDER BY weekday, hour, level
    """, (days,))

    rows = cur.fetchall()
    conn.close()
    return rows