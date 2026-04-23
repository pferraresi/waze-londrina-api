import os
import json
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from collections import defaultdict
from datetime import timedelta

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


@app.get("/analytics/top-streets-by-length")
def top_streets_by_length(limit: int = Query(10, le=100), hours: int = Query(24, le=168)):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT
            street,
            COUNT(*) AS total_jams,
            SUM(COALESCE(length, 0)) AS total_length_m,
            AVG(COALESCE(length, 0)) AS avg_length_m
        FROM jams
        WHERE collected_at >= NOW() - (%s || ' hours')::interval
          AND street IS NOT NULL
          AND street != ''
        GROUP BY street
        ORDER BY total_length_m DESC
        LIMIT %s
    """, (hours, limit))

    rows = cur.fetchall()
    conn.close()
    return rows

@app.get("/analytics/top-critical-streets-by-length")
def top_critical_streets_by_length(limit: int = Query(10, le=100), hours: int = Query(24, le=168)):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT
            street,
            COUNT(*) AS total_jams,
            SUM(COALESCE(length, 0)) AS total_length_m
        FROM jams
        WHERE collected_at >= NOW() - (%s || ' hours')::interval
          AND level = 5
          AND street IS NOT NULL
          AND street != ''
        GROUP BY street
        ORDER BY total_length_m DESC
        LIMIT %s
    """, (hours, limit))

    rows = cur.fetchall()
    conn.close()
    return rows


@app.get("/analytics/jams-with-closures")
def jams_with_closures(hours: int = Query(24, le=168), limit: int = Query(100, le=1000)):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT
            j.street,
            j.level,
            j.speed_kmh,
            j.delay,
            j.length,
            j.collected_at AS jam_time,
            a.type AS alert_type,
            a.subtype AS alert_subtype,
            a.report_description
        FROM jams j
        LEFT JOIN alerts a
          ON j.street = a.street
         AND a.collected_at BETWEEN j.collected_at - interval '30 minutes'
                               AND j.collected_at + interval '30 minutes'
        WHERE j.collected_at >= NOW() - (%s || ' hours')::interval
          AND j.street IS NOT NULL
          AND j.street != ''
          AND (
              a.type ILIKE '%%CLOSED%%'
              OR a.subtype ILIKE '%%CONSTRUCTION%%'
              OR a.subtype ILIKE '%%ROAD_CLOSED%%'
              OR a.subtype ILIKE '%%HAZARD_ON_ROAD_CONSTRUCTION%%'
              OR a.report_description ILIKE '%%obra%%'
              OR a.report_description ILIKE '%%interdi%%'
          )
        ORDER BY j.collected_at DESC
        LIMIT %s
    """, (hours, limit))

    rows = cur.fetchall()
    conn.close()
    return rows

@app.get("/analytics/top-streets-with-closures")
def top_streets_with_closures(hours: int = Query(24, le=168), limit: int = Query(20, le=100)):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT
            j.street,
            COUNT(*) AS total_jams,
            SUM(COALESCE(j.length, 0)) AS total_length_m,
            MAX(j.level) AS max_level
        FROM jams j
        JOIN alerts a
          ON j.street = a.street
         AND a.collected_at BETWEEN j.collected_at - interval '30 minutes'
                               AND j.collected_at + interval '30 minutes'
        WHERE j.collected_at >= NOW() - (%s || ' hours')::interval
          AND j.street IS NOT NULL
          AND j.street != ''
          AND (
              a.type ILIKE '%%CLOSED%%'
              OR a.subtype ILIKE '%%CONSTRUCTION%%'
              OR a.subtype ILIKE '%%ROAD_CLOSED%%'
              OR a.subtype ILIKE '%%HAZARD_ON_ROAD_CONSTRUCTION%%'
              OR a.report_description ILIKE '%%obra%%'
              OR a.report_description ILIKE '%%interdi%%'
          )
        GROUP BY j.street
        ORDER BY total_length_m DESC
        LIMIT %s
    """, (hours, limit))

    rows = cur.fetchall()
    conn.close()
    return rows

@app.get("/analytics/jam-durations")
def jam_durations(hours: int = Query(24, le=168), gap_minutes: int = Query(5, le=30), limit: int = Query(200, le=1000)):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT street, level, length, delay, collected_at
        FROM jams
        WHERE collected_at >= NOW() - (%s || ' hours')::interval
          AND street IS NOT NULL
          AND street != ''
        ORDER BY street, level, collected_at
    """, (hours,))

    rows = cur.fetchall()
    conn.close()

    grouped = defaultdict(list)
    for row in rows:
        key = (row["street"], row["level"])
        grouped[key].append(row)

    episodes = []
    max_gap = timedelta(minutes=gap_minutes)

    for (street, level), items in grouped.items():
        if not items:
            continue

        start = items[0]["collected_at"]
        last = items[0]["collected_at"]
        total_length = float(items[0]["length"] or 0)
        total_delay = float(items[0]["delay"] or 0)
        count = 1

        for row in items[1:]:
            current = row["collected_at"]
            if current - last <= max_gap:
                last = current
                total_length += float(row["length"] or 0)
                total_delay += float(row["delay"] or 0)
                count += 1
            else:
                duration_min = (last - start).total_seconds() / 60
                episodes.append({
                    "street": street,
                    "level": level,
                    "start_time": start.isoformat(),
                    "end_time": last.isoformat(),
                    "estimated_duration_min": round(duration_min, 1),
                    "observations": count,
                    "avg_length_m": round(total_length / count, 1),
                    "avg_delay_s": round(total_delay / count, 1)
                })
                start = current
                last = current
                total_length = float(row["length"] or 0)
                total_delay = float(row["delay"] or 0)
                count = 1

        duration_min = (last - start).total_seconds() / 60
        episodes.append({
            "street": street,
            "level": level,
            "start_time": start.isoformat(),
            "end_time": last.isoformat(),
            "estimated_duration_min": round(duration_min, 1),
            "observations": count,
            "avg_length_m": round(total_length / count, 1),
            "avg_delay_s": round(total_delay / count, 1)
        })

    episodes.sort(key=lambda x: x["estimated_duration_min"], reverse=True)
    return episodes[:limit]

@app.get("/analytics/jam-duration-summary")
def jam_duration_summary(hours: int = Query(24, le=168), gap_minutes: int = Query(5, le=30)):
    episodes = jam_durations(hours=hours, gap_minutes=gap_minutes, limit=5000)

    if not episodes:
        return {
            "episodes": 0,
            "avg_duration_min": 0,
            "max_duration_min": 0
        }

    durations = [e["estimated_duration_min"] for e in episodes]
    return {
        "episodes": len(episodes),
        "avg_duration_min": round(sum(durations) / len(durations), 1),
        "max_duration_min": round(max(durations), 1)
    }

@app.get("/analytics/impact-summary")
def impact_summary(hours: int = Query(24, le=168)):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT
            COUNT(*) AS total_jams,
            SUM(COALESCE(delay, 0)) AS total_delay_s,
            SUM(COALESCE(length, 0)) AS total_length_m,
            SUM(COALESCE(delay, 0) * COALESCE(length, 0)) AS impact_score
        FROM jams
        WHERE collected_at >= NOW() - (%s || ' hours')::interval
    """, (hours,))

    row = cur.fetchone()
    conn.close()
    return row

@app.get("/analytics/top-streets-by-impact")
def top_streets_by_impact(hours: int = Query(24, le=168), limit: int = Query(10, le=100)):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT
            street,
            COUNT(*) AS total_jams,
            SUM(COALESCE(delay, 0)) AS total_delay_s,
            SUM(COALESCE(length, 0)) AS total_length_m,
            SUM(COALESCE(delay, 0) * COALESCE(length, 0)) AS impact_score
        FROM jams
        WHERE collected_at >= NOW() - (%s || ' hours')::interval
          AND street IS NOT NULL
          AND street != ''
        GROUP BY street
        ORDER BY impact_score DESC
        LIMIT %s
    """, (hours, limit))

    rows = cur.fetchall()
    conn.close()
    return rows

@app.get("/analytics/critical-jams")
def critical_jams(hours: int = Query(24, le=168), limit: int = Query(20, le=100)):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT
            street,
            level,
            AVG(length) AS avg_length,
            AVG(delay) AS avg_delay,
            COUNT(*) AS observations,
            MIN(collected_at) AS start_time,
            MAX(collected_at) AS end_time,
            EXTRACT(EPOCH FROM (MAX(collected_at) - MIN(collected_at))) / 60 AS duration_min
        FROM jams
        WHERE collected_at >= NOW() - (%s || ' hours')::interval
          AND street IS NOT NULL
          AND street != ''
        GROUP BY street, level
    """, (hours,))

    rows = cur.fetchall()
    conn.close()

    results = []
    for r in rows:
        duration = float(r["duration_min"] or 0)
        length = float(r["avg_length"] or 0)
        level = int(r["level"] or 1)

        criticidade = level * duration * length

        results.append({
            "street": r["street"],
            "level": level,
            "duration_min": round(duration, 1),
            "avg_length_m": round(length, 1),
            "criticidade": round(criticidade, 0)
        })

    results.sort(key=lambda x: x["criticidade"], reverse=True)

    return results[:limit]