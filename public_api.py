"""
Endpoints públicos do Observatório (/public/*), pensados para o dashboard
voltado à comunidade. Métricas normalizadas por snapshot — ver public_sql.py.

Para ativar, em api.py:
    from public_api import router as public_router, ensure_indexes
    app.include_router(public_router)
    ensure_indexes()   # cria índices só se PUBLIC_API_CREATE_INDEXES=1
"""
import json
import os
import time
import threading
from datetime import datetime, timedelta, date
from decimal import Decimal
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras
from fastapi import APIRouter, Query, HTTPException

import public_sql as Q

router = APIRouter(prefix="/public", tags=["público"])
LOCAL_TZ = ZoneInfo(Q.TZ)

# ---------------------------------------------------------------- infraestrutura

_cache = {}
_cache_lock = threading.Lock()


def cached(key, ttl, fn):
    """Cache em memória. Protege o Postgres do Railway: o dashboard pode ter
    muitos visitantes, mas cada consulta pesada roda no máximo 1x por TTL."""
    now = time.time()
    with _cache_lock:
        hit = _cache.get(key)
        if hit and now - hit[0] < ttl:
            return hit[1]
    value = fn()
    with _cache_lock:
        _cache[key] = (now, value)
    return value


def _clean(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return v


def query(sql, params=None):
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params or {})
        return [{k: _clean(v) for k, v in row.items()} for row in cur.fetchall()]
    finally:
        conn.close()


def ensure_indexes():
    """Só cria índices se PUBLIC_API_CREATE_INDEXES=1 (ocupam ~100–200 MB)."""
    if os.getenv("PUBLIC_API_CREATE_INDEXES") != "1":
        return
    try:
        conn = psycopg2.connect(os.environ["DATABASE_URL"])
        conn.autocommit = True
        cur = conn.cursor()
        for stmt in Q.INDEXES:
            cur.execute(stmt)
        conn.close()
    except Exception as e:  # não derruba a API se faltar permissão
        print(f"[public_api] índices não criados: {e}")


def latest_snapshot():
    rows = query(Q.LATEST_SNAPSHOT)
    if not rows or not rows[0]["last"]:
        raise HTTPException(503, "Sem dados coletados ainda")
    return datetime.fromisoformat(rows[0]["last"])


def to_local(ts_utc):
    return ts_utc.replace(tzinfo=ZoneInfo("UTC")).astimezone(LOCAL_TZ)


def P(**kw):
    return Q.default_params(**kw)


# ---------------------------------------------------------------- endpoints

@router.get("/now")
def now():
    """Situação no último snapshot + comparação com o 'normal' para o mesmo
    dia da semana e hora (média das últimas 4 semanas)."""
    def compute():
        last = latest_snapshot()
        summary = query(Q.NOW_SUMMARY, P(start=last, end=last))[0]
        loc = to_local(last)
        typical = query(Q.TYPICAL_FOR_SLOT, P(
            start=last - timedelta(days=28), end=last,
            dow=loc.isoweekday() % 7, hour=loc.hour, today=loc.date()))[0]
        a_last = query(Q.LATEST_ALERT_SNAPSHOT)[0]["last"]
        alerts = []
        if a_last:
            alerts = query(Q.NOW_ALERTS, {"end": datetime.fromisoformat(a_last)})
        by_type = {}
        for a in alerts:
            key = a["subtype"] or a["type"]
            by_type[key] = by_type.get(key, 0) + 1
        age_min = (datetime.utcnow() - last).total_seconds() / 60
        return {
            "collected_at_utc": last.isoformat(),
            "collected_at_local": loc.isoformat(),
            "data_age_minutes": round(age_min, 1),
            "stale": age_min > 30,
            **summary,
            "typical": typical,
            "alerts_active": len(alerts),
            "alerts_by_subtype": by_type,
        }
    return cached("now", 90, compute)


@router.get("/map")
def map_now():
    """GeoJSON do último snapshot: congestionamentos (linhas) + alertas (pontos)."""
    def compute():
        last = latest_snapshot()
        feats = []
        for r in query(Q.NOW_MAP, P(start=last, end=last)):
            try:
                line = json.loads(r["line_json"] or "[]")
            except ValueError:
                continue
            coords = [[p["x"], p["y"]] for p in line if "x" in p and "y" in p]
            if len(coords) < 2:
                continue
            feats.append({
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": coords},
                "properties": {
                    "kind": "jam", "street": r["street"], "level": r["level"],
                    "speed_kmh": r["speed_kmh"], "length_m": r["length"],
                    "delay_s": r["delay"], "interdicao": r["interdicao"],
                },
            })
        a_last = query(Q.LATEST_ALERT_SNAPSHOT)[0]["last"]
        if a_last:
            for a in query(Q.NOW_ALERTS, {"end": datetime.fromisoformat(a_last)}):
                if a["location_x"] is None:
                    continue
                feats.append({
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [a["location_x"], a["location_y"]]},
                    "properties": {"kind": "alert", "type": a["type"],
                                   "subtype": a["subtype"], "street": a["street"],
                                   "pub_millis": a["pub_millis"]},
                })
        return {"type": "FeatureCollection", "collected_at_utc": last.isoformat(), "features": feats}
    return cached("map", 90, compute)


@router.get("/hourly")
def hourly(hours: int = Query(168, ge=1, le=4800)):
    """Série horária (hora local) normalizada por snapshot. Até 200 dias.
    Horas sem coleta não aparecem (lacunas ficam visíveis no gráfico)."""
    def compute():
        last = latest_snapshot()
        return query(Q.HOURLY, P(start=last - timedelta(hours=hours), end=last))
    return cached(f"hourly:{hours}", 3600 if hours > 720 else (600 if hours > 48 else 120), compute)


@router.get("/typical-week")
def typical_week(days: int = Query(28, ge=7, le=90)):
    """Semana típica: média de km em trânsito pesado por dia da semana × hora."""
    def compute():
        last = latest_snapshot()
        return query(Q.TYPICAL_WEEK, P(start=last - timedelta(days=days), end=last))
    return cached(f"week:{days}", 3600, compute)


@router.get("/streets")
def streets(days: int = Query(7, ge=1, le=60), limit: int = Query(15, le=50)):
    """Vias críticas: horas por dia em trânsito pesado (N4/N5), excluindo
    interdições, obra da Pialarissi e lazer de domingo."""
    def compute():
        last = latest_snapshot()
        min_snaps = max(10, days * 5)  # evita vias com poucas observações
        return query(Q.STREETS, P(start=last - timedelta(days=days), end=last,
                                  min_snaps=min_snaps, limit=limit))
    return cached(f"streets:{days}:{limit}", 1800, compute)


@router.get("/alerts")
def alerts(days: int = Query(7, ge=1, le=60)):
    """Alertas da comunidade Waze na janela (uuid distinto) + vias com mais
    buracos e alagamentos."""
    def compute():
        last = latest_snapshot()
        win = {"start": last - timedelta(days=days), "end": last}
        return {
            "by_subtype": query(Q.ALERTS_SUMMARY, win),
            "top_potholes": query(Q.ALERTS_TOP_STREETS, {**win, "pattern": "%POT_HOLE%", "limit": 10}),
            "top_floods": query(Q.ALERTS_TOP_STREETS, {**win, "pattern": "%FLOOD%", "limit": 10}),
        }
    return cached(f"alerts:{days}", 900, compute)


@router.get("/interdicoes")
def interdicoes():
    """Bloqueios longos (nível 5 por mais de 48 h, obras conhecidas)."""
    def compute():
        last = latest_snapshot()
        return query(Q.INTERDICTIONS, P(start=last, end=last))
    return cached("interdicoes", 300, compute)


@router.get("/audit/slot")
def audit_slot(dow: int = Query(0, ge=0, le=6), h1: int = Query(10, ge=0, le=23),
               h2: int = Query(14, ge=0, le=23), days: int = Query(56, ge=7, le=180),
               limit: int = Query(30, le=100)):
    """Auditoria por rua de uma faixa horária de um dia da semana (0 = domingo)
    contra dias úteis no mesmo horário. Inclui ruas já excluídas (marcadas)."""
    def compute():
        last = latest_snapshot()
        return query(Q.AUDIT_SLOT, P(start=last - timedelta(days=days), end=last,
                                     dow=dow, h1=h1, h2=h2, limit=limit))
    return cached(f"audit:{dow}:{h1}:{h2}:{days}:{limit}", 1800, compute)


@router.get("/audit/street")
def audit_street(street: str = Query(..., min_length=3),
                 lat: float = Query(...), lon: float = Query(...),
                 radius: int = Query(400, ge=50, le=3000),
                 dow: int = Query(0, ge=0, le=6),
                 h1: int = Query(8, ge=0, le=23), h2: int = Query(14, ge=0, le=23),
                 days: int = Query(56, ge=7, le=180)):
    """Auditoria de uma rua em torno de um ponto: por hora (perto/longe do ponto,
    dia-alvo × dias úteis) e por trecho de ~200 m no dia-alvo entre h1 e h2."""
    def compute():
        last = latest_snapshot()
        params = P(start=last - timedelta(days=days), end=last, dow=dow,
                   street=f"%{street}%", lat=lat, lon=lon, radius=radius, h1=h1, h2=h2)
        return {
            "params": {"street": street, "lat": lat, "lon": lon, "radius_m": radius,
                       "dow": dow, "h1": h1, "h2": h2, "days": days},
            "by_hour": query(Q.AUDIT_STREET_BY_HOUR, params),
            "by_segment": query(Q.AUDIT_STREET_BY_SEGMENT, params),
        }
    key = f"astreet:{street}:{lat}:{lon}:{radius}:{dow}:{h1}:{h2}:{days}"
    return cached(key, 1800, compute)


@router.get("/meta")
def meta():
    """Regras de exclusão em vigor — exibidas na aba 'Sobre os dados'."""
    return {
        "excluded_streets": Q.EXCLUDED_STREETS,
        "sunday_leisure_streets": Q.SUNDAY_LEISURE_STREETS,
        "holidays": Q.HOLIDAYS,
        "n5_interdiction_hours": Q.N5_INTERDICTION_HOURS,
        "timezone": Q.TZ,
    }
