"""
Consultas SQL dos endpoints públicos (/public/*) do Observatório.

Princípio: o coletor grava o feed inteiro a cada ~2 min. Cada linha de `jams`
é "um congestionamento visto em um snapshot", não um congestionamento novo.
Por isso todas as métricas aqui são normalizadas por snapshot:

  - congestionamentos ativos = linhas no snapshot (média por snapshot na hora)
  - km congestionados       = soma de `length` no snapshot (média por snapshot)
  - horas/dia em trânsito pesado por via = fração dos snapshots com N4/N5
    naquela via × 24 h  (equivalente ao ISO 37120 Ind. 17.2 usado no artigo)
  - alertas = contagem de `uuid` distintos, nunca de linhas

Sem dependências: só strings SQL com parâmetros nomeados no estilo psycopg2.
Nenhum '%' literal aparece no SQL (padrões ILIKE chegam por parâmetro).
"""

TZ = "America/Sao_Paulo"

# --- Exclusões combinadas na análise (out/2026) ------------------------------
# Ruas inteiras fora das estatísticas (obra de longa duração).
EXCLUDED_STREETS = ["%Constantino Pialarissi%"]
# Ruas fechadas para lazer aos domingos: o Waze registra como congestionamento.
SUNDAY_LEISURE_STREETS = ["%Joaquim de Matos Barreto%", "%Bento Munhoz da Rocha%"]
# Nível 5 que dura mais que isso é tratado como interdição (não trânsito).
N5_INTERDICTION_HOURS = 48


def default_params(**kw):
    p = {
        "tz": TZ,
        "excl": EXCLUDED_STREETS,
        "leisure": SUNDAY_LEISURE_STREETS,
        "n5h": N5_INTERDICTION_HOURS,
    }
    p.update(kw)
    return p


# CTE comum. Parâmetros: start, end (timestamps UTC sem fuso, como em collected_at).
#  j      : jams da janela, com horário local
#  snaps  : snapshots da janela (base de normalização)
#  long_n5: uuids de nível 5 que duram mais que n5h horas → interdição
#  jall   : todos os jams + marcações `interdicao` e `fora` (excluído das estatísticas)
#  jf     : jams que entram nas estatísticas
BASE_CTE = """
WITH j AS (
    SELECT jm.*,
           (jm.collected_at AT TIME ZONE 'UTC') AT TIME ZONE %(tz)s AS ts_local
    FROM jams jm
    WHERE jm.collected_at >= %(start)s
      AND jm.collected_at <= %(end)s
      AND jm.street IS NOT NULL AND jm.street <> ''
),
snaps AS (
    SELECT DISTINCT collected_at, ts_local FROM j
),
long_n5 AS (
    SELECT uuid
    FROM jams
    WHERE level = 5 AND uuid IS NOT NULL
      AND collected_at >= %(start)s - interval '3 days'
      AND collected_at <= %(end)s
    GROUP BY uuid
    HAVING MAX(collected_at) - MIN(collected_at) > make_interval(hours => %(n5h)s)
),
jall AS (
    SELECT j.*,
           (j.level = 5 AND j.uuid IN (SELECT uuid FROM long_n5)) AS interdicao,
           (j.street ILIKE ANY(%(excl)s::text[])
            OR (j.street ILIKE ANY(%(leisure)s::text[])
                AND EXTRACT(DOW FROM j.ts_local) = 0)) AS fora
    FROM j
),
jf AS (
    SELECT * FROM jall WHERE NOT fora
)
"""

LATEST_SNAPSHOT = "SELECT MAX(collected_at) AS last FROM jams"
LATEST_ALERT_SNAPSHOT = "SELECT MAX(collected_at) AS last FROM alerts"

# Situação no último snapshot (start = end = último snapshot).
NOW_SUMMARY = BASE_CTE + """
SELECT
    COUNT(*) FILTER (WHERE NOT interdicao)                               AS active_jams,
    COALESCE(SUM(length) FILTER (WHERE NOT interdicao), 0) / 1000.0      AS km_total,
    COALESCE(SUM(length) FILTER (WHERE level >= 4 AND NOT interdicao), 0) / 1000.0 AS km_heavy,
    COALESCE(SUM(GREATEST(delay, 0)) FILTER (WHERE NOT interdicao), 0) / 60.0 AS delay_min,
    AVG(speed_kmh) FILTER (WHERE level BETWEEN 1 AND 4)                  AS avg_speed_kmh,
    COUNT(*) FILTER (WHERE level = 1 AND NOT interdicao) AS l1,
    COUNT(*) FILTER (WHERE level = 2 AND NOT interdicao) AS l2,
    COUNT(*) FILTER (WHERE level = 3 AND NOT interdicao) AS l3,
    COUNT(*) FILTER (WHERE level = 4 AND NOT interdicao) AS l4,
    COUNT(*) FILTER (WHERE level = 5 AND NOT interdicao) AS l5,
    COUNT(*) FILTER (WHERE interdicao)                   AS interdicoes
FROM jf
"""

# Referência "normal": média por snapshot do mesmo dia da semana e hora local
# nas últimas N semanas (start..end = janela histórica), excluindo o dia atual.
TYPICAL_FOR_SLOT = BASE_CTE + """
, per_snap AS (
    SELECT s.collected_at,
           COALESCE(SUM(jf.length) FILTER (WHERE jf.level >= 4 AND NOT jf.interdicao), 0) / 1000.0 AS km_heavy,
           COALESCE(SUM(jf.length) FILTER (WHERE NOT jf.interdicao), 0) / 1000.0 AS km_total
    FROM snaps s
    LEFT JOIN jf ON jf.collected_at = s.collected_at
    WHERE EXTRACT(DOW FROM s.ts_local) = %(dow)s
      AND EXTRACT(HOUR FROM s.ts_local) = %(hour)s
      AND s.ts_local::date <> %(today)s::date
    GROUP BY s.collected_at
)
SELECT COUNT(*) AS snapshots,
       AVG(km_heavy) AS km_heavy,
       AVG(km_total) AS km_total
FROM per_snap
"""

# Geometrias do último snapshot (inclui ruas excluídas das estatísticas,
# marcadas como interdição/lazer para o mapa ficar fiel ao que o Waze mostra).
NOW_MAP = BASE_CTE + """
SELECT street, level, speed_kmh, length, delay, line_json,
       (interdicao OR (fora AND level = 5)) AS interdicao, fora
FROM jall
ORDER BY level, length
"""

# Alertas ativos no último snapshot de alertas (uuid distinto).
NOW_ALERTS = """
SELECT DISTINCT ON (uuid)
       uuid, type, subtype, street, location_x, location_y, pub_millis
FROM alerts
WHERE collected_at = %(end)s
ORDER BY uuid
"""

# Série horária: média por snapshot em cada hora local.
HOURLY = BASE_CTE + """
, per_snap AS (
    SELECT s.collected_at,
           date_trunc('hour', s.ts_local) AS hour_local,
           COUNT(jf.*) FILTER (WHERE NOT jf.interdicao) AS jams,
           COALESCE(SUM(jf.length) FILTER (WHERE NOT jf.interdicao), 0) / 1000.0 AS km_total,
           COALESCE(SUM(jf.length) FILTER (WHERE jf.level >= 4 AND NOT jf.interdicao), 0) / 1000.0 AS km_heavy,
           COALESCE(SUM(GREATEST(jf.delay, 0)) FILTER (WHERE NOT jf.interdicao), 0) / 60.0 AS delay_min
    FROM snaps s
    LEFT JOIN jf ON jf.collected_at = s.collected_at
    GROUP BY s.collected_at, date_trunc('hour', s.ts_local)
),
alert_hours AS (
    SELECT date_trunc('hour', (a.collected_at AT TIME ZONE 'UTC') AT TIME ZONE %(tz)s) AS hour_local,
           COUNT(DISTINCT a.uuid) FILTER (WHERE a.subtype ILIKE 'HAZARD_WEATHER_FLOOD'
                                            OR a.subtype ILIKE 'HAZARD_ON_ROAD_FLOOD') AS flood_alerts,
           COUNT(DISTINCT a.uuid) FILTER (WHERE a.type = 'ACCIDENT') AS accident_alerts
    FROM alerts a
    WHERE a.collected_at >= %(start)s AND a.collected_at <= %(end)s
    GROUP BY 1
)
SELECT to_char(p.hour_local, 'YYYY-MM-DD"T"HH24:00') AS hour,
       COUNT(*)                     AS snapshots,
       ROUND(AVG(p.jams)::numeric, 2)      AS avg_jams,
       ROUND(AVG(p.km_total)::numeric, 3)  AS avg_km_total,
       ROUND(AVG(p.km_heavy)::numeric, 3)  AS avg_km_heavy,
       ROUND(AVG(p.delay_min)::numeric, 1) AS avg_delay_min,
       COALESCE(MAX(ah.flood_alerts), 0)    AS flood_alerts,
       COALESCE(MAX(ah.accident_alerts), 0) AS accident_alerts
FROM per_snap p
LEFT JOIN alert_hours ah ON ah.hour_local = p.hour_local
GROUP BY p.hour_local
ORDER BY p.hour_local
"""

# Semana típica: dia da semana × hora local, média por snapshot.
TYPICAL_WEEK = BASE_CTE + """
, per_snap AS (
    SELECT s.collected_at, s.ts_local,
           COALESCE(SUM(jf.length) FILTER (WHERE jf.level >= 4 AND NOT jf.interdicao), 0) / 1000.0 AS km_heavy,
           COALESCE(SUM(jf.length) FILTER (WHERE NOT jf.interdicao), 0) / 1000.0 AS km_total,
           COUNT(jf.*) FILTER (WHERE NOT jf.interdicao) AS jams
    FROM snaps s
    LEFT JOIN jf ON jf.collected_at = s.collected_at
    GROUP BY s.collected_at, s.ts_local
)
SELECT EXTRACT(DOW FROM ts_local)::int  AS weekday,
       EXTRACT(HOUR FROM ts_local)::int AS hour,
       COUNT(*) AS snapshots,
       COUNT(DISTINCT ts_local::date) AS days,
       ROUND(AVG(km_heavy)::numeric, 3) AS avg_km_heavy,
       ROUND(AVG(km_total)::numeric, 3) AS avg_km_total,
       ROUND(AVG(jams)::numeric, 2)     AS avg_jams
FROM per_snap
GROUP BY 1, 2
ORDER BY 1, 2
"""

# Vias críticas. h/dia em trânsito pesado = fração de snapshots com N4/N5 × 24.
STREETS = BASE_CTE + """
, total AS (SELECT COUNT(*)::float AS n FROM snaps),
per_street_snap AS (
    SELECT street, collected_at, ts_local,
           MAX(level) AS max_level,
           SUM(GREATEST(delay, 0)) AS delay_s,
           AVG(speed_kmh) FILTER (WHERE level BETWEEN 1 AND 4) AS speed
    FROM jf
    WHERE NOT interdicao
    GROUP BY street, collected_at, ts_local
),
heavy_hour AS (
    SELECT street, EXTRACT(HOUR FROM ts_local)::int AS hour, COUNT(*) AS n,
           ROW_NUMBER() OVER (PARTITION BY street ORDER BY COUNT(*) DESC) AS rk
    FROM per_street_snap
    WHERE max_level >= 4
    GROUP BY street, EXTRACT(HOUR FROM ts_local)
)
SELECT p.street,
       ROUND((COUNT(*) FILTER (WHERE p.max_level >= 4) / t.n * 24)::numeric, 2) AS heavy_hours_per_day,
       ROUND((COUNT(*) / t.n * 24)::numeric, 2)                                  AS any_jam_hours_per_day,
       ROUND((AVG(p.delay_s) / 60.0)::numeric, 1)                                AS avg_delay_min,
       ROUND(AVG(p.speed)::numeric, 1)                                           AS avg_speed_kmh,
       ROUND((COUNT(*) FILTER (WHERE p.max_level >= 4)::float / COUNT(*) * 100)::numeric, 1) AS heavy_share_pct,
       COUNT(*) AS snapshots_with_jam,
       MAX(h.hour) AS peak_hour
FROM per_street_snap p
CROSS JOIN total t
LEFT JOIN heavy_hour h ON h.street = p.street AND h.rk = 1
GROUP BY p.street, t.n
HAVING COUNT(*) >= %(min_snaps)s
ORDER BY heavy_hours_per_day DESC, any_jam_hours_per_day DESC
LIMIT %(limit)s
"""

# Alertas na janela, por tipo/subtipo (uuid distinto).
ALERTS_SUMMARY = """
SELECT type, COALESCE(NULLIF(subtype, ''), type) AS subtype,
       COUNT(DISTINCT uuid) AS total
FROM alerts
WHERE collected_at >= %(start)s AND collected_at <= %(end)s
GROUP BY 1, 2
ORDER BY total DESC
"""

ALERTS_TOP_STREETS = """
SELECT street, COUNT(DISTINCT uuid) AS total
FROM alerts
WHERE collected_at >= %(start)s AND collected_at <= %(end)s
  AND street IS NOT NULL AND street <> ''
  AND (type ILIKE %(pattern)s OR subtype ILIKE %(pattern)s)
GROUP BY street
ORDER BY total DESC
LIMIT %(limit)s
"""

# Interdições: N5 longos ativos no último snapshot + desde quando.
INTERDICTIONS = BASE_CTE + """
SELECT ja.street,
       MAX(ja.length) AS length_m,
       (SELECT MIN((x.collected_at AT TIME ZONE 'UTC') AT TIME ZONE %(tz)s)
          FROM jams x WHERE x.uuid = ja.uuid AND x.level = 5) AS since_local
FROM jall ja
WHERE ja.interdicao OR (ja.fora AND ja.level = 5)
GROUP BY ja.street, ja.uuid
ORDER BY since_local
"""

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_jams_collected_at ON jams (collected_at)",
    "CREATE INDEX IF NOT EXISTS idx_jams_uuid_level ON jams (uuid, level)",
    "CREATE INDEX IF NOT EXISTS idx_alerts_collected_at ON alerts (collected_at)",
]
