import os
import json
import time
import signal
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests

WAZE_FEED_URL = "https://www.waze.com/row-partnerhub-api/partners/11989759594/waze-feeds/416872e9-afd0-41fb-9c5f-ce4d1bc84706?format=1"
DATABASE_URL = os.getenv("DATABASE_URL")

BASE_DIR = Path(__file__).parent
SNAPSHOT_DIR = BASE_DIR / "snapshots"
# Snapshots em JSON no disco local: desligados por padrão (o banco já guarda tudo).
SAVE_SNAPSHOTS = os.getenv("SAVE_SNAPSHOTS", "0") == "1"

# Limites de tempo: nenhuma etapa pode travar o laço para sempre.
CYCLE_TIMEOUT_S = int(os.getenv("CYCLE_TIMEOUT_S", "90"))


class CycleTimeout(Exception):
    pass


def _on_alarm(signum, frame):
    raise CycleTimeout(f"ciclo passou de {CYCLE_TIMEOUT_S} s")


def get_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não definida")
    return psycopg2.connect(
        DATABASE_URL,
        connect_timeout=10,              # desiste de conectar após 10 s
        keepalives=1,                    # detecta conexão morta (rede caiu)
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=3,
        options="-c statement_timeout=60000",  # nenhuma consulta passa de 60 s
    )


def fetch_data():
    response = requests.get(WAZE_FEED_URL, timeout=(10, 30))
    response.raise_for_status()
    return response.json()


def save_snapshot(data):
    SNAPSHOT_DIR.mkdir(exist_ok=True)
    now = datetime.now().isoformat().replace(":", "-").replace(".", "-")
    arquivo = SNAPSHOT_DIR / f"data_{now}.json"
    with open(arquivo, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def alert_rows(data, collected_at):
    for alert in data.get("alerts", []):
        location = alert.get("location", {})
        yield (
            alert.get("uuid"), alert.get("country"), alert.get("city"),
            alert.get("type"), alert.get("subtype"), alert.get("street"),
            alert.get("roadType"), alert.get("reportRating"), alert.get("reliability"),
            alert.get("confidence"), alert.get("magvar"), alert.get("reportDescription"),
            location.get("x"), location.get("y"), alert.get("pubMillis"), collected_at,
        )


def jam_rows(data, collected_at):
    for jam in data.get("jams", []):
        yield (
            jam.get("id"), jam.get("uuid"), jam.get("country"), jam.get("city"),
            jam.get("street"), jam.get("level"), jam.get("speedKMH"), jam.get("length"),
            jam.get("delay"), jam.get("roadType"), jam.get("turnType"),
            jam.get("blockingAlertUuid"), json.dumps(jam.get("line", []), ensure_ascii=False),
            jam.get("pubMillis"), collected_at,
        )


def save_to_db(data, collected_at):
    """Grava alertas e congestionamentos numa única transação: ou entra o
    snapshot inteiro, ou nada (sem snapshots pela metade)."""
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO alerts (
                        uuid, country, city, type, subtype, street, road_type,
                        report_rating, reliability, confidence, magvar,
                        report_description, location_x, location_y, pub_millis, collected_at
                    ) VALUES %s
                """, list(alert_rows(data, collected_at)))
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO jams (
                        id, uuid, country, city, street, level, speed_kmh, length,
                        delay, road_type, turn_type, blocking_alert_uuid,
                        line_json, pub_millis, collected_at
                    ) VALUES %s
                """, list(jam_rows(data, collected_at)))
    finally:
        conn.close()


def run_once():
    data = fetch_data()
    # collected_at em UTC, explícito (convenção do banco)
    collected_at = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    if SAVE_SNAPSHOTS:
        save_snapshot(data)
    save_to_db(data, collected_at)
    print(f"{collected_at}Z ok | jams: {len(data.get('jams', []))} | alerts: {len(data.get('alerts', []))}", flush=True)


def run_loop(intervalo_segundos=120):
    use_alarm = hasattr(signal, "SIGALRM")  # Linux/macOS; no Windows o limite fica só nos timeouts
    if use_alarm:
        signal.signal(signal.SIGALRM, _on_alarm)
    falhas = 0
    while True:
        inicio = time.monotonic()
        try:
            if use_alarm:
                signal.alarm(CYCLE_TIMEOUT_S)
            run_once()
            falhas = 0
        except Exception as e:
            falhas += 1
            print(f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S}Z ERRO na coleta ({falhas}ª seguida): {e}", flush=True)
        finally:
            if use_alarm:
                signal.alarm(0)
        # mantém o ritmo de 2 min; após falhas seguidas, tenta de novo mais cedo
        espera = 30 if falhas else intervalo_segundos - (time.monotonic() - inicio)
        time.sleep(max(5, espera))


if __name__ == "__main__":
    run_loop(120)
