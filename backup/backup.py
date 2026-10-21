"""
Backup diário do banco do Observatório para um bucket do Railway.

Roda como serviço agendado (cron) no Railway: faz o backup, apaga os antigos
e termina. Para cada tabela do schema public grava um CSV comprimido com
COPY (formato nativo do Postgres, lido de volta com COPY FROM), mais um
schema.sql com a estrutura das tabelas e um manifest.json com contagens.

Estrutura no bucket:
    backups/AAAA-MM-DD/schema.sql
    backups/AAAA-MM-DD/<tabela>.csv.gz
    backups/AAAA-MM-DD/manifest.json      (escrito por último = backup completo)

Retenção: últimos 7 dias + domingos dos últimos 28 dias.

Variáveis de ambiente: DATABASE_URL, BUCKET, ACCESS_KEY_ID,
SECRET_ACCESS_KEY, ENDPOINT, REGION.
"""
import gzip
import json
import os
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone

import boto3
import psycopg2

PREFIX = "backups/"
KEEP_DAILY_DAYS = 7
KEEP_SUNDAY_DAYS = 28


def log(msg):
    print(f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S}Z {msg}", flush=True)


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["ENDPOINT"],
        aws_access_key_id=os.environ["ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["SECRET_ACCESS_KEY"],
        region_name=os.environ.get("REGION", "auto"),
    )


def table_ddl(cur, table):
    """CREATE TABLE simples a partir do catálogo (tipos, NOT NULL, default)."""
    cur.execute("""
        SELECT column_name, data_type, is_nullable, column_default,
               character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
    """, (table,))
    cols = []
    for name, dtype, nullable, default, maxlen in cur.fetchall():
        t = dtype
        if dtype == "character varying" and maxlen:
            t = f"varchar({maxlen})"
        if default and default.startswith("nextval("):
            t = "bigserial" if dtype == "bigint" else "serial"
            default = None
        piece = f'    "{name}" {t}'
        if default:
            piece += f" DEFAULT {default}"
        if nullable == "NO" and "serial" not in t:
            piece += " NOT NULL"
        cols.append(piece)
    cur.execute("""
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
        WHERE tc.table_schema = 'public' AND tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY'
        ORDER BY kcu.ordinal_position
    """, (table,))
    pk = [r[0] for r in cur.fetchall()]
    if pk:
        cols.append("    PRIMARY KEY (" + ", ".join(f'"{c}"' for c in pk) + ")")
    return f'CREATE TABLE IF NOT EXISTS "{table}" (\n' + ",\n".join(cols) + "\n);\n"


def run_backup():
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    base = f"{PREFIX}{day}/"
    s3 = s3_client()
    bucket = os.environ["BUCKET"]
    t0 = time.monotonic()

    conn = psycopg2.connect(os.environ["DATABASE_URL"], connect_timeout=15)
    # leitura consistente: todas as tabelas no mesmo instante
    conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
    manifest = {"started_utc": datetime.now(timezone.utc).isoformat(), "tables": {}}
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT version()")
            manifest["postgres"] = cur.fetchone()[0]
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)
            tables = [r[0] for r in cur.fetchall()]

            schema = "".join(table_ddl(cur, t) for t in tables)
            s3.put_object(Bucket=bucket, Key=base + "schema.sql", Body=schema.encode("utf-8"))

            for t in tables:
                cur.execute(f'SELECT COUNT(*) FROM "{t}"')
                rows = cur.fetchone()[0]
                with tempfile.NamedTemporaryFile(suffix=".csv.gz") as tmp:
                    with gzip.open(tmp.name, "wb", compresslevel=6) as gz:
                        cur.copy_expert(f'COPY "{t}" TO STDOUT WITH (FORMAT csv, HEADER true)', gz)
                    size = os.path.getsize(tmp.name)
                    s3.upload_file(tmp.name, bucket, f"{base}{t}.csv.gz")
                manifest["tables"][t] = {"rows": rows, "gzip_bytes": size}
                log(f"{t}: {rows} linhas, {size / 1e6:.1f} MB comprimido")
    finally:
        conn.close()

    manifest["finished_utc"] = datetime.now(timezone.utc).isoformat()
    manifest["seconds"] = round(time.monotonic() - t0, 1)
    s3.put_object(Bucket=bucket, Key=base + "manifest.json",
                  Body=json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"))
    total = sum(v["gzip_bytes"] for v in manifest["tables"].values())
    log(f"backup {day} completo: {total / 1e6:.1f} MB em {manifest['seconds']} s")
    return s3, bucket


def prune(s3, bucket):
    """Apaga backups fora da política de retenção."""
    today = datetime.now(timezone.utc).date()
    days = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=PREFIX, Delimiter="/"):
        for p in page.get("CommonPrefixes", []):
            days.add(p["Prefix"][len(PREFIX):].rstrip("/"))
    for d in sorted(days):
        try:
            dt = datetime.strptime(d, "%Y-%m-%d").date()
        except ValueError:
            continue
        age = (today - dt).days
        keep = age < KEEP_DAILY_DAYS or (dt.weekday() == 6 and age < KEEP_SUNDAY_DAYS)
        if keep:
            continue
        keys = []
        for page in paginator.paginate(Bucket=bucket, Prefix=f"{PREFIX}{d}/"):
            keys += [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})
            log(f"backup antigo removido: {d}")


if __name__ == "__main__":
    try:
        s3, bucket = run_backup()
        prune(s3, bucket)
    except Exception as e:
        log(f"ERRO no backup: {e}")
        sys.exit(1)
