"""
Restaura um backup do bucket num banco Postgres.

Uso (com as mesmas variáveis do bucket definidas):
    python restore.py                      # lista os backups disponíveis
    python restore.py 2026-10-21 DESTINO   # restaura esse dia no banco DESTINO

DESTINO é uma URL postgresql://... . Por segurança, a restauração só roda
num banco em que as tabelas ainda não existem ou estão vazias: o normal é
restaurar num banco NOVO (por exemplo, um segundo Postgres no Railway) e,
depois de conferir, apontar a API e o coletor para ele.

Também funciona com os arquivos baixados à mão: passe a pasta local em vez
da data (python restore.py ./2026-10-21 DESTINO).
"""
import gzip
import json
import os
import sys
import tempfile

import psycopg2

PREFIX = "backups/"


def s3_client():
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=os.environ["ENDPOINT"],
        aws_access_key_id=os.environ["ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["SECRET_ACCESS_KEY"],
        region_name=os.environ.get("REGION", "auto"),
    )


def list_backups():
    s3 = s3_client()
    bucket = os.environ["BUCKET"]
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=PREFIX, Delimiter="/")
    for page in pages:
        for p in page.get("CommonPrefixes", []):
            day = p["Prefix"][len(PREFIX):].rstrip("/")
            try:
                m = json.loads(s3.get_object(Bucket=bucket, Key=f"{p['Prefix']}manifest.json")["Body"].read())
                info = ", ".join(f"{t}: {v['rows']} linhas" for t, v in m["tables"].items())
                print(f"{day}  completo  ({info})")
            except Exception:
                print(f"{day}  INCOMPLETO (sem manifest.json)")


def fetch(source, name, dest_dir):
    """Devolve o caminho local de um arquivo do backup (bucket ou pasta)."""
    if os.path.isdir(source):
        return os.path.join(source, name)
    s3 = s3_client()
    path = os.path.join(dest_dir, name)
    s3.download_file(os.environ["BUCKET"], f"{PREFIX}{source}/{name}", path)
    return path


def restore(source, target_url):
    with tempfile.TemporaryDirectory() as tmp:
        manifest = json.load(open(fetch(source, "manifest.json", tmp)))
        schema = open(fetch(source, "schema.sql", tmp), encoding="utf-8").read()
        conn = psycopg2.connect(target_url)
        try:
            with conn, conn.cursor() as cur:
                cur.execute(schema)
                for table, info in manifest["tables"].items():
                    cur.execute(f'SELECT COUNT(*) FROM "{table}"')
                    if cur.fetchone()[0]:
                        raise SystemExit(f'A tabela "{table}" já tem dados no destino. Use um banco vazio.')
                    path = fetch(source, f"{table}.csv.gz", tmp)
                    with gzip.open(path, "rb") as gz:
                        cur.copy_expert(f'COPY "{table}" FROM STDIN WITH (FORMAT csv, HEADER true)', gz)
                    cur.execute(f'SELECT COUNT(*) FROM "{table}"')
                    got = cur.fetchone()[0]
                    status = "ok" if got == info["rows"] else f"ATENÇÃO: esperado {info['rows']}"
                    print(f"{table}: {got} linhas restauradas ({status})")
                    # acerta os contadores das colunas serial
                    cur.execute("""
                        SELECT column_name, pg_get_serial_sequence(%s, column_name)
                        FROM information_schema.columns
                        WHERE table_schema = 'public' AND table_name = %s
                          AND pg_get_serial_sequence(%s, column_name) IS NOT NULL
                    """, (table, table, table))
                    for col, seq in cur.fetchall():
                        cur.execute(f'SELECT setval(%s, COALESCE(MAX("{col}"), 1)) FROM "{table}"', (seq,))
        finally:
            conn.close()
    print("Restauração concluída.")


if __name__ == "__main__":
    if len(sys.argv) == 1:
        list_backups()
    elif len(sys.argv) == 3:
        restore(sys.argv[1], sys.argv[2])
    else:
        print(__doc__)
        sys.exit(1)
