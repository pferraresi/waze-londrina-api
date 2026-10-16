"""
Converte a planilha horária do IAPAR (como o IAPAR envia) no arquivo estático
data/chuva-iapar.json, lido pela aba Chuva do dashboard.

Uso:  python scripts/iapar_para_json.py caminho/da/planilha.xlsx

Convenções (ver notas do projeto):
- O rótulo da hora do IAPAR marca o FIM do intervalo: o registro de 08:00 é a
  chuva entre 7h e 8h. No JSON, cada valor é indexado pelo INÍCIO da hora,
  igual à série horária da API (/public/hourly), em horário de Brasília.
- Precipitação em mm, resolução 0,2 mm.
"""
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import openpyxl


def main(src, out):
    ws = openpyxl.load_workbook(src, read_only=True, data_only=True).worksheets[0]
    rows = list(ws.iter_rows(values_only=True))
    head = [str(c).strip().lower() if c else "" for c in rows[0]]
    col = {name: head.index(name) for name in ("hora", "ano", "mes", "dia", "latitude", "longitude", "altitude")}
    pcol = next(i for i, h in enumerate(head) if h.startswith("precipitacao"))

    series = {}
    meta = None
    for r in rows[1:]:
        if r[0] is None:
            continue
        h = r[col["hora"]]
        hour = h.hour if hasattr(h, "hour") else int(str(h).split(":")[0])
        end = datetime(int(r[col["ano"]]), int(r[col["mes"]]), int(r[col["dia"]]), hour)
        start = end - timedelta(hours=1)
        v = r[pcol]
        mm = None if v in (None, "") else float(str(v).replace(",", "."))
        series[start] = mm
        if meta is None:
            meta = {
                "lat": int(r[col["latitude"]]) / 1e6,
                "lon": int(r[col["longitude"]]) / 1e6,
                "altitude_m": float(str(r[col["altitude"]]).replace(",", ".")),
            }

    first, last = min(series), max(series)
    values, t = [], first
    while t <= last:
        mm = series.get(t)
        values.append(None if mm is None else round(mm, 1))
        t += timedelta(hours=1)

    data = {
        "fonte": "IAPAR – estação Londrina (pluviômetro)",
        **meta,
        "fuso": "America/Sao_Paulo",
        "inicio": first.strftime("%Y-%m-%dT%H:00"),
        "passo": "1h (valor = chuva da hora que começa no horário indicado)",
        "unidade": "mm",
        "gerado_em": datetime.now().strftime("%Y-%m-%d"),
        "mm": values,
    }
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    Path(out).write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    total = sum(v for v in values if v)
    print(f"{len(values)} horas, {first:%d/%m/%Y %H:00} a {last:%d/%m/%Y %H:00}, total {total:.1f} mm → {out}")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else "data/chuva-iapar.json")
