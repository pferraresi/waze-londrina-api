"""
Conta, a partir do GTFS da CMTU, quantas viagens de ônibus param em cada rua
num dia útil típico, e quantas delas nos horários de pico (7h–9h, 12h–14h e
17h–19h, como no artigo). Gera data/onibus-por-rua.json, lido pela aba "Ruas
críticas" do painel.

Uso:
    python scripts/gtfs_onibus_por_rua.py gtfs.zip [AAAA-MM-DD]

A data (opcional) é o dia útil de referência; por padrão, a quarta-feira mais
próxima do meio do período de validade do arquivo.

O casamento com os nomes do Waze é feito pela chave de nome normalizada
(função chave(), repetida em JavaScript no index.html): sem acento, sem tipo
de via (Rua, Av., Rodovia…), sem títulos (Prof., Dr., Prefeito…) e sem
números de casa. Os nomes de parada com acento corrompido no arquivo da CMTU
("B¿lgica") são corrigidos quando outra parada tem o nome certo; senão o "¿"
fica na chave e o painel o trata como uma letra qualquer. O painel também
aceita nome mais curto no GTFS ("Ayrton Senna" ↔ "Ayrton Senna da Silva").
"""
import csv
import io
import json
import re
import sys
import unicodedata
import zipfile
from collections import defaultdict
from datetime import date, timedelta

TIPOS = {"rua", "r", "avenida", "av", "rodovia", "rod", "marginal", "marg", "travessa", "tv",
         "alameda", "al", "praca", "pc", "pca", "estrada", "estr", "viaduto", "via"}
TITULOS = {"prof", "professor", "profa", "professora", "dr", "doutor", "dra", "doutora",
           "pref", "prefeito", "pres", "presidente", "gov", "governador", "sen", "senador",
           "dep", "deputado", "eng", "engenheiro", "cel", "coronel", "gen", "general",
           "mal", "marechal", "pe", "padre", "fr", "frei", "ver", "vereador", "des",
           "desembargador", "min", "ministro", "cap", "capitao", "ten", "tenente", "maj",
           "major", "brig", "brigadeiro", "alm", "almirante", "d"}
LIGACOES = {"de", "da", "do", "das", "dos", "e"}
# horários de pico, os mesmos do artigo: 7h–9h, 12h–14h e 17h–19h (dias úteis)
PICO = [("07:00:00", "08:59:59"), ("12:00:00", "13:59:59"), ("17:00:00", "18:59:59")]


def no_pico(hora):
    hora = hora.strip().zfill(8)
    return any(a <= hora <= b for a, b in PICO)


def chave(nome):
    s = unicodedata.normalize("NFD", nome or "")
    s = "".join(c for c in s if unicodedata.category(c) != "Mn").lower()
    # número da casa no fim, mas não o número de rodovia ("PR 445", "BR-369")
    if not re.search(r"\b(pr|br|sp)[\s-]*\d+\s*$", s):
        s = re.sub(r"\s+\d+[a-z]?\s*$", "", s)
    s = re.sub(r"[^a-z0-9¿]+", " ", s)
    pal = s.split()
    while pal and pal[0] in TIPOS:
        pal = pal[1:]
    pal = [p for p in pal if p not in TITULOS and p not in LIGACOES]
    return " ".join(pal)


def ler(z, nome):
    return list(csv.DictReader(io.TextIOWrapper(z.open(nome), encoding="utf-8-sig")))


def servicos_do_dia(calendar, calendar_dates, dia):
    dias = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    d = dia.strftime("%Y%m%d")
    ativos = {c["service_id"] for c in calendar
              if c[dias[dia.weekday()]] == "1" and c["start_date"] <= d <= c["end_date"]}
    for e in calendar_dates:
        if e["date"] == d:
            (ativos.add if e["exception_type"] == "1" else ativos.discard)(e["service_id"])
    return ativos


def main(caminho, dia_ref=None):
    z = zipfile.ZipFile(caminho)
    calendar, cdates = ler(z, "calendar.txt"), ler(z, "calendar_dates.txt")
    ini = min(c["start_date"] for c in calendar)
    fim = max(c["end_date"] for c in calendar)
    if dia_ref:
        dia = date.fromisoformat(dia_ref)
    else:
        a = date(int(ini[:4]), int(ini[4:6]), int(ini[6:]))
        b = date(int(fim[:4]), int(fim[4:6]), int(fim[6:]))
        dia = a + (b - a) / 2
        while dia.weekday() != 2:                      # quarta-feira
            dia += timedelta(days=1)
    ativos = servicos_do_dia(calendar, cdates, dia)

    trips = {t["trip_id"]: t["route_id"] for t in ler(z, "trips.txt") if t["service_id"] in ativos}
    paradas = {s["stop_id"]: s["stop_name"] for s in ler(z, "stops.txt")}

    # acento corrompido ("B¿lgica"): usa o nome limpo equivalente, se houver
    limpas = {chave(n) for n in paradas.values() if "¿" not in n}
    corrige = {}
    for n in paradas.values():
        k = chave(n)
        if "¿" in k and k not in corrige:
            padrao = re.compile("^" + re.escape(k).replace("¿", "[a-z]{1,2}") + "$")
            achou = [c for c in limpas if padrao.match(c)]
            # sem equivalente limpo, o "¿" fica: o painel o trata como uma letra qualquer
            corrige[k] = achou[0] if len(achou) == 1 else k
    def chave_parada(sid):
        k = chave(paradas.get(sid, ""))
        return corrige.get(k, k)

    viagens = defaultdict(set)
    pico = defaultdict(set)
    linhas = defaultdict(set)
    nomes = defaultdict(lambda: defaultdict(int))
    with z.open("stop_times.txt") as f:
        for st in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
            rota = trips.get(st["trip_id"])
            if rota is None:
                continue
            k = chave_parada(st["stop_id"])
            if not k:
                continue
            viagens[k].add(st["trip_id"])
            if no_pico(st["departure_time"] or st["arrival_time"]):
                pico[k].add(st["trip_id"])
            linhas[k].add(rota)
            nomes[k][paradas[st["stop_id"]]] += 1

    ruas = {
        k: {"viagens": len(v), "pico": len(pico[k]), "linhas": len(linhas[k]),
            "nome_cmtu": max(nomes[k].items(), key=lambda x: x[1])[0]}
        for k, v in sorted(viagens.items(), key=lambda x: -len(x[1]))
    }
    out = {
        "fonte": "CMTU Londrina, GTFS (programação do transporte coletivo)",
        "validade_arquivo": f"{ini[:4]}-{ini[4:6]}-{ini[6:]} a {fim[:4]}-{fim[4:6]}-{fim[6:]}",
        "dia_util_referencia": dia.isoformat(),
        "metodo": "viagens programadas com pelo menos uma parada na rua, no dia de referência; "
                  "pico = viagens que param na rua entre 7h e 9h, 12h e 14h ou 17h e 19h",
        "ruas": ruas,
    }
    with open("data/onibus-por-rua.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))
    print(f"{len(ruas)} ruas, {len(trips)} viagens no dia {dia} (serviços {sorted(ativos)})")


if __name__ == "__main__":
    main(*sys.argv[1:3])
