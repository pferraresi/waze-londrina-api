# Dashboard v2: como publicar

## O que muda

| Arquivo | O que é |
|---|---|
| `public_sql.py` | Consultas dos endpoints públicos. Regras de exclusão no topo do arquivo. |
| `public_api.py` | Endpoints `/public/*` (FastAPI), com cache em memória. |
| `api.py` | +8 linhas: registra o `public_api`. Endpoints antigos continuam iguais. |
| `index.html` | Novo dashboard público (abas Agora, Quando sair, Ruas críticas, Chuva, Sobre os dados). |
| `v1/index.html` | O dashboard antigo, cópia idêntica, publicado em `/v1/`. |

Nenhuma tabela nova, nenhuma mudança no coletor (`main.py`).

## Passo a passo

1. Copie os arquivos para o repositório (incluindo a pasta `v1/`) e faça commit no `main`. O `index.html` antigo vira `v1/index.html`, sem nenhuma alteração.
2. O Railway (serviço `web`) faz o deploy sozinho a partir do `main`.
3. Teste: abra `https://web-production-b07b0.up.railway.app/public/now` e `/docs`.
4. O GitHub Pages publica o `index.html` novo em seguida.

## Opcional: índices

As consultas usam `collected_at`. Para deixá-las mais rápidas, defina a variável
`PUBLIC_API_CREATE_INDEXES=1` no serviço `web` do Railway. A API cria 3 índices na
inicialização (ocupam por volta de 100–200 MB no volume de 5 GB do Postgres).

## Endpoints

| Rota | Para quê |
|---|---|
| `/public/now` | Situação no último snapshot + comparação com o normal (mesmo dia/hora, 4 semanas) |
| `/public/map` | GeoJSON do último snapshot: trechos + alertas |
| `/public/hourly?hours=168` | Série horária normalizada por snapshot (até 2160 h), com alertas de alagamento |
| `/public/typical-week?days=28` | Dia da semana × hora |
| `/public/streets?days=7` | Horas/dia em trânsito pesado por via (ISO 37120 Ind. 17.2) |
| `/public/alerts?days=7` | Alertas por subtipo (uuid distinto), ruas com mais buracos e alagamentos |
| `/public/interdicoes` | Bloqueios longos ativos |
| `/public/meta` | Regras de exclusão em vigor |

## Princípio das métricas

O coletor grava o feed inteiro a cada ~2 min, então cada linha de `jams` é
"um congestionamento visto num snapshot". Os endpoints antigos somam linhas
(um congestionamento de 1 h vira ~30). Os novos usam médias por snapshot:
km congestionados, trechos ativos e fração do tempo em N4/N5.

## Testar localmente com dados simulados

`index.html?api=http://localhost:8000` aponta o dashboard para outra API;
`&meteo=<url>` troca a fonte de chuva.

## Atualizar a chuva do IAPAR

A aba Chuva lê `data/chuva-iapar.json`, gerado a partir da planilha horária que o
IAPAR envia. Quando chegar uma série nova:

    python scripts/iapar_para_json.py caminho/da/planilha.xlsx

Faça commit do `data/chuva-iapar.json` atualizado. O script converte o rótulo do
IAPAR (fim da hora) para o início da hora, como na API.
