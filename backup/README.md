# Backup do banco

Serviço `backup` no Railway (pasta `backup/`, roda todo dia às 03:00 de Brasília).
Grava uma cópia completa do Postgres no bucket `backups` do mesmo projeto.

## O que fica guardado

```
backups/AAAA-MM-DD/schema.sql        estrutura das tabelas
backups/AAAA-MM-DD/alerts.csv.gz     tabela alerts (CSV comprimido)
backups/AAAA-MM-DD/jams.csv.gz       tabela jams (CSV comprimido)
backups/AAAA-MM-DD/manifest.json     contagem de linhas; só existe se o backup terminou
```

Retenção: os últimos 7 dias, mais os domingos dos últimos 28 dias.

Os CSV abrem direto em Python/pandas (`pd.read_csv("jams.csv.gz")`), R ou planilha,
sem precisar restaurar o banco.

## Cópia mensal fora do Railway

No painel do Railway, clique no bucket **backups** → **Files** → abra a pasta do
dia mais recente e baixe os quatro arquivos. Guarde no Google Drive ou num
computador da UEL.

## Restaurar

1. Crie um Postgres novo e vazio (por exemplo, um segundo Postgres no Railway).
2. Rode, com as variáveis do bucket definidas:
   ```
   python backup/restore.py                         # lista os backups
   python backup/restore.py 2026-10-21 postgresql://...destino...
   ```
   Também aceita uma pasta local com os arquivos baixados à mão.
3. Confira as contagens que o script imprime e aponte `DATABASE_URL` da API e do
   coletor para o banco novo.

O script se recusa a escrever num banco que já tenha dados, para não misturar.
