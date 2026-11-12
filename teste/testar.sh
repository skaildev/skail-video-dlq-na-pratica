#!/usr/bin/env bash
# Roda o incidente inteiro contra um SQS local (moto) e um Postgres de teste, do zero até a DLQ zerada.
#   docker compose up -d --wait        (sobe moto na 5005 e Postgres na 5433)
#   ./teste/testar.sh
# O moto não implementa a tarefa de redrive; teste/bin/aws a simula com teste/mover.py.
set -euo pipefail
cd "$(dirname "$0")/.."
export AWS_ENDPOINT_URL=${AWS_ENDPOINT_URL:-http://localhost:5005}
export AWS_ACCESS_KEY_ID=testing AWS_SECRET_ACCESS_KEY=testing AWS_REGION=sa-east-1 AWS_DEFAULT_REGION=sa-east-1
export DATABASE_URL=${DATABASE_URL:-postgresql://postgres:postgres@localhost:5433/dlq}
export PATH="$PWD/teste/bin:$PATH" INTERVALO=2
LOTE=20261016-0830
saida=$(mktemp -d); trap 'rm -rf "$saida"' EXIT

passo() { printf '\n== %s\n' "$*"; }
falhas=0
confere() {   # confere <descrição> <obtido> <esperado>
  if [ "$2" = "$3" ]; then printf '  ok     %-48s %s\n' "$1" "$2"
  else printf '  FALHOU %-48s %s (esperado %s)\n' "$1" "$2" "$3"; falhas=$((falhas + 1)); fi
}

passo "zera o SQS local e as tabelas"
curl -sf -X POST "$AWS_ENDPOINT_URL/moto-api/reset" >/dev/null
psql "$DATABASE_URL" -X -q -v ON_ERROR_STOP=1 -c 'drop table if exists dlq_ficha, dlq_lida, portal_cancelados, dlq_descartes'
psql "$DATABASE_URL" -X -q -f sql/01-tabelas.sql

passo "01 cria as filas"
./sqs/01-criar-filas.sh

passo "simula a noite de quinta"
python3 simulacao/popular.py
psql "$DATABASE_URL" -X -q -v ON_ERROR_STOP=1 -c "\\copy portal_cancelados from 'simulacao/cancelados.csv' with (format csv, header)"

passo "02 lê a DLQ sem apagar"
LOTE=$LOTE ./sqs/02-ler-dlq.sh pedidos-dlq
psql "$DATABASE_URL" -X -v lote=$LOTE -f sql/02-grupos.sql

passo "03 decide os destinos"
psql "$DATABASE_URL" -X -q -v lote=$LOTE -f sql/03-destinos.sql

passo "03 separa a DLQ por grupo"
./sqs/03-triagem.sh $LOTE | tee "$saida/triagem.txt"

passo "04 redrive das criações a 10 por segundo"
./sqs/04-redrive.sh 10

source sqs/comum.sh
fila() { contagem "$(url_da_fila "$1")" | awk '{print $1 + $2}'; }
movidas() { awk -v f="$1" '$1 == f {print $2}' "$saida/triagem.txt"; }
# No teste, ninguém consome a fila de pedidos: conta o que chegou e esvazia, para conferir cada etapa.
esvaziar() { local n; n=$(fila pedidos); aws sqs purge-queue --queue-url "$(url_da_fila pedidos)"; echo "$n"; }
redrive=$(esvaziar)

passo "05 devolve as alterações, depois o código, o cadastro e o crédito"
./sqs/05-reenviar.sh pedidos-dlq-alteracoes 10
./sqs/05-reenviar.sh pedidos-dlq-codigo 10
./sqs/05-reenviar.sh pedidos-dlq-cadastro 10
./sqs/05-reenviar.sh pedidos-dlq-credito 10
reenvio=$(esvaziar)

passo "06 descarta os cancelados com registro"
./sqs/06-descartar.sh pedidos-dlq-cancelados "cancelado no portal" "comercial"

passo "07 alarmes"
aws sns create-topic --name dlq-alertas --output text >/dev/null
./sqs/07-alarmes.sh "arn:aws:sns:sa-east-1:111122223333:dlq-alertas"

passo "confere"
confere "triagem: pedidos-dlq-alteracoes"  "$(movidas pedidos-dlq-alteracoes)" 31
confere "triagem: pedidos-dlq-codigo"      "$(movidas pedidos-dlq-codigo)" 260
confere "triagem: pedidos-dlq-cadastro"    "$(movidas pedidos-dlq-cadastro)" 85
confere "triagem: pedidos-dlq-credito"     "$(movidas pedidos-dlq-credito)" 25
confere "triagem: pedidos-dlq-cancelados"  "$(movidas pedidos-dlq-cancelados)" 40
confere "redrive: chegaram em pedidos"     "$redrive" 799
confere "reenvio: chegaram em pedidos"     "$reenvio" 401
for f in pedidos-dlq pedidos-dlq-alteracoes pedidos-dlq-codigo pedidos-dlq-cadastro pedidos-dlq-credito pedidos-dlq-cancelados; do
  confere "$f: visíveis + escondidas" "$(fila "$f")" 0
done
confere "dlq_descartes"                              "$(sql -c 'select count(*) from dlq_descartes')" 40
confere "descartes que não são cancelados no portal" "$(sql -c 'select count(*) from dlq_descartes where pedido not in (select pedido from portal_cancelados)')" 0
confere "descartes ligados à leitura da DLQ"         "$(sql -c 'select count(*) from dlq_descartes d join dlq_lida l using (message_id)')" 40
confere "descartes com ficha"                        "$(sql -c 'select count(*) from dlq_descartes d join dlq_ficha f using (message_id)')" 40
[ $falhas -eq 0 ] && echo "ok: DLQ zerada, 1.240 mensagens com destino conferido" || { echo "FALHOU ($falhas)"; exit 1; }
