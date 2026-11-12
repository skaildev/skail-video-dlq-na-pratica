#!/usr/bin/env bash
# Lê a DLQ inteira sem apagar nada e grava cada mensagem em dlq_lida, num lote.
#   ./sqs/02-ler-dlq.sh [fila]            (padrão: $DLQ)
# Variáveis: LOTE (padrão AAAAMMDD-HHMM), ESCONDER em segundos (padrão 3600, de 300 a 43200).
# Se a leitura passar de ESCONDER - 60 s, o script para e pede um ESCONDER maior.
#
# No SQS não existe "espiar": ler esconde a mensagem pelo visibility timeout.
# O script esconde cada mensagem por ESCONDER segundos para não ler a mesma duas vezes
# e, no fim, devolve todas para a fila na hora (visibilidade 0), também quando para no meio por erro.
set -euo pipefail
source "$(dirname "$0")/comum.sh"

fila=${1:-$DLQ}
LOTE=${LOTE:-$(date +%Y%m%d-%H%M)}

url=$(url_da_fila "$fila")
echo "$fila: $(visiveis "$url") mensagens visíveis | lote $LOTE"

tmp=$(mktemp -d)
trap 'if [ -s "$tmp/recibos.tsv" ]; then
        echo "parou no meio: devolvendo as mensagens lidas (as que não voltarem agora voltam sozinhas em $ESCONDER s)" >&2
        devolver "$url" "$tmp/recibos.tsv" >/dev/null || true
      fi
      rm -rf "$tmp"' EXIT
: > "$tmp/linhas.jsonl"; : > "$tmp/recibos.tsv"
lidas=0; vazias=0
while :; do
  dentro_do_prazo "$ESCONDER" || exit 1
  resp=$(receber "$url" "$ESCONDER")
  n=$(jq '.Messages | length' <<<"$resp")
  if [ "$n" -eq 0 ]; then
    vazias=$((vazias + 1))
    if fila_lida "$url"; then break; fi
    if [ $vazias -ge 3 ]; then echo "três respostas vazias seguidas, mas o SQS ainda conta mensagens visíveis: confira a contagem" >&2; break; fi
    continue
  fi
  vazias=0; lidas=$((lidas + n))
  # Corpo que não é JSON fica guardado como texto em {"bruto": ...}; corpo_bruto guarda o texto exato sempre.
  jq -c --arg lote "$LOTE" --arg fila "$fila" '.Messages[] | {
      lote: $lote, fila: $fila, message_id: .MessageId,
      recebimentos: (.Attributes.ApproximateReceiveCount | tonumber),
      enviada_em: (.Attributes.SentTimestamp | tonumber / 1000 | todate),
      fila_origem: .Attributes.DeadLetterQueueSourceArn,
      pedido: .MessageAttributes.pedido.StringValue,
      evento: .MessageAttributes.evento.StringValue,
      corpo: (.Body as $b | try ($b | fromjson) catch {bruto: $b}),
      corpo_bruto: .Body,
      atributos: .MessageAttributes
    }' <<<"$resp" >> "$tmp/linhas.jsonl"
  jq -r '.Messages[] | [.MessageId, .ReceiptHandle] | @tsv' <<<"$resp" >> "$tmp/recibos.tsv"
  if [ $((lidas % 200)) -lt 10 ]; then echo "lidas: $lidas"; fi
done
echo "lidas: $lidas (total)"

# Grava tudo de uma vez. O CSV com delimitador e aspas que nunca aparecem preserva o JSON como está.
gravadas=$(sql -v lote="$LOTE" <<SQL
create temp table entrada (linha jsonb);
\copy entrada from '$tmp/linhas.jsonl' with (format csv, quote e'\x01', delimiter e'\x02')
with novas as (
  insert into dlq_lida (lote, message_id, fila, recebimentos, enviada_em, fila_origem, pedido, evento,
                        corpo, corpo_bruto, atributos)
  select linha->>'lote', linha->>'message_id', linha->>'fila', (linha->>'recebimentos')::int,
         (linha->>'enviada_em')::timestamptz, linha->>'fila_origem', linha->>'pedido', linha->>'evento',
         linha->'corpo', linha->>'corpo_bruto', linha->'atributos'
  from entrada
  on conflict (lote, message_id) do nothing
  returning 1)
select count(*) from novas;
SQL
)
echo "gravadas em dlq_lida: $gravadas (lote $LOTE)"

unicas=$(sort -u -k1,1 "$tmp/recibos.tsv" | wc -l)
devolvidas=$(devolver "$url" "$tmp/recibos.tsv")
echo "apagadas: 0 | mensagens lidas: $unicas | recibos devolvidos à fila: $devolvidas"
echo "próximo passo: psql \"\$DATABASE_URL\" -v lote=$LOTE -f sql/02-grupos.sql"
