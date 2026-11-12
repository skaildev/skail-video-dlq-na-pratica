#!/usr/bin/env bash
# Descarta com registro: grava cada mensagem em dlq_descartes e só depois apaga da fila.
#   ./sqs/06-descartar.sh <fila> "<motivo>" "<quem decidiu>"
# Exemplo: ./sqs/06-descartar.sh pedidos-dlq-cancelados "cancelado no portal" "comercial"
# Variáveis: LOTE (padrão descarte-AAAAMMDD-HHMM), ESCONDER em segundos (padrão 3600, de 300 a 43200).
#
# Só apaga quando todas as mensagens lidas estão gravadas em dlq_descartes no lote. Se não bater,
# nada é apagado e as mensagens voltam para a fila. Rodar de novo com o mesmo LOTE é seguro:
# o que já foi gravado não duplica, e a conferência olha só as mensagens desta leitura.
set -euo pipefail
source "$(dirname "$0")/comum.sh"

fila=${1:?informe a fila}
motivo=${2:?informe o motivo}
quem=${3:?informe quem decidiu}
LOTE=${LOTE:-descarte-$(date +%Y%m%d-%H%M)}

url=$(url_da_fila "$fila")
tmp=$(mktemp -d)
trap 'if [ -s "$tmp/recibos.tsv" ]; then devolver "$url" "$tmp/recibos.tsv" >/dev/null || true; fi; rm -rf "$tmp"' EXIT
: > "$tmp/linhas.jsonl"; : > "$tmp/recibos.tsv"
vazias=0
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
  vazias=0
  # message_id: o da mensagem na DLQ, que a triagem guarda no atributo dlq.message_id. Liga o descarte à ficha.
  jq -c '.Messages[] | {message_id: (.MessageAttributes["dlq.message_id"].StringValue // .MessageId),
                        pedido: .MessageAttributes.pedido.StringValue,
                        evento: .MessageAttributes.evento.StringValue,
                        payload: (.Body as $b | try ($b | fromjson) catch {bruto: $b}),
                        payload_bruto: .Body,
                        atributos: .MessageAttributes}' <<<"$resp" >> "$tmp/linhas.jsonl"
  jq -r '.Messages[] | [.MessageId, .ReceiptHandle] | @tsv' <<<"$resp" >> "$tmp/recibos.tsv"
done
# Para apagar vale o recibo mais recente de cada mensagem.
tac "$tmp/recibos.tsv" | sort -s -u -k1,1 > "$tmp/unicos.tsv"; : > "$tmp/recibos.tsv"
lidas=$(wc -l < "$tmp/unicos.tsv")
echo "lidas: $lidas"
if [ "$lidas" -eq 0 ]; then exit 0; fi
# Duas cópias da mesma mensagem da DLQ (triada duas vezes) têm o mesmo dlq.message_id e viram uma linha só.
ids=$(jq -r .message_id "$tmp/linhas.jsonl" | sort -u | wc -l)
trap 'if [ -s "$tmp/unicos.tsv" ]; then devolver "$url" "$tmp/unicos.tsv" >/dev/null || true; fi; rm -rf "$tmp"' EXIT

gravadas=$(sql -v lote="$LOTE" -v fila="$fila" -v motivo="$motivo" -v quem="$quem" <<SQL
create temp table entrada (linha jsonb);
\copy entrada from '$tmp/linhas.jsonl' with (format csv, quote e'\x01', delimiter e'\x02')
insert into dlq_descartes (lote, fila, message_id, pedido, evento, motivo, decidido_por, payload, payload_bruto, atributos)
select distinct on (linha->>'message_id')
       :'lote', :'fila', linha->>'message_id', linha->>'pedido', linha->>'evento', :'motivo', :'quem',
       linha->'payload', linha->>'payload_bruto', linha->'atributos'
from entrada
on conflict (lote, message_id) do nothing;
select count(distinct d.message_id)
from dlq_descartes d
join entrada e on d.message_id = e.linha->>'message_id'
where d.lote = :'lote';
SQL
)
echo "gravadas em dlq_descartes (lote $LOTE): $gravadas"

if [ "$gravadas" -ne "$ids" ]; then
  echo "gravadas ($gravadas) diferente das mensagens lidas ($ids): nada foi apagado" >&2
  exit 1
fi
apagadas=$(apagar "$url" "$tmp/unicos.tsv"); : > "$tmp/unicos.tsv"
echo "apagadas de $fila: $apagadas"
if [ "$apagadas" -ne "$lidas" ]; then
  echo "apagadas ($apagadas) diferente de lidas ($lidas): rode de novo com LOTE=$LOTE" >&2
  exit 1
fi
