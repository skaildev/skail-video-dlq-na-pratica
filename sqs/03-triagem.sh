#!/usr/bin/env bash
# Separa a DLQ por grupo: cada mensagem com destino em dlq_lida vai para a fila do grupo.
#   ./sqs/03-triagem.sh <lote> [fila]       (padrão: $DLQ)
# Antes: sqs/02-ler-dlq.sh e sql/03-destinos.sql no mesmo lote.
#
# O redrive do SQS move a fila inteira, sem filtro. Por isso a separação vem antes:
# o que fica na DLQ é só o que pode voltar agora, pelo sqs/04-redrive.sh.
# Cada mensagem é enviada para a fila do grupo e só então apagada da DLQ.
#
# Saída 0: na DLQ ficou só o que tem destino vazio no lote. Pode rodar o 04-redrive.sh.
# Qualquer outra saída: sobrou mensagem sem triagem (fora do lote, envio que falhou, leitura incompleta ou erro).
#          Não rode o 04-redrive.sh: rode o 02 e o 03 de novo com um lote novo.
# Variável: ESCONDER em segundos (padrão 3600, de 300 a 43200).
set -euo pipefail
source "$(dirname "$0")/comum.sh"

LOTE=${1:?informe o lote lido pelo 02-ler-dlq.sh}
fila=${2:-$DLQ}

url=$(url_da_fila "$fila")
declare -A destino urls movidas
linhas=$(sql -v lote="$LOTE" <<'SQL'
select message_id || '|' || coalesce(destino, '') from dlq_lida where lote = :'lote';
SQL
)
while IFS='|' read -r id d; do [ -n "$id" ] && destino[$id]=$d; done <<<"$linhas"
[ ${#destino[@]} -gt 0 ] || { echo "lote $LOTE vazio ou inexistente; rode o 02-ler-dlq.sh antes" >&2; exit 1; }
com_destino=0; for d in "${destino[@]}"; do [ -n "$d" ] && com_destino=$((com_destino + 1)); done
[ $com_destino -gt 0 ] || { echo "nenhum destino no lote $LOTE; rode sql/03-destinos.sql antes" >&2; exit 1; }

tmp=$(mktemp -d)
# Em qualquer saída, o que foi lido e não foi movido volta para a DLQ na hora.
trap 'cat "$tmp/ficam.tsv" "$tmp/fora.tsv" "$tmp/falharam.tsv" "$tmp/lote.tsv" > "$tmp/voltam.tsv" 2>/dev/null || true
      devolver "$url" "$tmp/voltam.tsv" >/dev/null || true
      rm -rf "$tmp"' EXIT
: > "$tmp/ficam.tsv"; : > "$tmp/fora.tsv"; : > "$tmp/falharam.tsv"; : > "$tmp/lote.tsv"
vazias=0; incompleta=0
while :; do
  dentro_do_prazo "$ESCONDER" || exit 1
  resp=$(receber "$url" "$ESCONDER")
  n=$(jq '.Messages | length' <<<"$resp")
  if [ "$n" -eq 0 ]; then
    vazias=$((vazias + 1))
    if fila_lida "$url"; then break; fi
    if [ $vazias -ge 3 ]; then echo "três respostas vazias seguidas, mas o SQS ainda conta mensagens visíveis" >&2; incompleta=1; break; fi
    continue
  fi
  vazias=0
  # Recibos do lote em andamento: se o script parar no meio dele, o trap devolve essas mensagens na hora.
  jq -r '.Messages[] | [.MessageId, .ReceiptHandle] | @tsv' <<<"$resp" > "$tmp/lote.tsv"

  # Separa o que chegou: fora do lote, destino vazio (fica na DLQ) e destino preenchido (vai para a fila do grupo).
  declare -A grupo=()
  while IFS=$'\t' read -r i id rh; do
    if [ -z "${destino[$id]+x}" ]; then printf '%s\t%s\n' "$id" "$rh" >> "$tmp/fora.tsv"; continue; fi
    d=${destino[$id]}
    if [ -z "$d" ]; then printf '%s\t%s\n' "$id" "$rh" >> "$tmp/ficam.tsv"; continue; fi
    grupo[$d]="${grupo[$d]:-} $i"
  done < <(jq -r '.Messages | to_entries[] | [.key, .value.MessageId, .value.ReceiptHandle] | @tsv' <<<"$resp")

  for d in "${!grupo[@]}"; do
    urls[$d]=${urls[$d]:-$(url_ou_criar "$d")}
    idx=$(jq -cn --arg s "${grupo[$d]}" '$s | split(" ") | map(select(length > 0) | tonumber)')
    # Dois atributos marcam a origem: o lote e o MessageId na DLQ, que liga o descarte à ficha e a dlq_lida.
    # O SQS aceita até 10 atributos por mensagem; se já houver mais de 8, a mensagem segue sem eles.
    jq -c --argjson idx "$idx" --arg lote "$LOTE" '[.Messages | to_entries[] | select(.key as $k | $idx | index($k)) | {
        Id: ("m" + (.key | tostring)),
        MessageBody: .value.Body,
        MessageAttributes: ((.value.MessageAttributes // {}) as $a
          | if ($a | length) <= 8
            then $a + {"dlq.lote":       {DataType: "String", StringValue: $lote},
                       "dlq.message_id": {DataType: "String", StringValue: .value.MessageId}}
            else $a end)
      }]' <<<"$resp" > "$tmp/envio.json"
    enviados=$(enviar "${urls[$d]}" "$tmp/envio.json")
    # Apaga da DLQ só o que o SQS confirmou no destino. O resto volta para a DLQ no fim.
    jq -r --argjson idx "$idx" --arg ok "$enviados" '($ok | split("\n")) as $okl
        | .Messages | to_entries[] | select(.key as $k | $idx | index($k))
        | [(if (("m" + (.key | tostring)) as $m | $okl | index($m)) then "apagar" else "falharam" end),
           .value.MessageId, .value.ReceiptHandle] | @tsv' <<<"$resp" \
      | while IFS=$'\t' read -r para id rh; do printf '%s\t%s\n' "$id" "$rh" >> "$tmp/$para.tsv"; done
    pedidas=$( [ -f "$tmp/apagar.tsv" ] && wc -l < "$tmp/apagar.tsv" || echo 0 )
    ok=$(apagar "$url" "$tmp/apagar.tsv"); rm -f "$tmp/apagar.tsv"
    if [ "$ok" -ne "$pedidas" ]; then
      echo "$d: $pedidas enviadas, mas só $ok apagadas da DLQ; as outras ficam nas duas filas, confira antes do redrive" >&2
      exit 2
    fi
    movidas[$d]=$(( ${movidas[$d]:-0} + ok ))
  done
  unset grupo
  : > "$tmp/lote.tsv"
done

ficam=$(sort -u -k1,1 "$tmp/ficam.tsv" | wc -l)
fora=$(sort -u -k1,1 "$tmp/fora.tsv" | wc -l)
falharam=$(sort -u -k1,1 "$tmp/falharam.tsv" | wc -l)
for arq in ficam fora falharam; do devolver "$url" "$tmp/$arq.tsv" >/dev/null; done

echo "lote $LOTE | $fila"
total=0
for d in pedidos-dlq-alteracoes pedidos-dlq-codigo pedidos-dlq-cadastro pedidos-dlq-credito pedidos-dlq-cancelados "${!movidas[@]}"; do
  [ -n "${movidas[$d]:-}" ] || continue
  printf '  %-26s %5d\n' "$d" "${movidas[$d]}"
  total=$((total + movidas[$d])); unset "movidas[$d]"
done
echo "movidas: $total | ficaram em $fila: $ficam"
read -r vis esc < <(contagem "$url")
echo "$fila no SQS agora: $vis visíveis, $esc escondidas"

if [ $((fora + falharam)) -gt 0 ]; then
  echo "$fora fora do lote $LOTE, $falharam envios falharam: voltaram para a DLQ." >&2
  echo "Não rode o 04-redrive.sh: rode o 02 e o 03 de novo com um lote novo." >&2
  exit 2
fi
# A contagem do SQS é aproximada; se ela atrasar, o erro cai do lado seguro.
if [ $incompleta -eq 1 ] || [ $((vis + esc)) -gt "$ficam" ]; then
  echo "a $fila tem $((vis + esc)) mensagens e a triagem deixou $ficam: sobrou mensagem sem triagem." >&2
  echo "Não rode o 04-redrive.sh: rode o 02 e o 03 de novo com um lote novo." >&2
  exit 2
fi
