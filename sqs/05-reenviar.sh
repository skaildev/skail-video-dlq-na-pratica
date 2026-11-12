#!/usr/bin/env bash
# Reenvia as mensagens de uma fila de grupo para a fila de pedidos, com vazão controlada.
#   ./sqs/05-reenviar.sh <fila> [por-segundo] [destino]     (padrão: até 10 por segundo, $FILA)
#
# Use para as filas criadas pelo 03-triagem.sh, que não são DLQ de nenhuma fila e por isso
# não aceitam o redrive nativo. Cada mensagem é enviada ao destino e só então apagada da origem.
# O por-segundo é um teto: o script nunca passa dele, mas pode ficar abaixo, porque faz uma chamada por lote de 10.
# Ordem: as alterações (pedidos-dlq-alteracoes) só depois que as criações entraram no ERP.
# A mensagem chega ao destino como mensagem nova, com outro MessageId.
set -euo pipefail
source "$(dirname "$0")/comum.sh"

origem=${1:?informe a fila de origem}
POR_SEGUNDO=${2:-10}
destino=${3:-$FILA}
[[ $POR_SEGUNDO =~ ^[1-9][0-9]*$ ]] || { echo "por-segundo precisa ser inteiro, 1 ou mais" >&2; exit 1; }

url=$(url_da_fila "$origem")
url_destino=$(url_da_fila "$destino")
echo "$origem -> $destino: $(visiveis "$url") mensagens | até $POR_SEGUNDO por segundo"

tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
lote=$(( POR_SEGUNDO < 10 ? POR_SEGUNDO : 10 ))
enviadas=0; vazias=0; inicio=$(date +%s.%N)
while :; do
  resp=$(receber "$url" 120 "$lote")
  n=$(jq '.Messages | length' <<<"$resp")
  if [ "$n" -eq 0 ]; then
    vazias=$((vazias + 1))
    if fila_lida "$url"; then break; fi
    if [ $vazias -ge 3 ]; then echo; echo "três respostas vazias seguidas, mas o SQS ainda conta mensagens visíveis: confira a contagem" >&2; break; fi
    continue
  fi
  vazias=0

  # Os atributos dlq.* da triagem não seguem para a fila de pedidos.
  jq -c '[.Messages | to_entries[] | {Id: ("m" + (.key | tostring)), MessageBody: .value.Body,
           MessageAttributes: ((.value.MessageAttributes // {}) | with_entries(select(.key | startswith("dlq.") | not)))}]' \
     <<<"$resp" > "$tmp/envio.json"
  ok_ids=$(enviar "$url_destino" "$tmp/envio.json")
  # Apaga da origem só o que o SQS confirmou no destino; o resto volta sozinho depois de 120 s.
  jq -r --arg ok "$ok_ids" '($ok | split("\n")) as $okl | .Messages | to_entries[]
      | select(("m" + (.key | tostring)) as $m | $okl | index($m))
      | [.value.MessageId, .value.ReceiptHandle] | @tsv' <<<"$resp" > "$tmp/apagar.tsv"
  pedidas=$(wc -l < "$tmp/apagar.tsv"); ok=$(apagar "$url" "$tmp/apagar.tsv"); enviadas=$((enviadas + ok))
  if [ "$ok" -ne "$pedidas" ]; then
    echo; echo "$((pedidas - ok)) enviadas e não apagadas de $origem: voltariam em 120 s e iriam de novo; confira antes de rodar outra vez" >&2
    exit 2
  fi

  # Segura o ritmo: no máximo POR_SEGUNDO mensagens por segundo desde o início.
  alvo=$(echo "$inicio + $enviadas / $POR_SEGUNDO" | bc -l)
  espera=$(echo "$alvo - $(date +%s.%N)" | bc -l)
  if [ "$(echo "$espera > 0" | bc)" = 1 ]; then sleep "$espera"; fi
  printf '\renviadas: %d' "$enviadas"
done
echo
read -r vis esc < <(contagem "$url")
echo "$origem: $vis visíveis, $esc escondidas | $destino recebeu $enviadas"
[ "$esc" -eq 0 ] || echo "as escondidas tiveram envio recusado e voltam para $origem em até 120 s: rode de novo" >&2
