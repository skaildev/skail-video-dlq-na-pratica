#!/usr/bin/env bash
# Devolve a DLQ para a fila de pedidos com vazão controlada (redrive nativo do SQS, StartMessageMoveTask).
#   ./sqs/04-redrive.sh [por-segundo] [fila]     (padrão: 10 por segundo, $DLQ; máximo 500)
# Variável: INTERVALO em segundos entre as consultas de andamento (padrão 10).
#
# O redrive move a fila inteira, sem filtro. Rode o 03-triagem.sh antes, para que na DLQ fique só o que pode voltar agora.
# O destino vai explícito (--destination-arn da $FILA). Sem ele, o SQS devolve cada mensagem para a fila de onde veio,
# mas a tarefa falha com CouldNotDetermineMessageSource se houver mensagem que entrou na DLQ por SendMessage,
# como as falhas de dado que o importador manda direto para a DLQ.
# Só funciona com uma fila que é DLQ de outra fila, e só uma tarefa por vez em cada fila.
set -euo pipefail
source "$(dirname "$0")/comum.sh"

POR_SEGUNDO=${1:-10}
fila=${2:-$DLQ}
[[ $POR_SEGUNDO =~ ^[1-9][0-9]*$ ]] && [ "$POR_SEGUNDO" -le 500 ] \
  || { echo "por-segundo precisa ser inteiro de 1 a 500" >&2; exit 1; }

url=$(url_da_fila "$fila")
arn=$(arn_da_fila "$url")
destino_arn=$(arn_da_fila "$(url_da_fila "$FILA")")
echo "$fila -> $FILA: $(visiveis "$url") mensagens | até $POR_SEGUNDO por segundo"

aws sqs start-message-move-task --source-arn "$arn" --destination-arn "$destino_arn" \
  --max-number-of-messages-per-second "$POR_SEGUNDO" --output json

while :; do
  sleep "${INTERVALO:-10}"
  # A tarefa roda na AWS; se a consulta falhar, o script só tenta de novo.
  r=$(aws sqs list-message-move-tasks --source-arn "$arn" --max-results 1 --output json | jq -c '.Results[0] // empty') || r=""
  [ -n "$r" ] || { echo "sem status da tarefa; tentando de novo" >&2; continue; }
  status=$(jq -r '.Status' <<<"$r")
  printf '%s  %-9s movidas %s de %s\n' "$(date +%H:%M:%S)" "$status" \
    "$(jq -r '.ApproximateNumberOfMessagesMoved' <<<"$r")" "$(jq -r '.ApproximateNumberOfMessagesToMove // "?"' <<<"$r")"
  case "$status" in
    RUNNING|CANCELLING) ;;
    COMPLETED) break ;;
    *) jq . <<<"$r"; exit 1 ;;
  esac
done
echo "$fila: $(visiveis "$url") mensagens"
