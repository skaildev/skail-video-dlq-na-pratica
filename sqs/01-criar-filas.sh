#!/usr/bin/env bash
# Cria (ou ajusta) a fila de pedidos e a DLQ. Filas standard.
#   ./sqs/01-criar-filas.sh
# Variáveis: FILA, DLQ, MAX_RECEBIMENTOS (padrão 5), VISIBILIDADE em segundos (padrão 30),
#            RETENCAO_FILA em segundos (padrão 345600, 4 dias).
# Rodar de novo só ajusta os atributos. Atenção: se a fila de pedidos já tiver retenção maior que RETENCAO_FILA,
# as mensagens mais velhas que o novo prazo expiram.
set -euo pipefail
source "$(dirname "$0")/comum.sh"

MAX_RECEBIMENTOS=${MAX_RECEBIMENTOS:-5}
VISIBILIDADE=${VISIBILIDADE:-30}
RETENCAO_FILA=${RETENCAO_FILA:-345600}   # 4 dias, o padrão do SQS

atrib=$(mktemp -d); trap 'rm -rf "$atrib"' EXIT

# Na fila standard, o prazo de retenção conta desde a entrada na fila de origem, também depois que a mensagem
# vai para a DLQ (na FIFO ele recomeça na DLQ). Com a mesma retenção nas duas, a mensagem pode sumir da DLQ
# antes de alguém olhar. Por isso 14 dias na DLQ.
dlq_url=$(url_ou_criar "$DLQ")
aws sqs set-queue-attributes --queue-url "$dlq_url" --attributes MessageRetentionPeriod=$RETENCAO_DLQ
dlq_arn=$(arn_da_fila "$dlq_url")

fila_url=$(aws sqs get-queue-url --queue-name "$FILA" --query QueueUrl --output text 2>/dev/null \
           || aws sqs create-queue --queue-name "$FILA" --query QueueUrl --output text)
fila_arn=$(arn_da_fila "$fila_url")

redrive=$(jq -cn --arg arn "$dlq_arn" --argjson max "$MAX_RECEBIMENTOS" '{deadLetterTargetArn: $arn, maxReceiveCount: $max}')
jq -cn --arg r "$redrive" --arg v "$VISIBILIDADE" --arg ret "$RETENCAO_FILA" \
  '{RedrivePolicy: $r, VisibilityTimeout: $v, MessageRetentionPeriod: $ret}' > "$atrib/fila.json"
aws sqs set-queue-attributes --queue-url "$fila_url" --attributes "file://$atrib/fila.json"

# Só a fila de pedidos pode usar esta DLQ.
permite=$(jq -cn --arg arn "$fila_arn" '{redrivePermission: "byQueue", sourceQueueArns: [$arn]}')
jq -cn --arg p "$permite" '{RedriveAllowPolicy: $p}' > "$atrib/dlq.json"
aws sqs set-queue-attributes --queue-url "$dlq_url" --attributes "file://$atrib/dlq.json"

for url in "$fila_url" "$dlq_url"; do
  aws sqs get-queue-attributes --queue-url "$url" \
    --attribute-names VisibilityTimeout MessageRetentionPeriod RedrivePolicy --output json \
    | jq --arg url "$url" '{fila: ($url | split("/") | last)} + .Attributes
        | if .RedrivePolicy then .RedrivePolicy |= fromjson else . end'
done
