#!/usr/bin/env bash
# Cria os dois alarmes da DLQ no CloudWatch.
#   ./sqs/07-alarmes.sh <arn-do-topico-sns> [fila]     (padrão: $DLQ)
#
# 1. Mensagens visíveis > 0: avisa quando cai mensagem na DLQ.
# 2. Sem triagem há 1 hora: a DLQ passou uma hora inteira sem ficar vazia (mínimo de mensagens visíveis > 0
#    em todos os 60 minutos). Mede o atraso da triagem sem depender da ApproximateAgeOfOldestMessage, que numa
#    DLQ conta desde a chegada na DLQ e, pela documentação, deixa de fora mensagem recebida três vezes
#    ou mais sem ser apagada.
# Use ApproximateNumberOfMessagesVisible: a NumberOfMessagesSent não conta as mensagens que o SQS move para a DLQ.
# Fila sem atividade por mais de 6 horas para de publicar métrica, e a primeira métrica depois disso pode atrasar
# até 15 minutos. TreatMissingData=notBreaching evita alarme por falta de dado.
set -euo pipefail
source "$(dirname "$0")/comum.sh"

topico=${1:?informe o ARN do tópico SNS que recebe o alarme}
fila=${2:-$DLQ}
MINUTOS=${MINUTOS:-60}

aws cloudwatch put-metric-alarm --alarm-name "$fila-mensagens-visiveis" \
  --alarm-description "Mensagem na DLQ $fila: abrir a triagem (runbook em docs/runbook.md)" \
  --namespace AWS/SQS --metric-name ApproximateNumberOfMessagesVisible \
  --dimensions Name=QueueName,Value="$fila" --statistic Maximum --period 60 \
  --evaluation-periods 1 --threshold 0 --comparison-operator GreaterThanThreshold \
  --treat-missing-data notBreaching --alarm-actions "$topico"

aws cloudwatch put-metric-alarm --alarm-name "$fila-sem-triagem" \
  --alarm-description "DLQ $fila há $MINUTOS min sem esvaziar: a triagem está atrasada" \
  --namespace AWS/SQS --metric-name ApproximateNumberOfMessagesVisible \
  --dimensions Name=QueueName,Value="$fila" --statistic Minimum --period 60 \
  --evaluation-periods "$MINUTOS" --datapoints-to-alarm "$MINUTOS" --threshold 0 \
  --comparison-operator GreaterThanThreshold \
  --treat-missing-data notBreaching --alarm-actions "$topico"

aws cloudwatch describe-alarms --alarm-name-prefix "$fila-" --output json \
  | jq '.MetricAlarms[] | {AlarmName, MetricName, Statistic, Threshold, EvaluationPeriods, ComparisonOperator, StateValue}'
