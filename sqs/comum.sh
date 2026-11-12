# Funções usadas pelos scripts desta pasta. Não execute direto: os scripts fazem "source" deste arquivo.
#
# Requisitos: bash 4.4 ou mais novo e GNU coreutils (Linux ou WSL; no macOS, bash e coreutils do Homebrew),
# AWS CLI v2 (o v1 roda, mas não copia atributo Binary), jq, psql e bc.
#
# Variáveis de ambiente:
#   AWS_REGION        região das filas (padrão: AWS_DEFAULT_REGION ou a região do profile)
#   AWS_ENDPOINT_URL  opcional; aponta o AWS CLI para um SQS local (moto, LocalStack)
#   DATABASE_URL      conexão do Postgres onde ficam dlq_ficha, dlq_lida e dlq_descartes
#   FILA, DLQ         nomes das filas (padrão pedidos e pedidos-dlq)
#   ESCONDER          segundos que a leitura esconde cada mensagem no 02, no 03 e no 06 (padrão 3600, de 300 a 43200)

if (( BASH_VERSINFO[0] < 4 || (BASH_VERSINFO[0] == 4 && BASH_VERSINFO[1] < 4) )); then
  echo "estes scripts precisam do bash 4.4 ou mais novo (este é o $BASH_VERSION)" >&2; exit 1
fi
# Sem isto o bash ignora o set -e dentro de $(...), e uma chamada que falha vira "fila vazia".
shopt -s inherit_errexit

for prog in aws jq psql bc; do
  command -v "$prog" >/dev/null || { echo "falta o comando $prog no PATH" >&2; exit 1; }
done

AWS_REGION=${AWS_REGION:-${AWS_DEFAULT_REGION:-$(command aws configure get region 2>/dev/null || true)}}
export AWS_REGION=${AWS_REGION:?defina AWS_REGION com a região das filas}
export AWS_DEFAULT_REGION=$AWS_REGION
export AWS_PAGER=""
export AWS_CLI_FILE_ENCODING=UTF-8   # no Windows o padrão é cp1252, que estraga acento nos arquivos file://
FILA=${FILA:-pedidos}
DLQ=${DLQ:-pedidos-dlq}
RETENCAO_DLQ=1209600   # 14 dias, o máximo do SQS
ESCONDER=${ESCONDER:-3600}
[[ $ESCONDER =~ ^[0-9]+$ ]] && [ "$ESCONDER" -ge 300 ] && [ "$ESCONDER" -le 43200 ] \
  || { echo "ESCONDER precisa ser de 300 a 43200 segundos" >&2; exit 1; }

# O AWS CLI v2 lê AWS_ENDPOINT_URL sozinho; o v1 precisa do parâmetro.
if [ -n "${AWS_ENDPOINT_URL:-}" ]; then
  aws() { command aws --endpoint-url "$AWS_ENDPOINT_URL" "$@"; }
fi

# O receive-message entrega atributo Binary em base64. O v2 manda de volta certo com --cli-binary-format base64;
# o v1 manda o texto base64 como se fossem os bytes, e o valor chega corrompido.
if command aws --version 2>&1 | grep -q '^aws-cli/1\.'; then
  AWS_V1=1; BINARIO=()
else
  AWS_V1=; BINARIO=(--cli-binary-format base64)
fi

url_da_fila() {
  aws sqs get-queue-url --queue-name "$1" --query QueueUrl --output text
}

# Devolve a URL e cria a fila se ela não existir, com retenção de 14 dias.
url_ou_criar() {
  aws sqs get-queue-url --queue-name "$1" --query QueueUrl --output text 2>/dev/null \
    || aws sqs create-queue --queue-name "$1" --attributes MessageRetentionPeriod=$RETENCAO_DLQ \
         --query QueueUrl --output text
}

arn_da_fila() {
  aws sqs get-queue-attributes --queue-url "$1" --attribute-names QueueArn \
    --query Attributes.QueueArn --output text
}

# Mensagens visíveis, segundo o SQS (o número é aproximado).
visiveis() {
  aws sqs get-queue-attributes --queue-url "$1" --attribute-names ApproximateNumberOfMessages \
    --query Attributes.ApproximateNumberOfMessages --output text
}

# Visíveis e escondidas (em processamento), no formato "visíveis escondidas".
contagem() {
  aws sqs get-queue-attributes --queue-url "$1" \
    --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible --output json \
    | jq -r '.Attributes | "\(.ApproximateNumberOfMessages) \(.ApproximateNumberOfMessagesNotVisible)"'
}

# psql sem enfeite, parando no primeiro erro. Aceita -v nome=valor e lê o SQL do stdin ou de -f.
sql() {
  psql "${DATABASE_URL:?defina DATABASE_URL}" -X -q -A -t -v ON_ERROR_STOP=1 "$@"
}

# Recebe até 10 mensagens, escondendo cada uma por $2 segundos. Saída em JSON, com {"Messages": []} quando vier vazio.
# Long polling de 20 s: reduz as respostas vazias com mensagem na fila, mas não zera. Quem chama confere a contagem.
receber() {
  local r
  r=$(aws sqs receive-message --queue-url "$1" --max-number-of-messages "${3:-10}" \
        --visibility-timeout "$2" --wait-time-seconds 20 \
        --message-system-attribute-names All --message-attribute-names All --output json) \
    || { echo "receive-message falhou em $1" >&2; return 1; }
  [ -n "$r" ] || r='{}'
  jq -c '{Messages: (.Messages // [])}' <<<"$r"
}

# A leitura terminou? Só com resposta vazia e a fila sem mensagem visível, segundo o SQS.
fila_lida() {
  local v
  v=$(visiveis "$1") || return 2
  [ "$v" -eq 0 ]
}

# Para antes de as primeiras mensagens lidas voltarem a ficar visíveis, o que faria o laço ler as mesmas de novo.
dentro_do_prazo() {
  local esconder=$1
  if [ "$SECONDS" -ge $((esconder - 60)) ]; then
    echo "a leitura passou de $((esconder - 60)) s: rode de novo com ESCONDER maior (máximo 43200)" >&2
    return 1
  fi
}

# Envia as mensagens do arquivo $2 (lista JSON de entradas do send-message-batch, até 10) para a fila $1.
# Imprime o Id de cada mensagem que o SQS confirmou, uma por linha.
enviar() {
  local url=$1 arq=$2 bytes
  if [ -n "$AWS_V1" ] && jq -e 'any(.[].MessageAttributes[]?; .DataType | startswith("Binary"))' "$arq" >/dev/null; then
    echo "mensagem com atributo Binary: use o AWS CLI v2 (o v1 corrompe o valor)" >&2; return 1
  fi
  # O lote inteiro tem limite de 1 MiB. Acima de 900 KB, uma mensagem por chamada.
  bytes=$(jq '[.[] | (.MessageBody | utf8bytelength)
                     + ([(.MessageAttributes // {}) | to_entries[] | (.key | utf8bytelength) + (.value | tostring | utf8bytelength)] | add // 0)]
              | add // 0' "$arq")
  # Se a chamada falhar, as mensagens contam como envio que falhou: quem chama devolve e sai com erro.
  if [ "$bytes" -le 921600 ]; then
    aws sqs send-message-batch "${BINARIO[@]}" --queue-url "$url" --entries "file://$arq" --output json \
      | jq -r '(.Successful // [])[].Id' \
      || echo "send-message-batch falhou: o lote conta como envio que falhou" >&2
  else
    local i n
    n=$(jq length "$arq")
    for ((i = 0; i < n; i++)); do
      jq -c ".[$i:$i+1]" "$arq" > "$arq.um"
      aws sqs send-message-batch "${BINARIO[@]}" --queue-url "$url" --entries "file://$arq.um" --output json \
        | jq -r '(.Successful // [])[].Id' \
        || echo "send-message-batch falhou na mensagem $i: conta como envio que falhou" >&2
    done
    rm -f "$arq.um"
  fi
}

# Devolve à fila, na hora, as mensagens cujos recibos estão no arquivo $2 (uma linha "id<TAB>recibo" por mensagem).
# Imprime quantas o SQS confirmou e esvazia o arquivo, para que uma segunda chamada não faça nada.
devolver() {
  local url=$1 arq=$2 devolvidas=0 ok parte
  [ -s "$arq" ] || { echo 0; return 0; }
  rm -f "$arq".parte.*
  split -l 10 "$arq" "$arq.parte."
  for parte in "$arq".parte.*; do
    jq -R -s -c '[split("\n")[] | select(length > 0) | split("\t") | {ReceiptHandle: .[1], VisibilityTimeout: 0}]
                  | to_entries | map(.value + {Id: ("m" + (.key|tostring))})' "$parte" > "$parte.json"
    ok=$(aws sqs change-message-visibility-batch --queue-url "$url" --entries "file://$parte.json" --output json \
           | jq '(.Successful // []) | length') || return 1
    devolvidas=$((devolvidas + ok))
    rm -f "$parte" "$parte.json"
  done
  : > "$arq"
  echo "$devolvidas"
}

# Apaga as mensagens cujos recibos estão no arquivo $2, no mesmo formato de devolver(). Imprime quantas o SQS confirmou.
apagar() {
  local url=$1 arq=$2 apagadas=0 ok parte
  [ -s "$arq" ] || { echo 0; return 0; }
  rm -f "$arq".parte.*
  split -l 10 "$arq" "$arq.parte."
  for parte in "$arq".parte.*; do
    jq -R -s -c '[split("\n")[] | select(length > 0) | split("\t") | {ReceiptHandle: .[1]}]
                  | to_entries | map(.value + {Id: ("m" + (.key|tostring))})' "$parte" > "$parte.json"
    ok=$(aws sqs delete-message-batch --queue-url "$url" --entries "file://$parte.json" --output json \
           | jq '(.Successful // []) | length') || return 1
    apagadas=$((apagadas + ok))
    rm -f "$parte" "$parte.json"
  done
  echo "$apagadas"
}
