# skail-video-dlq-na-pratica

Material de apoio do vídeo [**Dead letter queue: o que fazer com a mensagem que falhou**](https://www.youtube.com/watch?v=i-APZKUdjoM): os scripts, as tabelas, o importador e o runbook usados para zerar 1.240 mensagens de uma DLQ do Amazon SQS sem derrubar o ERP e sem criar pedido em dobro.

O cenário é o do vídeo. O portal B2B da Distribuidora Vale Norte manda pedidos pela fila `pedidos` do SQS, e um importador em C# cria cada pedido no Prumo ERP. Na quinta às 22:00, um deploy quebrou a leitura do desconto, e à 01:00 de sexta o ERP ficou 40 minutos em manutenção. Na sexta às 08:05 a `pedidos-dlq` tinha 1.240 mensagens.

| Destino | Mensagens |
|---|---|
| Importadas no ERP | 1.178 |
| Recusadas com aviso ao comprador (`pedido.recusado`) | 22 |
| Descartadas com registro (`dlq_descartes`) | 40 |

## O que tem aqui

| Pasta | Conteúdo |
|---|---|
| `sqs/` | Scripts em bash com AWS CLI, jq e psql, um por etapa da triagem |
| `sql/` | Tabelas (`dlq_ficha`, `dlq_lida`, `dlq_descartes`), o agrupamento e as regras de destino |
| `importador/` | O importador em C# com as três saídas (apaga, relança, manda direto para a DLQ), a ficha de diagnóstico e um exemplo de unit do systemd |
| `skail/` | O mesmo fluxo como function do skail, usado na parte de Time Travel Debug (veja `skail/README.md`) |
| `docs/` | `runbook.md` e `outras-filas.md` (RabbitMQ 4.4, Azure Service Bus, Kafka, Sidekiq, BullMQ, Oban) |
| `simulacao/` e `teste/` | Reproduzem o incidente num SQS local e rodam a triagem inteira; `simulacao/pedido-PV-48877.json` é o corpo que o importador manda ao ERP para o PV-48877, usado no `curl` da chave de idempotência |

## A triagem, passo a passo

Pré-requisitos:

- Linux ou WSL, com bash 4.4 ou mais novo e GNU coreutils. No macOS, instale `bash` e `coreutils` pelo Homebrew e ponha o diretório `gnubin` no PATH.
- AWS CLI v2. O v1 também roda os scripts, menos com atributo Binary, e está em modo de manutenção desde 15/07/2026 (fim do suporte em 15/07/2027).
- `jq`, `psql` e `bc`.

As credenciais e a região vêm do ambiente, como em qualquer uso do AWS CLI (`AWS_REGION`, `AWS_DEFAULT_REGION` ou a região do profile). As filas são standard.

```bash
git clone https://github.com/skaildev/skail-video-dlq-na-pratica && cd skail-video-dlq-na-pratica
chmod +x sqs/*.sh teste/testar.sh teste/bin/aws
export AWS_REGION=sa-east-1
export DATABASE_URL=postgresql://usuario:senha@host:5432/banco
psql "$DATABASE_URL" -f sql/01-tabelas.sql
export LOTE=$(date +%Y%m%d-%H%M)
```

| Etapa | Comando | O que faz |
|---|---|---|
| Filas | `./sqs/01-criar-filas.sh` | `pedidos` com redrive policy (5 recebimentos, visibility timeout de 30 s, 4 dias) e `pedidos-dlq` com 14 dias |
| Ler | `./sqs/02-ler-dlq.sh` | Lê a DLQ inteira sem apagar, grava em `dlq_lida` no lote `$LOTE` e devolve tudo para a fila |
| Agrupar | `psql "$DATABASE_URL" -v lote=$LOTE -f sql/02-grupos.sql` | Grupos por exceção, com versões e horários |
| Cancelados | `psql "$DATABASE_URL" -c "\copy portal_cancelados from 'cancelados.csv' with (format csv, header)"` | Importa a exportação do portal (uma coluna, `pedido`) |
| Decidir | `psql "$DATABASE_URL" -v lote=$LOTE -f sql/03-destinos.sql` | Grava o destino de cada mensagem (edite as regras antes) |
| Separar | `./sqs/03-triagem.sh $LOTE` | Move cada grupo para uma fila própria; na DLQ fica o que pode voltar agora |
| Redrive | `./sqs/04-redrive.sh 10` | `start-message-move-task` para a `pedidos`, até 10 mensagens por segundo |
| Reenviar | `./sqs/05-reenviar.sh pedidos-dlq-alteracoes 10` | Devolve uma fila de grupo para `pedidos`, até 10 por segundo |
| Descartar | `./sqs/06-descartar.sh pedidos-dlq-cancelados "cancelado no portal" "comercial"` | Grava em `dlq_descartes` e só depois apaga |
| Alarmes | `./sqs/07-alarmes.sh arn:aws:sns:...` | Mensagens visíveis > 0, e DLQ uma hora sem esvaziar |

A ordem entre separar e redrive é de propósito. O redrive nativo do SQS move a fila inteira, sem filtro por motivo, e só aceita como origem uma fila que é DLQ de outra. Por isso a triagem tira da DLQ tudo que não pode voltar agora, e as filas de grupo voltam pelo `05-reenviar.sh`. As alterações só voltam depois que as criações entraram no ERP: espere a `pedidos` esvaziar antes de devolver a `pedidos-dlq-alteracoes`.

O `03-triagem.sh` só sai com código 0 quando na DLQ ficou exatamente o que tem destino vazio no lote. Com qualquer outro código (mensagem fora do lote, envio que falhou, leitura incompleta), não rode o redrive: leia e separe de novo com um lote novo. O `04-redrive.sh` passa o destino explícito (`--destination-arn` da `pedidos`), porque sem ele o SQS não sabe para onde devolver as mensagens que o importador manda direto para a DLQ, e a tarefa falha com `CouldNotDetermineMessageSource`.

O redrive, o `03-triagem.sh` e o `05-reenviar.sh` criam mensagens novas, com outro MessageId. A triagem guarda o MessageId da DLQ no atributo `dlq.message_id`, e o `06-descartar.sh` grava esse id em `dlq_descartes`, para o descarte continuar ligado à ficha. O SQS aceita até 10 atributos por mensagem: o importador usa 2 (`erro.tipo` e `erro.mensagem`) e a triagem mais 2, então sobram 6 para o portal.

## Rodar o incidente na sua máquina

Sobe um SQS local (moto) e um Postgres, recria as 1.240 mensagens com as fichas e roda todas as etapas até a DLQ zerada, conferindo a contagem de cada etapa. Leva de 14 a 18 minutos: o AWS CLI faz uma chamada por lote de 10 mensagens, o redrive anda a 10 por segundo e cada leitura termina com uma espera de 20 s na fila vazia.

```bash
docker compose up -d --wait
python3 -m venv .venv && .venv/bin/pip install boto3
chmod +x sqs/*.sh teste/testar.sh teste/bin/aws
PATH="$PWD/.venv/bin:$PATH" ./teste/testar.sh
```

O moto não implementa a tarefa de redrive (`start-message-move-task`), então o teste usa `teste/bin/aws`, que repassa tudo para o AWS CLI e simula só essa chamada. O moto também não devolve o atributo `DeadLetterQueueSourceArn`, então a coluna `fila_origem` da `dlq_lida` fica vazia no teste. O `simulacao/popular.py` regrava o `simulacao/cancelados.csv` a cada execução, sempre com os mesmos 40 pedidos. Nunca rode o `popular.py` numa conta de verdade: ele recusa sem `AWS_ENDPOINT_URL`.

## O importador

`importador/Prumo.Importador` é um Worker Service em .NET 8 com AWSSDK.SQS e Npgsql. O que importa está em `ProcessadorDePedidos.cs`:

- sucesso, ou pedido que o ERP já tinha (chave de idempotência com o número do pedido): apaga a mensagem;
- crédito recusado: publica `pedido.recusado` para o portal e apaga;
- 503 ou timeout: grava a ficha e relança, sem apagar; o SQS entrega de novo;
- JSON quebrado ou dado que o ERP recusa: manda direto para a DLQ com o motivo nos atributos, apaga da origem e grava a ficha.

A ficha (`FichaDeDiagnostico.cs`) grava o que o SQS não anota: exceção, mensagem, tentativas, primeira falha e versão. A chave é o `MessageId`. Quando a redrive policy move a mensagem para a DLQ, o MessageId continua o mesmo (a AWS mostra isso no post de junho de 2023 sobre as APIs de redrive); se o pedido voltar e falhar de novo, a ficha abre outra linha, e o histórico se junta pelo número do pedido.

O log sai em uma linha por evento (`01:02:00 warn PV-48213 receive=1 ...`, formatador em `FormatoDeLinha.cs`). Em produção, o `importador/prumo-importador.service` grava o stdout em `/var/log/prumo-importador/importador.log`.

## A versão no skail

`skail/Prumo.Importacao` é o mesmo fluxo como function durável: `ImportarPedido` chama os commands `ValidarPedido`, `CriarPedidoNoErp` e `AvisarPortal`. No vídeo, uma execução que falhou no `CriarPedidoNoErp` é aberta no Monitor e depurada com Time Travel Debug. O passo a passo está em [Depurar com Time Travel Debug](https://docs.skail.dev/construa/como-fazer/depurar-com-time-travel-debug).

Transparência: o autor do vídeo trabalha na skail.

