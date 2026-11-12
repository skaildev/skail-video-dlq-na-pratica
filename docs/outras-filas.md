# A mesma triagem em outras filas

O repositório roda no Amazon SQS. Os passos são os mesmos em qualquer broker: o que muda é quem move a mensagem para a DLQ, o que ela leva junto, como ler sem apagar e como devolver com vazão. Conferido em novembro de 2026.

## RabbitMQ 4.4 (quorum queues)

**Configuração.** Policy na fila de origem com `dead-letter-exchange` e `delivery-limit`. Desde a 4.0 a quorum queue vem com `delivery-limit` 20. O limite conta as entregas que falharam, e a mensagem sai na primeira falha acima dele: com 4, são 5 entregas, como o Maximum receives 5 do SQS. Até a 4.2, mudar o `delivery-limit` por policy não alterava uma fila que já existia; desde a 4.3 a fila passa a seguir a policy.

```bash
rabbitmqctl set_policy pedidos-dlx "^pedidos$" \
  '{"dead-letter-exchange":"pedidos.dlx","delivery-limit":4}' --apply-to quorum_queues
```

A policy não cria a exchange. Crie antes a exchange `pedidos.dlx` do tipo fanout e a fila `pedidos-dlq`, com um binding entre as duas (pela interface de gerenciamento ou pelo `rabbitmqadmin`). A fanout resolve porque a mensagem chega na DLX com a routing key original. Se a exchange não existir na hora do dead lettering, a mensagem é descartada sem aviso.

**O que a mensagem leva.** O cabeçalho `x-death` traz fila, motivo (`rejected`, `expired`, `maxlen`, `delivery_limit`), horário e quantas vezes a mensagem foi para a DLQ por aquele motivo, além de `x-first-death-*` e `x-last-death-*`. O número de tentativas fica no `x-delivery-count`, que a quorum queue põe nas reentregas da fila de origem. A exceção do seu código não vem: a ficha de diagnóstico continua necessária.

**Mandar direto para a DLQ.** `basic.reject` ou `basic.nack` com `requeue=false` manda para a dead letter exchange com motivo `rejected`.

**Ler sem apagar.** Na interface de gerenciamento, Get messages com Ack mode "Nack message requeue true". Num script, consuma com prefetch maior que a DLQ, grave tudo e devolva no fim com `basic.nack` com `requeue=true` e `multiple=true`. Desde a 4.3 o nack não conta como entrega falha, e um canal fechado com mensagem pendente conta. Até a 4.2 o nack contava, e cada leitura gastava uma das 20 entregas da própria DLQ.

**Cuidado com a própria DLQ.** Se ela for quorum queue, tem o mesmo `delivery-limit` 20. Sem dead letter exchange configurada nela, a mensagem que passar do limite é descartada.

**Devolver com vazão.** Shovel dinâmico da `pedidos-dlq` para a `pedidos` (Admin, Shovel Management). `src-delete-after: queue-length` move só o que estava lá na criação. O `src-prefetch-count` (padrão 1000) limita mensagens em trânsito; não existe limite por segundo. O shovel move a fila inteira, sem filtro: separe os grupos antes, como no `03-triagem.sh`.

**Alarme.** `rabbitmq_queue_messages_ready{queue="pedidos-dlq"}` maior que zero. O `/metrics` padrão soma todas as filas e não tem o label `queue`: faça o scrape de `/metrics/per-object`, ou use `/metrics/detailed?family=queue_coarse_metrics&queue=^pedidos-dlq$`, onde a métrica se chama `rabbitmq_detailed_queue_messages_ready`.

**Retenção.** Sem TTL, a mensagem fica até alguém consumir.

## Azure Service Bus

**Configuração.** Toda fila e toda assinatura já tem a subfila `$deadletterqueue`. `MaxDeliveryCount` vem com 10.

**O que a mensagem leva.** As propriedades `DeadLetterReason` e `DeadLetterErrorDescription`. Por limite de entregas, o motivo é `MaxDeliveryCountExceeded`.

**Mandar direto para a DLQ.** `ServiceBusReceiver.DeadLetterMessageAsync(mensagem, motivo, descrição)`. O motivo e a descrição são seus, e aqui o broker carrega a causa junto.

**Ler sem apagar.** `PeekMessagesAsync` num receiver com `SubQueue = SubQueue.DeadLetter`, ou o Service Bus Explorer do portal em modo peek.

**Devolver.** No Service Bus Explorer, você marca na tela as mensagens (o peek mostra até 100 por vez), sem filtro por motivo. Re-send selected messages envia uma cópia e a original continua na DLQ; apague depois de conferir. Reenviar da DLQ de uma assinatura manda a cópia para o tópico, e todas as assinaturas recebem. Para controlar vazão, um script com o SDK que recebe da DLQ, envia para a fila e completa a mensagem.

**Alarme.** Métrica Dead-lettered Messages no Azure Monitor, com a dimensão da entidade.

**Retenção.** A mensagem na DLQ não expira: fica até alguém tirar, e ocupa a cota da entidade.

## Kafka com consumer group

**Configuração.** O broker não tem DLQ para consumer group: quem publica no tópico de DLQ é o cliente. No Spring Kafka (3.3 ou mais novo), `DefaultErrorHandler` com `DeadLetterPublishingRecoverer` publica em `<tópico>-dlt` (aqui, `pedidos-dlt`), na mesma partição, depois de 10 tentativas por padrão. Até a 3.2 o sufixo era `.DLT`. O tópico de DLQ precisa ter pelo menos tantas partições quanto o original.

**O que a mensagem leva.** No Spring Kafka, os cabeçalhos `kafka_dlt-exception-fqcn`, `kafka_dlt-exception-message`, `kafka_dlt-original-topic`, `kafka_dlt-original-partition` e `kafka_dlt-original-offset`. Nos clientes sem esse recurso (Confluent .NET, confluent-kafka-go, `@confluentinc/kafka-javascript`), você publica no tópico de DLQ com os cabeçalhos que quiser e faz o commit do offset.

**Ler sem apagar.** Ler nunca apaga no Kafka. Use um consumer group próprio para a triagem ou leia sem commit (`kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic pedidos-dlt --from-beginning --formatter-property print.headers=true`). Sem `--group`, a ferramenta cria um grupo descartável com o auto commit desligado.

**Devolver com vazão.** Um consumidor do `pedidos-dlt` que publica em `pedidos` no ritmo que você definir. A mensagem devolvida entra no fim da partição: se a ordem por chave importa, segure as alterações como no `03-triagem.sh`.

**Alarme.** Entrada de mensagens no tópico de DLQ (`MessagesInPerSec` do broker, por tópico).

**Retenção.** A do tópico (`retention.ms`; o padrão do broker é 7 dias). Aumente no tópico de DLQ.

## Kafka 4.4 com share groups

**Configuração.** A 4.4 trouxe DLQ no broker para share groups (KIP-1191). No grupo, `errors.deadletterqueue.topic.name`; o registro vai para lá depois de `share.delivery.count.limit` entregas (padrão 5) ou quando o consumidor rejeita. O nome do tópico precisa começar com `dlq.` (`errors.deadletterqueue.topic.name.prefix`), o tópico precisa de `errors.deadletterqueue.group.enable=true` e a criação automática vem desligada. O recurso depende de `share.version` 2.

**O que a mensagem leva.** Os cabeçalhos `__dlq.errors.topic`, `__dlq.errors.partition`, `__dlq.errors.offset`, `__dlq.errors.group`, `__dlq.errors.delivery.count` e `__dlq.errors.message`. O conteúdo do registro original só é copiado com `errors.deadletterqueue.copy.record.enable=true`, que vem desligado. Sem a cópia, recuperar o payload depende do registro original ainda estar dentro da retenção do tópico de origem.

**Ler, devolver, alarme e retenção.** Como no consumer group: o tópico `dlq.pedidos` é um tópico comum.

## Bibliotecas de job

**Sidekiq.** 25 tentativas por padrão; depois o job vai para o Dead set, que guarda até 10.000 jobs por 6 meses. Pela interface web, dá para reenviar um por um ou todos.

**BullMQ.** Sem `attempts`, o job roda uma vez. Ao esgotar, fica no failed set até `removeOnFail` limpar.

**Oban.** `max_attempts` 20 por padrão; depois o job fica como `discarded`. Com o pruner ligado sem opções (`pruner: Oban.Pruner` desde a 2.24, que renomeou o `Oban.Plugins.Pruner`), jobs `completed`, `cancelled` e `discarded` com mais de 60 segundos são apagados: aumente o `max_age` antes de precisar da triagem. O instalador da 2.24 já gera a configuração com um dia.
