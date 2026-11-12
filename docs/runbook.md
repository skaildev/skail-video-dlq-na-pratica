# Runbook: mensagens na pedidos-dlq

Modelo de uma página para a DLQ da importação de pedidos. Troque nomes, prazos e responsáveis pelos do seu time.

**Quando abrir:** alarme `pedidos-dlq-mensagens-visiveis` (mensagem na DLQ) ou `pedidos-dlq-sem-triagem` (a DLQ passou 1 hora sem esvaziar). Os dois são criados pelo `sqs/07-alarmes.sh`.

**Prazo de triagem:** 1 dia útil a partir do alarme. A retenção da DLQ é de 14 dias, contados desde a entrada na fila de origem (fila standard), então 1 dia útil cabe com folga.

**Vazão máxima de reprocessamento:** 10 mensagens por segundo. O Prumo ERP aguenta 15 por segundo; os outros 5 ficam para o tráfego normal.

**Descartes:** sempre pela `sqs/06-descartar.sh`, que grava em `dlq_descartes` antes de apagar. Nenhuma mensagem sai da DLQ sem destino registrado.

## Quem decide cada motivo

| Motivo na ficha | Causa provável | Quem decide | Para onde vai |
|---|---|---|---|
| `HttpRequestException` 5xx, `TaskCanceledException` | ERP fora do ar (manutenção, deploy, sobrecarga) | Time do importador | Redrive com vazão, depois que o ERP voltar |
| `DescontoInvalido`, `JsonException` | Código do importador ou contrato do portal | Time do importador | Fila de código; volta depois da versão corrigida |
| `SkuNaoEncontrado` | Cadastro de produto no Prumo | Comercial | Fila de cadastro; volta depois do cadastro |
| `ClienteBloqueado` | Crédito do cliente | Financeiro | Fila de crédito; liberados voltam, os outros viram `pedido.recusado` |
| Pedido cancelado no portal | Comprador cancelou depois do envio | Comercial | Descarte com registro |

## As quatro perguntas, para cada grupo

1. Qual foi a causa, e ela já foi corrigida?
2. A mensagem ainda vale? (pedido cancelado, preço vencido, evento substituído por outro mais novo)
3. Reprocessar repete uma operação de I/O? (o pedido já foi gravado no ERP antes do timeout; sem chave de idempotência, cria em dobro)
4. Quem decide, e em que ordem a mensagem pode voltar? (alteração depois da criação)

## Passo a passo

1. Confirme que a causa do maior grupo acabou. Com o ERP fora, qualquer reprocessamento volta para a DLQ.
2. Leia a DLQ sem apagar: `export LOTE=$(date +%Y%m%d-%H%M)` e depois `./sqs/02-ler-dlq.sh`.
3. Agrupe: `psql "$DATABASE_URL" -v lote=$LOTE -f sql/02-grupos.sql`.
4. Passe cada grupo pelas quatro perguntas e chame quem decide.
5. Exporte os cancelados do portal para `portal_cancelados` (`\copy portal_cancelados from 'cancelados.csv' with (format csv, header)`), ajuste as regras de `sql/03-destinos.sql` e rode: `psql "$DATABASE_URL" -v lote=$LOTE -f sql/03-destinos.sql`.
6. Separe a DLQ por grupo: `./sqs/03-triagem.sh $LOTE`. Na DLQ fica só o que pode voltar agora. Confira a contagem que o script mostra no fim (na sexta, 799 visíveis na DLQ). Se ele sair com código diferente de 0, sobrou mensagem sem triagem: não rode o redrive, leia e separe de novo com um lote novo.
7. Redrive com vazão: `./sqs/04-redrive.sh 10`. Acompanhe a latência e os erros do ERP enquanto roda.
8. Depois que as criações entraram no ERP: `./sqs/05-reenviar.sh pedidos-dlq-alteracoes 10`.
9. Cada fila de grupo volta pela `05-reenviar.sh` depois da sua correção (versão nova, cadastro, decisão do financeiro).
10. Descarte o que não vale mais: `./sqs/06-descartar.sh pedidos-dlq-cancelados "cancelado no portal" "comercial"`.
11. Feche: DLQ e filas de grupo em zero. Registre no incidente o lote, a contagem por destino e o lote de descarte.

## O que não fazer

- Redrive da DLQ inteira sem separar os grupos e sem limite de vazão.
- `purge-queue` na DLQ ou em fila de grupo.
- Reprocessar sem chave de idempotência na chamada ao ERP.
- Devolver alterações antes das criações do mesmo pedido.
- Apagar mensagem sem linha em `dlq_descartes`.
