-- Decide para onde vai cada mensagem do lote, depois das quatro perguntas.
-- Edite as regras para o seu incidente e rode:
--   psql "$DATABASE_URL" -v lote=20261016-0830 -f sql/03-destinos.sql
-- Mensagem com destino nulo fica na DLQ e volta pelo redrive (sqs/04-redrive.sh).
-- A ordem importa: cada regra só pega o que as anteriores não pegaram.
\set ON_ERROR_STOP on

begin;

update dlq_lida set destino = null where lote = :'lote';

-- 1. A mensagem ainda vale? Pedido cancelado no portal não volta para o ERP.
update dlq_lida l set destino = 'pedidos-dlq-cancelados'
where l.lote = :'lote' and l.destino is null
  and coalesce(l.pedido, l.corpo->>'pedido') in (select pedido from portal_cancelados);

-- 2. Causa no código: espera a versão corrigida subir.
update dlq_lida l set destino = 'pedidos-dlq-codigo'
from dlq_ficha f
where f.message_id = l.message_id and l.lote = :'lote' and l.destino is null
  and f.excecao = 'DescontoInvalido';

-- 3. Causa no dado: espera o cadastro.
update dlq_lida l set destino = 'pedidos-dlq-cadastro'
from dlq_ficha f
where f.message_id = l.message_id and l.lote = :'lote' and l.destino is null
  and f.excecao = 'SkuNaoEncontrado';

-- 4. Decisão de outra área: o financeiro decide o crédito.
update dlq_lida l set destino = 'pedidos-dlq-credito'
from dlq_ficha f
where f.message_id = l.message_id and l.lote = :'lote' and l.destino is null
  and f.excecao = 'ClienteBloqueado';

-- 5. Ordem: alteração só volta depois que as criações entraram no ERP.
update dlq_lida l set destino = 'pedidos-dlq-alteracoes'
where l.lote = :'lote' and l.destino is null
  and coalesce(l.evento, l.corpo->>'evento') = 'pedido.alterado';

commit;

select coalesce(destino, 'fica na DLQ') as destino, count(*) as mensagens
from dlq_lida
where lote = :'lote'
group by 1
order by 2 desc;
