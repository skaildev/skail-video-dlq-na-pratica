-- Agrupa um lote lido da DLQ por exceção, com as versões do consumidor em que cada uma apareceu.
-- psql "$DATABASE_URL" -v lote=20261016-0830 -f sql/02-grupos.sql
-- Uma exceção que só aparece numa versão nova costuma ser causa no código.
\set ON_ERROR_STOP on

\pset footer off
select coalesce(f.excecao, '(sem ficha)')                                             as excecao,
       count(*)                                                                       as mensagens,
       string_agg(distinct f.versao, ', ')                                            as versoes,
       to_char(min(f.primeira_falha) at time zone 'America/Sao_Paulo', 'DD/MM HH24:MI') as primeira_falha,
       to_char(max(f.ultima_falha)   at time zone 'America/Sao_Paulo', 'DD/MM HH24:MI') as ultima_falha
from dlq_lida l
left join dlq_ficha f using (message_id)
where l.lote = :'lote'
group by 1
order by mensagens desc;

-- Uma mensagem de exemplo por grupo.
select distinct on (f.excecao) f.excecao, l.pedido, f.mensagem
from dlq_lida l
join dlq_ficha f using (message_id)
where l.lote = :'lote'
order by f.excecao, f.ultima_falha;

-- Dentro de cada grupo, quantas são criação e quantas são alteração de pedido.
select coalesce(f.excecao, '(sem ficha)') as excecao, l.evento, count(*) as mensagens
from dlq_lida l
left join dlq_ficha f using (message_id)
where l.lote = :'lote'
group by 1, 2
order by 1, 3 desc;
