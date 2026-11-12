-- Tabelas da triagem. Rodar uma vez: psql "$DATABASE_URL" -f sql/01-tabelas.sql
\set ON_ERROR_STOP on

-- Ficha de diagnóstico: o importador grava a cada falha, pelo id da mensagem.
-- Quando a redrive policy move a mensagem para a DLQ, o MessageId continua o mesmo (comportamento observado e
-- mostrado pela AWS no post de junho de 2023 sobre as APIs de redrive), então a ficha liga direto com o que está lá.
-- O redrive de volta, o 03-triagem.sh e o 05-reenviar.sh criam mensagens novas, com outro MessageId.
create table if not exists dlq_ficha (
  message_id      text primary key,
  fila_origem     text        not null,
  pedido          text,
  evento          text,
  excecao         text        not null,   -- tipo da exceção (HttpRequestException, DescontoInvalido...)
  mensagem        text        not null,   -- mensagem da exceção
  tentativas      int         not null,   -- ApproximateReceiveCount no momento da falha
  primeira_falha  timestamptz not null,
  ultima_falha    timestamptz not null,
  versao          text        not null    -- versão do consumidor que falhou
);

-- Cada leitura da DLQ vira um lote. Nada é apagado da fila na leitura.
create table if not exists dlq_lida (
  lote          text        not null,
  message_id    text        not null,
  fila          text        not null,
  lida_em       timestamptz not null default now(),
  recebimentos  int,
  enviada_em    timestamptz,
  fila_origem   text,                     -- DeadLetterQueueSourceArn, quando o SQS informa
  pedido        text,
  evento        text,
  corpo         jsonb       not null,   -- o corpo como JSON; quando não é JSON, {"bruto": "<texto>"}
  corpo_bruto   text,                     -- o texto exato do corpo
  atributos     jsonb,
  destino       text,                     -- preenchido por sql/03-destinos.sql; nulo fica na DLQ
  primary key (lote, message_id)
);

-- Pedidos cancelados no portal, exportados em CSV (uma coluna: pedido).
create table if not exists portal_cancelados (
  pedido text primary key
);

-- Registro de tudo que foi descartado: quem decidiu, quando, por quê e o payload original.
create table if not exists dlq_descartes (
  id            bigserial   primary key,
  lote          text        not null,
  fila          text        not null,
  message_id    text        not null,   -- MessageId na DLQ (atributo dlq.message_id da triagem), liga com dlq_ficha
  pedido        text,
  evento        text,
  motivo        text        not null,
  decidido_por  text        not null,
  decidido_em   timestamptz not null default now(),
  payload       jsonb       not null,
  payload_bruto text,
  atributos     jsonb,
  unique (lote, message_id)
);
