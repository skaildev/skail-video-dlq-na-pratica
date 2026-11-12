#!/usr/bin/env python3
"""Reproduz o incidente da Vale Norte num SQS local (moto ou LocalStack) e num Postgres de teste.

Cria 1.240 mensagens na fila de pedidos, faz cada uma ser recebida 5 vezes sem confirmação
(o SQS move para a pedidos-dlq) e grava a ficha de diagnóstico de cada uma em dlq_ficha.
Também gera simulacao/cancelados.csv, a exportação dos 40 pedidos cancelados no portal.

uso:  AWS_ENDPOINT_URL=http://localhost:5005 DATABASE_URL=postgresql://... python3 simulacao/popular.py
Requer boto3 e psql. Nunca rode contra uma conta AWS de verdade: o script recusa sem AWS_ENDPOINT_URL.
"""
import csv, json, os, random, subprocess, sys, tempfile
from datetime import datetime, timedelta, timezone

import boto3

if not os.environ.get('AWS_ENDPOINT_URL'):
    sys.exit('defina AWS_ENDPOINT_URL apontando para um SQS local')
DB = os.environ.get('DATABASE_URL') or sys.exit('defina DATABASE_URL')
FILA, DLQ = os.environ.get('FILA', 'pedidos'), os.environ.get('DLQ', 'pedidos-dlq')

sqs = boto3.client('sqs', region_name=os.environ.get('AWS_REGION', 'sa-east-1'),
                   endpoint_url=os.environ['AWS_ENDPOINT_URL'],
                   aws_access_key_id='teste', aws_secret_access_key='teste')
url = sqs.get_queue_url(QueueName=FILA)['QueueUrl']
url_dlq = sqs.get_queue_url(QueueName=DLQ)['QueueUrl']

BR = timezone(timedelta(hours=-3))
QUI = datetime(2026, 10, 15, tzinfo=BR)
rnd = random.Random(20261016)

CLIENTES = [('11.222.333/0001-81', 'Mercado Bom Preço Ltda'), ('22.333.444/0001-05', 'Rede Sol Nascente Supermercados'),
            ('33.444.555/0001-30', 'Empório Serra Azul'), ('44.555.666/0001-62', 'Atacado Rio Claro'),
            ('55.666.777/0001-90', 'Distribuidora Ponte Alta'), ('66.777.888/0001-10', 'Mercearia Dois Irmãos')]
BLOQUEADOS = [('77.888.999/0001-41', 'Comercial Vale do Aço'), ('88.999.000/0001-77', 'Supermercado Bela Vista')]
SKUS = ['VN-10233', 'VN-10871', 'VN-20415', 'VN-31102', 'VN-40057', 'VN-41290']


def itens(com_campanha=False):
    lista = [{'sku': s, 'quantidade': rnd.choice([6, 12, 24, 48]), 'precoUnitario': round(rnd.uniform(4, 60), 2)}
             for s in rnd.sample(SKUS, rnd.randint(1, 4))]
    if com_campanha:
        lista.append({'sku': 'VN-55012', 'quantidade': rnd.choice([12, 24]), 'precoUnitario': 32.9})
    return lista


# Números de pedido únicos, com os exemplos do vídeo nos grupos certos.
fixos = {'503': [48213, 48877],
         'credito': [50117, 46021, 47790, 49233, 44871, 51008, 45562]}   # os 7 da tela do financeiro
FIXOS = set(fixos['503'] + fixos['credito'])
numeros = rnd.sample(range(40000, 52000), 1240 - 31)
numeros = [n for n in numeros if n not in FIXOS][:1240 - 31 - len(FIXOS)]
pv = lambda n: f'PV-{n}'
# Cliente de cada pedido de crédito da tela: os liberados são da Bela Vista, os bloqueados da Vale do Aço.
CLIENTE_CREDITO = {50117: 0, 46021: 1, 47790: 0, 49233: 1, 44871: 0, 51008: 1, 45562: 0}

mensagens = []   # (evento, pedido, corpo, ficha)


def add(evento, numero, corpo, excecao, msg, primeira, versao):
    mensagens.append((evento, pv(numero), corpo, (excecao, msg, primeira, versao)))


def corpo(evento, numero, quando, cliente=None, desconto=None, campanha=False):
    cnpj, nome = cliente or rnd.choice(CLIENTES)
    c = {'evento': evento, 'pedido': pv(numero), 'cliente': {'cnpj': cnpj, 'razaoSocial': nome},
         'itens': itens(campanha), 'desconto': desconto or rnd.choice(['0', '5', '10']),
         ('alteradoEm' if evento == 'pedido.alterado' else 'criadoEm'): quando.isoformat()}
    return c


def versao(t):
    return '2.14.0' if t >= QUI + timedelta(hours=22) else '2.13.4'


ERRO_503 = 'Response status code does not indicate success: 503 (Service Unavailable).'
fila_numeros = iter(numeros)

# 870 do ERP em manutenção (01:00 a 01:40): 40 cancelados depois, 31 alterações e 799 criações.
manut = QUI + timedelta(days=1, hours=1)
criacoes_503 = fixos['503'] + [next(fila_numeros) for _ in range(799 + 40 - 2)]
for i, n in enumerate(criacoes_503):
    t = manut + timedelta(seconds=rnd.uniform(0, 37 * 60 + 30))
    if n == 48213: t = manut + timedelta(minutes=2)
    if n == 48877: t = manut + timedelta(seconds=0.4)
    c = corpo('pedido.criado', n, t - timedelta(seconds=2))
    if n == 48213:   # o pedido da depuração na cena 15: bruto 963,60, desconto 10%
        c['desconto'] = '10'
        c['itens'] = [{'sku': 'VN-20415', 'quantidade': 12, 'precoUnitario': 37.80},
                      {'sku': 'VN-40057', 'quantidade': 24, 'precoUnitario': 21.25}]
    if n == 48877:   # o pedido gravado no ERP antes do timeout (cena 9)
        c['cliente'] = {'cnpj': CLIENTES[2][0], 'razaoSocial': CLIENTES[2][1]}
        c['desconto'] = '0'
        c['itens'] = [{'sku': 'VN-10233', 'quantidade': 48, 'precoUnitario': 35.60},
                      {'sku': 'VN-31102', 'quantidade': 48, 'precoUnitario': 35.50}]
    add('pedido.criado', n, c, 'HttpRequestException', ERRO_503, t, '2.14.0')
cancelados = criacoes_503[-40:]
for n in rnd.sample(criacoes_503[:-40], 31):
    t = manut + timedelta(seconds=rnd.uniform(60, 37 * 60 + 30))
    add('pedido.alterado', n, corpo('pedido.alterado', n, t - timedelta(seconds=2)), 'HttpRequestException', ERRO_503, t, '2.14.0')

# 260 com desconto decimal depois do deploy da 2.14.0 (22:00). O primeiro às 22:07.
for i in range(260):
    n = next(fila_numeros)
    t = QUI + timedelta(hours=22, minutes=7) + (timedelta(minutes=rnd.uniform(0, 590)) if i else timedelta())
    d = rnd.choice(['12,5', '7,5', '2,5', '17,5'])
    if i == 0: d = '12,5'   # o PV da ficha mostrada na cena 7
    add('pedido.criado', n, corpo('pedido.criado', n, t, desconto=d), 'DescontoInvalido', f"desconto '{d}' não é um número", t, '2.14.0')

# 85 com o SKU da campanha, que não existe no cadastro do Prumo.
for i in range(85):
    n = next(fila_numeros)
    t = QUI + timedelta(hours=20, minutes=14) + (timedelta(minutes=rnd.uniform(0, 690)) if i else timedelta())
    add('pedido.criado', n, corpo('pedido.criado', n, t, campanha=True), 'SkuNaoEncontrado',
        'SKU VN-55012 não encontrado no cadastro do Prumo (HTTP 422)', t, versao(t))

# 25 de cliente bloqueado por crédito.
for i, n in enumerate(fixos['credito'] + [next(fila_numeros) for _ in range(25 - len(fixos['credito']))]):
    t = QUI + timedelta(hours=20, minutes=40) + timedelta(minutes=rnd.uniform(0, 660))
    cli = rnd.choice(BLOQUEADOS)
    if n in CLIENTE_CREDITO: cli = BLOQUEADOS[CLIENTE_CREDITO[n]]
    add('pedido.criado', n, corpo('pedido.criado', n, t, cliente=cli), 'ClienteBloqueado',
        f'cliente {cli[0]} bloqueado por crédito (HTTP 422)', t, versao(t))

rnd.shuffle(mensagens)
assert len(mensagens) == 1240, len(mensagens)

# 1. Envia para a fila de pedidos, guardando o MessageId de cada uma.
ids = {}
for k in range(0, len(mensagens), 10):
    lote = mensagens[k:k + 10]
    r = sqs.send_message_batch(QueueUrl=url, Entries=[{
        'Id': str(i), 'MessageBody': json.dumps(c, ensure_ascii=False),
        'MessageAttributes': {'evento': {'DataType': 'String', 'StringValue': ev},
                              'pedido': {'DataType': 'String', 'StringValue': p}}}
        for i, (ev, p, c, _) in enumerate(lote)])
    for s in r.get('Successful', []):
        ids[s['MessageId']] = lote[int(s['Id'])]
    assert not r.get('Failed'), r['Failed']

# 2. Recebe sem confirmar até o SQS mover tudo para a DLQ (5 recebimentos cada).
print('recebendo sem confirmar até a DLQ encher...', flush=True)
while True:
    r = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, VisibilityTimeout=0, AttributeNames=['All'])
    if not r.get('Messages'):
        a = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=['ApproximateNumberOfMessages'])['Attributes']
        if a['ApproximateNumberOfMessages'] == '0':
            break

# 3. Ficha de diagnóstico de cada mensagem, como o importador gravaria.
with tempfile.NamedTemporaryFile('w', suffix='.csv', delete=False, newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    for mid, (ev, p, c, (exc, msg, primeira, ver)) in ids.items():
        w.writerow([mid, FILA, p, ev, exc, msg, 5, primeira.isoformat(), (primeira + timedelta(seconds=120)).isoformat(), ver])
    tmp = f.name
subprocess.run(['psql', DB, '-X', '-q', '-v', 'ON_ERROR_STOP=1', '-c',
                f"\\copy dlq_ficha (message_id, fila_origem, pedido, evento, excecao, mensagem, tentativas, primeira_falha, ultima_falha, versao) from '{tmp}' with (format csv)"],
               check=True)
os.unlink(tmp)

# 4. Exportação do portal com os pedidos cancelados.
aqui = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(aqui, 'cancelados.csv'), 'w', newline='', encoding='utf-8') as f:
    f.write('pedido\n' + ''.join(pv(n) + '\n' for n in sorted(cancelados)))

n_dlq = sqs.get_queue_attributes(QueueUrl=url_dlq, AttributeNames=['ApproximateNumberOfMessages'])['Attributes']['ApproximateNumberOfMessages']
print(f'{len(ids)} mensagens enviadas | {n_dlq} na {DLQ} | {len(ids)} fichas | {len(cancelados)} cancelados em simulacao/cancelados.csv')
