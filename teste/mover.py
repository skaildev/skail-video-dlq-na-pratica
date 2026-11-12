#!/usr/bin/env python3
"""Substituto do StartMessageMoveTask para o teste local: o moto não implementa a tarefa de redrive.
Move as mensagens da DLQ para o destino (--destination-arn ou, sem ele, a fila $FILA) no ritmo pedido
e guarda o andamento num arquivo, que o ListMessageMoveTasks simulado lê. Usado só por teste/bin/aws.
Como no SQS, cada mensagem chega ao destino como mensagem nova, com outro MessageId.
Sem destino, a tarefa falha (CouldNotDetermineMessageSource) se houver mensagem com o atributo erro.tipo,
que o importador põe nas mensagens que manda direto para a DLQ por SendMessage."""
import json, os, sys, time, uuid
import boto3

ESTADO = os.path.join(os.environ.get('TMPDIR', '/tmp'), 'dlq-na-pratica-move-task.json')
cli = boto3.client('sqs', region_name=os.environ.get('AWS_REGION', 'sa-east-1'),
                   endpoint_url=os.environ['AWS_ENDPOINT_URL'])


def grava(e):
    json.dump(e, open(ESTADO + '.tmp', 'w')); os.replace(ESTADO + '.tmp', ESTADO)


acao = sys.argv[1]
if acao == 'start':
    arn, rate = sys.argv[2], int(sys.argv[3])
    destino = sys.argv[4] if len(sys.argv) > 4 else ''

    nome = arn.split(':')[-1]
    url = cli.get_queue_url(QueueName=nome)['QueueUrl']
    total = int(cli.get_queue_attributes(QueueUrl=url, AttributeNames=['ApproximateNumberOfMessages'])['Attributes']['ApproximateNumberOfMessages'])
    e = {'TaskHandle': uuid.uuid4().hex, 'Status': 'RUNNING', 'SourceArn': arn, 'MaxNumberOfMessagesPerSecond': rate,
         'ApproximateNumberOfMessagesMoved': 0, 'ApproximateNumberOfMessagesToMove': total, 'StartedTimestamp': int(time.time() * 1000)}
    grava(e)
    if os.fork():
        print(json.dumps({'TaskHandle': e['TaskHandle']}, indent=4)); sys.exit(0)
    origem = cli.get_queue_url(QueueName=destino.split(':')[-1] if destino else os.environ.get('FILA', 'pedidos'))['QueueUrl']
    inicio = time.time()
    while True:
        r = cli.receive_message(QueueUrl=url, MaxNumberOfMessages=min(10, rate), VisibilityTimeout=60, MessageAttributeNames=['All'], WaitTimeSeconds=1)
        ms = r.get('Messages', [])
        if not ms:
            break
        if not destino and any('erro.tipo' in m.get('MessageAttributes', {}) for m in ms):
            cli.change_message_visibility_batch(QueueUrl=url, Entries=[
                {'Id': str(i), 'ReceiptHandle': m['ReceiptHandle'], 'VisibilityTimeout': 0} for i, m in enumerate(ms)])
            e['Status'] = 'FAILED'; e['FailureReason'] = 'CouldNotDetermineMessageSource'; e.pop('TaskHandle', None); grava(e)
            sys.exit(0)
        cli.send_message_batch(QueueUrl=origem, Entries=[{'Id': str(i), 'MessageBody': m['Body'], 'MessageAttributes': m.get('MessageAttributes', {})} for i, m in enumerate(ms)])
        cli.delete_message_batch(QueueUrl=url, Entries=[{'Id': str(i), 'ReceiptHandle': m['ReceiptHandle']} for i, m in enumerate(ms)])
        e['ApproximateNumberOfMessagesMoved'] += len(ms); grava(e)
        espera = inicio + e['ApproximateNumberOfMessagesMoved'] / rate - time.time()
        if espera > 0: time.sleep(espera)
    e['Status'] = 'COMPLETED'; e.pop('TaskHandle', None); grava(e)
elif acao == 'list':
    e = json.load(open(ESTADO))
    print(json.dumps({'Results': [e]}, indent=4))
