using Amazon.SQS;
using Amazon.SQS.Model;

namespace Prumo.Importador;

// Lê a fila de pedidos em lotes de até 10, com long polling de 20 s.
// Uma mensagem que falha fica sem DeleteMessage: volta depois do visibility timeout e conta mais um recebimento.
public class LeitorDaFila(IAmazonSQS sqs, Filas filas, ProcessadorDePedidos processador, ILogger<LeitorDaFila> log)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            ReceiveMessageResponse resposta;
            try
            {
                resposta = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = filas.Pedidos,
                    MaxNumberOfMessages = 10,
                    WaitTimeSeconds = 20,
                    MessageSystemAttributeNames = ["ApproximateReceiveCount"],
                    MessageAttributeNames = ["All"],
                }, ct);
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                // Sem isto, uma queda do SQS derruba o processo (BackgroundServiceExceptionBehavior.StopHost).
                log.LogError(ex, "falha ao ler {Fila}; nova leitura em 5 s", filas.Pedidos);
                await Task.Delay(TimeSpan.FromSeconds(5), ct);
                continue;
            }

            foreach (var msg in resposta.Messages ?? [])
            {
                try
                {
                    await processador.ProcessarAsync(msg, ct);
                }
                catch (Exception ex) when (!ct.IsCancellationRequested)
                {
                    // A mensagem não foi apagada e o SQS entrega de novo. 503 e timeout já saíram no log e na ficha.
                    if (ex is HttpRequestException or TaskCanceledException)
                        log.LogDebug(ex, "mensagem {MessageId} fica na fila", msg.MessageId);
                    else
                        log.LogError(ex, "mensagem {MessageId} falhou sem tratamento e fica na fila", msg.MessageId);
                }
            }
        }
    }
}
