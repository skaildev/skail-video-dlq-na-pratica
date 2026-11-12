using System.Text.Json;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace Prumo.Importador;

// Decide o destino de cada mensagem da fila de pedidos:
//   sucesso ou pedido que já existia no ERP       -> apaga
//   crédito recusado                              -> publica pedido.recusado e apaga
//   falha temporária (503, timeout)               -> grava a ficha e relança: o SQS entrega de novo
//   falha de dado (JSON, desconto, SKU)           -> manda direto para a DLQ, apaga e grava a ficha
public class ProcessadorDePedidos(
    IAmazonSQS sqs, Filas filas, ClienteErp erp, PublicadorDeEventos eventos,
    FichaDeDiagnostico ficha, ILogger<ProcessadorDePedidos> log)
{
    public async Task ProcessarAsync(Message msg, CancellationToken ct)
    {
        var recebimentos = int.Parse(
            msg.Attributes?.GetValueOrDefault("ApproximateReceiveCount") ?? "1");
        PedidoDoPortal? pedido = null;
        try
        {
            pedido = PedidoDoPortal.Ler(msg.Body);
            var resultado = await erp.EnviarAsync(pedido, ct);   // Idempotency-Key = número do pedido

            if (resultado == Resultado.CreditoRecusado)
            {
                await eventos.PublicarAsync(
                    "pedido.recusado", pedido.Numero, "cliente bloqueado por crédito", ct);
                log.LogInformation("{Pedido} recusado por crédito: pedido.recusado publicado", pedido.Numero);
            }
            else
                log.LogInformation("{Pedido} enviado ao ERP: {Resultado}", pedido.Numero, resultado);

            await sqs.DeleteMessageAsync(filas.Pedidos, msg.ReceiptHandle, ct);
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException
                                   && !ct.IsCancellationRequested)
        {
            // Falha temporária: não apaga. A mensagem volta depois do visibility timeout.
            await ficha.GravarAsync(msg.MessageId, filas.Pedidos, pedido, ex, recebimentos, ct);
            log.LogWarning("{Pedido} receive={Recebimentos} {Erro}",
                           pedido?.Numero, recebimentos, ex.Message);
            throw;
        }
        catch (Exception ex) when (ex is JsonException or DadoInvalidoException)
        {
            // Tentar de novo não muda nada: vai direto para a DLQ, com o motivo nos atributos.
            // O SQS não tem "mandar para a DLQ": SendMessage na DLQ e DeleteMessage na origem.
            var enviada = await sqs.SendMessageAsync(new SendMessageRequest
            {
                QueueUrl = filas.Dlq,
                MessageBody = msg.Body,
                // O SQS aceita até 10 atributos: os do portal (até 8) mais os dois do motivo.
                MessageAttributes = new(
                    (msg.MessageAttributes ?? []).Where(a => !a.Key.StartsWith("erro.")).Take(8))
                {
                    ["erro.tipo"] = Texto(FichaDeDiagnostico.NomeDaExcecao(ex)),
                    ["erro.mensagem"] = Texto(ex.Message),
                },
            }, ct);
            // Apaga antes de gravar a ficha: com o banco fora, a mensagem não se repete na DLQ.
            await sqs.DeleteMessageAsync(filas.Pedidos, msg.ReceiptHandle, ct);
            try { await ficha.GravarAsync(enviada.MessageId, filas.Pedidos, pedido, ex, recebimentos, ct); }
            catch (Exception erro) when (!ct.IsCancellationRequested)
            { log.LogError(erro, "{MessageId} na DLQ sem ficha", enviada.MessageId); }
            log.LogWarning("{Pedido} enviado para a DLQ: {Erro}",
                           pedido?.Numero ?? msg.MessageId, ex.Message);
        }
    }

    private static MessageAttributeValue Texto(string valor) =>
        new() { DataType = "String", StringValue = valor.Length > 1000 ? valor[..1000] : valor };
}
