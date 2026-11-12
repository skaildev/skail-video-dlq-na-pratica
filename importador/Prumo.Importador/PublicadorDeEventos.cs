using System.Text.Json;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace Prumo.Importador;

// Avisa o portal B2B pela fila de eventos (pedido.recusado, por exemplo). O portal avisa o comprador.
public class PublicadorDeEventos(IAmazonSQS sqs, Filas filas)
{
    public async Task PublicarAsync(string evento, string pedido, string motivo, CancellationToken ct)
    {
        await sqs.SendMessageAsync(new SendMessageRequest
        {
            QueueUrl = filas.EventosDoPortal,
            MessageBody = JsonSerializer.Serialize(new { evento, pedido, motivo }),
            MessageAttributes = new()
            {
                ["evento"] = new() { DataType = "String", StringValue = evento },
                ["pedido"] = new() { DataType = "String", StringValue = pedido },
            },
        }, ct);
    }
}
