using System.Reflection;
using Npgsql;

namespace Prumo.Importador;

// Grava em dlq_ficha o que o broker não anota: exceção, mensagem, tentativas, primeira falha e versão.
// A chave é o MessageId, que continua o mesmo quando a redrive policy move a mensagem para a DLQ.
// Quem volta pelo redrive ou pelos scripts de reenvio é mensagem nova, com outro MessageId:
// para juntar o histórico, use o número do pedido.
public class FichaDeDiagnostico(NpgsqlDataSource banco)
{
    private static readonly string Versao =
        typeof(FichaDeDiagnostico).Assembly.GetName().Version?.ToString(3) ?? "?";

    public static string NomeDaExcecao(Exception ex) =>
        ex is DadoInvalidoException d ? d.Codigo : ex.GetType().Name;

    public async Task GravarAsync(string messageId, string filaOrigem, PedidoDoPortal? pedido,
                                  Exception ex, int tentativas, CancellationToken ct)
    {
        await using var cmd = banco.CreateCommand("""
            insert into dlq_ficha (message_id, fila_origem, pedido, evento, excecao, mensagem,
                                   tentativas, primeira_falha, ultima_falha, versao)
            values ($1, $2, $3, $4, $5, $6, $7, now(), now(), $8)
            on conflict (message_id) do update set
              excecao      = excluded.excecao,
              mensagem     = excluded.mensagem,
              tentativas   = excluded.tentativas,
              ultima_falha = excluded.ultima_falha,
              versao       = excluded.versao
            """);
        cmd.Parameters.AddWithValue(messageId);
        cmd.Parameters.AddWithValue(filaOrigem.Split('/')[^1]);
        cmd.Parameters.AddWithValue((object?)pedido?.Numero ?? DBNull.Value);
        cmd.Parameters.AddWithValue((object?)pedido?.Evento ?? DBNull.Value);
        cmd.Parameters.AddWithValue(NomeDaExcecao(ex));
        cmd.Parameters.AddWithValue(ex.Message);
        cmd.Parameters.AddWithValue(tentativas);
        cmd.Parameters.AddWithValue(Versao);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
