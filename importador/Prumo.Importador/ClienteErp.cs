using System.Net;
using System.Net.Http.Json;

namespace Prumo.Importador;

public enum Resultado { Criado, JaExistia, Alterado, CreditoRecusado }

// Cliente HTTP do Prumo ERP.
public class ClienteErp(IHttpClientFactory fabrica)
{
    public const string Nome = "prumo-erp";

    private record ErroDoErp(string Codigo, string Mensagem);
    private record RespostaDoErp(string Pedido, string NumeroErp, string Status);

    public async Task<Resultado> EnviarAsync(PedidoDoPortal pedido, CancellationToken ct)
    {
        var http = fabrica.CreateClient(Nome);
        var alteracao = pedido.Evento == "pedido.alterado";
        using var req = alteracao
            ? new HttpRequestMessage(HttpMethod.Put, $"/api/pedidos/{pedido.Numero}")
            : new HttpRequestMessage(HttpMethod.Post, "/api/pedidos");

        req.Content = JsonContent.Create(new
        {
            numero = pedido.Numero,
            cnpj = pedido.Cliente.Cnpj,
            itens = pedido.Itens.Select(i => new { sku = i.Sku, quantidade = i.Quantidade, preco = i.PrecoUnitario }),
            desconto = pedido.PercentualDeDesconto(),
            total = pedido.Total(),
        });

        // Chave de idempotência: se o ERP já gravou este pedido (o timeout da manutenção),
        // ele devolve o que existe com status "ja_existe" em vez de criar outro.
        req.Headers.Add("Idempotency-Key", alteracao ? $"{pedido.Numero}@{pedido.AlteradoEm:O}" : pedido.Numero);

        using var resp = await http.SendAsync(req, ct);

        if (resp.StatusCode == HttpStatusCode.UnprocessableEntity)
        {
            var erro = await resp.Content.ReadFromJsonAsync<ErroDoErp>(ct);
            return erro?.Codigo switch
            {
                "cliente_bloqueado" => Resultado.CreditoRecusado,
                "sku_nao_encontrado" => throw new DadoInvalidoException("SkuNaoEncontrado", $"{erro.Mensagem} (HTTP 422)"),
                _ => throw new DadoInvalidoException("RecusadoPeloErp", $"{erro?.Mensagem} (HTTP 422)"),
            };
        }

        resp.EnsureSuccessStatusCode();   // 503, 500, 404 da alteração sem pedido: falha temporária, exceção

        var corpo = await resp.Content.ReadFromJsonAsync<RespostaDoErp>(ct);
        return corpo?.Status switch
        {
            "ja_existe" => Resultado.JaExistia,
            _ when alteracao => Resultado.Alterado,
            _ => Resultado.Criado,
        };
    }
}
