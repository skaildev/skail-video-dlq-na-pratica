using System.Globalization;
using System.Net;
using System.Net.Http.Json;

namespace Prumo.Importacao;

// Cliente HTTP do Prumo ERP, usado só dentro dos commands.
public class ClienteErp(IHttpClientFactory fabrica)
{
    public const string Nome = "prumo-erp";
    private static readonly CultureInfo PtBr = CultureInfo.GetCultureInfo("pt-BR");

    private record Credito(bool Bloqueado);
    private record RespostaDoErp(string NumeroErp, string Status);

    public async Task<Validacao> ValidarAsync(PedidoDoPortal pedido)
    {
        if (!decimal.TryParse(pedido.Desconto, NumberStyles.AllowDecimalPoint, PtBr, out var desconto))
            return new Validacao(false, $"desconto '{pedido.Desconto}' não é um número", 0, 0);

        var http = fabrica.CreateClient(Nome);
        foreach (var item in pedido.Itens)
        {
            using var produto = await http.GetAsync($"/api/produtos/{item.Sku}");
            if (produto.StatusCode == HttpStatusCode.NotFound)
                return new Validacao(false, $"SKU {item.Sku} não encontrado no cadastro do Prumo", desconto, 0);
            produto.EnsureSuccessStatusCode();
        }

        var cnpj = new string(pedido.Cliente.Cnpj.Where(char.IsDigit).ToArray());
        var credito = await http.GetFromJsonAsync<Credito>($"/api/clientes/{cnpj}/credito");
        if (credito!.Bloqueado)
            return new Validacao(false, $"cliente {pedido.Cliente.Cnpj} bloqueado por crédito", desconto, 0);

        var total = Math.Round(pedido.Bruto * (1 - desconto / 100), 2);
        return new Validacao(true, null, desconto, total);
    }

    public async Task<PedidoCriado> CriarAsync(PedidoDoPortal pedido, decimal desconto)
    {
        var http = fabrica.CreateClient(Nome);
        using var req = new HttpRequestMessage(HttpMethod.Post, "/api/pedidos")
        {
            Content = JsonContent.Create(new
            {
                numero = pedido.Numero,
                cnpj = pedido.Cliente.Cnpj,
                itens = pedido.Itens.Select(i => new { sku = i.Sku, quantidade = i.Quantidade, preco = i.PrecoUnitario }),
                desconto,
            }),
        };
        req.Headers.Add("Idempotency-Key", pedido.Numero);

        var resposta = await http.SendAsync(req);
        resposta.EnsureSuccessStatusCode();   // falha técnica: exceção, o skail tenta de novo

        var corpo = await resposta.Content.ReadFromJsonAsync<RespostaDoErp>();
        return new PedidoCriado(corpo!.NumeroErp, corpo.Status == "ja_existe");
    }
}

// Avisa o portal B2B do resultado de cada pedido.
public class AvisosDoPortal(IHttpClientFactory fabrica)
{
    public const string Nome = "portal";

    public async Task PublicarAsync(string pedido, string evento, string detalhe)
    {
        var http = fabrica.CreateClient(Nome);
        var resposta = await http.PostAsJsonAsync("/api/eventos", new { evento, pedido, detalhe });
        resposta.EnsureSuccessStatusCode();
    }
}
