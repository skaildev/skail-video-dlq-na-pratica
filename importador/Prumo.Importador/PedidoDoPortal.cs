using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Prumo.Importador;

public record Cliente(string Cnpj, string RazaoSocial);

public record Item(string Sku, int Quantidade, decimal PrecoUnitario);

public record PedidoDoPortal(
    string Evento,
    [property: JsonPropertyName("pedido")] string Numero,
    Cliente Cliente,
    IReadOnlyList<Item> Itens,
    string Desconto,
    DateTimeOffset? CriadoEm,
    DateTimeOffset? AlteradoEm)
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private static readonly CultureInfo PtBr = CultureInfo.GetCultureInfo("pt-BR");

    public static PedidoDoPortal Ler(string corpo) =>
        JsonSerializer.Deserialize<PedidoDoPortal>(corpo, Json) ?? throw new JsonException("corpo vazio");

    // O portal manda o desconto como texto, com vírgula: "12,5".
    // A 2.14.0 passou a ler com ponto decimal (CultureInfo.InvariantCulture) e todo desconto fracionado falhou.
    // A 2.14.1 volta a ler em pt-BR e não aceita separador de milhar, para "12.5" não virar 125.
    public decimal PercentualDeDesconto() =>
        decimal.TryParse(Desconto, NumberStyles.AllowDecimalPoint, PtBr, out var valor)
            ? valor
            : throw new DadoInvalidoException("DescontoInvalido", $"desconto '{Desconto}' não é um número");

    public decimal Total() =>
        Math.Round(Itens.Sum(i => i.Quantidade * i.PrecoUnitario) * (1 - PercentualDeDesconto() / 100), 2);
}

// Falha de dado: tentar de novo não muda o resultado.
public class DadoInvalidoException(string codigo, string mensagem) : Exception(mensagem)
{
    public string Codigo { get; } = codigo;
}
