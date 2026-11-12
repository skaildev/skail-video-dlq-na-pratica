namespace Prumo.Importacao;

public record Cliente(string Cnpj, string RazaoSocial);

public record Item(string Sku, int Quantidade, decimal PrecoUnitario);

public record PedidoDoPortal(string Numero, Cliente Cliente, IReadOnlyList<Item> Itens, string Desconto, DateTimeOffset CriadoEm)
{
    public decimal Bruto => Itens.Sum(i => i.Quantidade * i.PrecoUnitario);
}

public record Validacao(bool Valido, string? Motivo, decimal Desconto, decimal Total);

public record PedidoCriado(string NumeroNoErp, bool JaExistia);
