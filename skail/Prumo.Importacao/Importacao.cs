using Skail.Platform.Runtime;
using Skail.Platform.Runtime.Threading;

namespace Prumo.Importacao;

// O mesmo importador como function do skail: cada pedido é uma execução de ImportarPedido.
// Cada command é uma etapa com I/O, com argumentos e resultado gravados no histórico da execução.
public class Importacao
{
    private readonly ClienteErp _erp;
    private readonly AvisosDoPortal _portal;

    public Importacao(ClienteErp erp, AvisosDoPortal portal)
    {
        _erp = erp;
        _portal = portal;
    }

    [SkailFunction]
    public async SkailTask<string> ImportarPedido(PedidoDoPortal pedido)
    {
        var validacao = await ValidarPedido(pedido);
        if (!validacao.Valido)
        {
            await AvisarPortal(pedido.Numero, "pedido.recusado", validacao.Motivo!);
            return "recusado";
        }

        var criado = await CriarPedidoNoErp(pedido, validacao.Desconto);
        await AvisarPortal(pedido.Numero, "pedido.importado", criado.NumeroNoErp);
        return criado.NumeroNoErp;
    }

    // Lê o desconto e consulta SKU e crédito no ERP. Só leituras: repetir é seguro.
    // Pedido inválido volta como resultado, sem exceção.
    [SkailCommand(retryCount: 5)]
    public async SkailTask<Validacao> ValidarPedido(PedidoDoPortal pedido)
    {
        return await _erp.ValidarAsync(pedido);
    }

    // O número do pedido vai como chave de idempotência:
    // se o ERP já gravou, devolve o que existe.
    [SkailCommand(retryCount: 5)]
    public async SkailTask<PedidoCriado> CriarPedidoNoErp(PedidoDoPortal pedido, decimal desconto)
    {
        return await _erp.CriarAsync(pedido, desconto);
    }

    [SkailCommand(retryCount: 5)]
    public async SkailTask AvisarPortal(string pedido, string evento, string detalhe)
    {
        await _portal.PublicarAsync(pedido, evento, detalhe);
    }
}
