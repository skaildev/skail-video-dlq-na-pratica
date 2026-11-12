# Prumo.Importacao no skail

O mesmo fluxo do importador como function durável, com uma diferença: aqui desconto ilegível e SKU fora do cadastro voltam como recusa e avisam o portal, em vez de ir para uma DLQ. `ImportarPedido` é a function e chama três commands, um por etapa:

- `ValidarPedido`: lê o desconto e consulta o cadastro de SKU e o crédito no Prumo. Só faz leituras, então repetir as consultas numa nova tentativa é seguro. Pedido inválido volta como resultado, sem exceção.
- `CriarPedidoNoErp`: cria o pedido com o número do pedido como chave de idempotência. 503 e timeout viram exceção, e o skail tenta de novo.
- `AvisarPortal`: publica `pedido.importado` ou `pedido.recusado` para o portal.

Não há `try/catch` no `await CriarPedidoNoErp`. Quando o command esgota as tentativas, a function é reentregue e falha no mesmo ponto, sem chamar o ERP de novo: o replay devolve o erro gravado. A execução fica em falha no Monitor até alguém corrigir a causa e tentar de novo.

O projeto usa os pacotes `Skail.Platform.Runtime` e `Skail.Platform.Analyzer` 2.2 do NuGet.org e roda em .NET 8. A configuração do runtime (`SKAIL_*`) está em [Configurar o runtime](https://docs.skail.dev/opere/configurar-e-hospedar/configurar-o-runtime).

## Depurar uma execução de produção

1. No Monitor, abra a execução e clique em Depurar.
2. A janela mostra a configuração JSON de um perfil `importacao.Debug`, com `SKAIL_WORKLOAD`, `SKAIL_DEBUG_FILE` e `SKAIL_LOG_LEVEL`. O `Properties/launchSettings.json` fica fora do Git: se ele não existir, crie com `{ "profiles": { } }` e cole o JSON copiado dentro de `profiles`.
3. Coloque um breakpoint na `ImportarPedido` e rode o perfil `importacao.Debug` no depurador da IDE. Pela linha de comando, use `dotnet run --launch-profile importacao.Debug`; sem a opção, o `dotnet run` usa o primeiro perfil do arquivo.

Cada `await` de command devolve o resultado gravado em produção, sem chamar o ERP nem o portal, e o processo termina sozinho no fim. O código precisa ser o mesmo da versão que rodou. O passo a passo completo está em [Depurar com Time Travel Debug](https://docs.skail.dev/construa/como-fazer/depurar-com-time-travel-debug).

O `launchSettings.json` fica fora do Git (`.gitignore`): a URL do `SKAIL_DEBUG_FILE` dá acesso aos dados da execução.
