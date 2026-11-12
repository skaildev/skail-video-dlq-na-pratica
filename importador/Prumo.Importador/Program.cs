using Amazon.SQS;
using Microsoft.Extensions.Logging.Console;
using Npgsql;
using Prumo.Importador;

var builder = Host.CreateApplicationBuilder(args);
var cfg = builder.Configuration;

// Uma linha por evento ("01:02:00 warn PV-48213 receive=1 ..."). O serviço grava o stdout em
// /var/log/prumo-importador/importador.log (veja prumo-importador.service).
builder.Logging.ClearProviders();
builder.Logging.AddConsole(o => o.FormatterName = FormatoDeLinha.Nome)
               .AddConsoleFormatter<FormatoDeLinha, ConsoleFormatterOptions>();

builder.Services.AddSingleton<IAmazonSQS>(_ => new AmazonSQSClient());
builder.Services.AddSingleton(new Filas(
    Pedidos: cfg["Filas:Pedidos"]!,
    Dlq: cfg["Filas:Dlq"]!,
    EventosDoPortal: cfg["Filas:EventosDoPortal"]!));
builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(cfg.GetConnectionString("Triagem")!));
builder.Services.AddHttpClient(ClienteErp.Nome, c =>
{
    c.BaseAddress = new Uri(cfg["Erp:Url"]!);
    c.Timeout = TimeSpan.FromSeconds(10);
});

builder.Services.AddSingleton<ClienteErp>();
builder.Services.AddSingleton<FichaDeDiagnostico>();
builder.Services.AddSingleton<PublicadorDeEventos>();
builder.Services.AddSingleton<ProcessadorDePedidos>();
builder.Services.AddHostedService<LeitorDaFila>();

await builder.Build().RunAsync();

public record Filas(string Pedidos, string Dlq, string EventosDoPortal);
