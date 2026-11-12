using Microsoft.Extensions.Hosting;
using Prumo.Importacao;
using Skail.Platform.Runtime;

[assembly: VisibleToSkailPlatform]

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddHttpClient(ClienteErp.Nome, c =>
{
    c.BaseAddress = new Uri(builder.Configuration["Erp:Url"] ?? "https://erp.prumo.example");
    c.Timeout = TimeSpan.FromSeconds(10);
});
builder.Services.AddHttpClient(AvisosDoPortal.Nome, c =>
    c.BaseAddress = new Uri(builder.Configuration["Portal:Url"] ?? "https://portal.valenorte.example"));
builder.Services.AddTransient<ClienteErp>();
builder.Services.AddTransient<AvisosDoPortal>();

builder.UseSkail();

await builder.Build().RunAsync();
