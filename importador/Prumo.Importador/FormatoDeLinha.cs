using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Logging.Console;

namespace Prumo.Importador;

// Uma linha por evento, no formato "01:02:00 warn PV-48213 receive=1 ...", para filtrar com grep.
public sealed class FormatoDeLinha() : ConsoleFormatter(Nome)
{
    public const string Nome = "linha";

    public override void Write<TState>(in LogEntry<TState> entrada, IExternalScopeProvider? escopos, TextWriter saida)
    {
        var nivel = entrada.LogLevel switch
        {
            LogLevel.Trace => "trce", LogLevel.Debug => "dbug", LogLevel.Information => "info",
            LogLevel.Warning => "warn", LogLevel.Error => "fail", _ => "crit",
        };
        saida.Write($"{DateTime.Now:HH:mm:ss} {nivel} {entrada.Formatter(entrada.State, entrada.Exception)}");
        if (entrada.Exception is not null) saida.Write($"{Environment.NewLine}{entrada.Exception}");
        saida.WriteLine();
    }
}
