using System.Text.Json;
using DataConnect.Shared;
using DataConnect.Models;
using WatsonWebserver.Core;
using WatsonWebserver.Lite;
using WatsonWebserver.Lite.Extensions.HostBuilderExtension;
using DataConnect.Routes;

namespace DataConnect.Controller;

public class Server(int port, string conStr, string database, int threadPagination) : IDisposable
{
    private bool _disposed;
    private readonly int _port = port;
    private readonly WebserverLite _server = new HostBuilder("*", port, false, NotFound)
            .MapStaticRoute(WatsonWebserver.Core.HttpMethod.GET, "/api", GetRoutes)
            .MapStaticRoute(WatsonWebserver.Core.HttpMethod.POST, "/api/custom/ponto_assinatura_espelho", (HttpContextBase ctx) => {
                return StouApi.StouAssinaturaEspelho(ctx, conStr, database);
            })
            .MapStaticRoute(WatsonWebserver.Core.HttpMethod.POST, "/api/custom/ponto_espelho", (HttpContextBase ctx) => {
                return StouApi.StouEspelho(ctx, conStr, database, threadPagination);
            })
            .MapStaticRoute(WatsonWebserver.Core.HttpMethod.POST, "/api/sql", GetRoutes)
            .Build();

    public void Start()
    {
        _server.Start();
        Log.Out($"Listening on *:{_port}");
    }

    private static async Task NotFound(HttpContextBase ctx)
    {
        Log.Out(
            $"Receiving {ctx.Request.Method} request for a non-existent resource " +
            $"by {ctx.Request.Source.IpAddress}:{ctx.Request.Source.Port}"
        );
        
        string res = JsonSerializer.Serialize(new Response() {
            Error = false,
            Status = 404,
            Message = "The resource was not found.",
            Options = []
        });

        ctx.Response.StatusCode = 404;
        await ctx.Response.Send(res);
        
        Log.Out($"Response to {ctx.Guid} was: {ctx.Response.StatusCode} - {ctx.Response.StatusDescription}");
    }

    private static async Task GetRoutes(HttpContextBase ctx) 
    {
        Log.Out(
            $"Receiving {ctx.Request.Method} request for {ctx.Request.Url.RawWithoutQuery} " +
            $"by {ctx.Request.Source.IpAddress}:{ctx.Request.Source.Port}"
        );
        
        List<string> routes = [
            "POST /api/custom/ponto_espelho",
            "POST /api/sql",
        ];

        string res = JsonSerializer.Serialize(new Response() {
            Error = false,
            Status = 200,
            Message = "Refer to the following options.",
            Options = routes
        });

        ctx.Response.StatusCode = 200;
        await ctx.Response.Send(res);

        Log.Out($"Response to {ctx.Guid} was: {ctx.Response.StatusCode} - {ctx.Response.StatusDescription}");
    }
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    private void Dispose(bool disposing)
    {
        if (!_disposed) {
            return;
        }

        if (disposing) {
           _server.Dispose();
        }

        _disposed = true;
    }

    ~Server()
    {
        Dispose(false);
    }
}