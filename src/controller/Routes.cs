using System.Text.Json;
using DataConnect.Shared;
using DataConnect.Etl.Http;
using WatsonWebserver.Core;
using WatsonWebserver.Lite;
using WatsonWebserver.Lite.Extensions.HostBuilderExtension;

namespace DataConnect.Controller;

public class Route(int port) : IDisposable
{
    private bool _disposed;
    private readonly WebserverLite _server = new HostBuilder("*", port, false, NotFound)
            .MapStaticRoute(WatsonWebserver.Core.HttpMethod.GET, "/api", GetRoutes)
            .MapStaticRoute(WatsonWebserver.Core.HttpMethod.POST, "/api/ponto_espelho", GetRoutes)
            .MapStaticRoute(WatsonWebserver.Core.HttpMethod.POST, "/api/sql", GetRoutes)
            .Build();
    private readonly int _port = port;

    public void Start()
    {
        _server.Start();
        Console.WriteLine($"Listening on *:{_port}");
    }

    private static async Task NotFound(HttpContextBase ctx)
    {
        Log.Out($"Receiving {ctx.Request.Method} request for a non-existent resource by {ctx.Request.Source.IpAddress}:{ctx.Request.Source.Port}");
        
        string res = JsonSerializer.Serialize(new Response() {
            Error = false,
            Status = 404,
            Message = "The resource was not found.",
            Options = []
        });

        ctx.Response.StatusCode = 404;
        await ctx.Response.Send(res);
    }

    private static async Task GetRoutes(HttpContextBase ctx) 
    {
        Log.Out($"Receiving {ctx.Request.Method} request for {ctx.Route} by {ctx.Request.Source.IpAddress}:{ctx.Request.Source.Port}");
        
        List<string> routes = [
            "POST /api/execute/ponto_espelho",
            "POST /api/execute/sql",
        ];

        string res = JsonSerializer.Serialize(new Response() {
            Error = false,
            Status = 200,
            Message = "Refer to the following options.",
            Options = routes
        });

        ctx.Response.StatusCode = 200;
        Log.Out($"Response was: {ctx.Response.ResponseSent}");
        await ctx.Response.Send(res);
    }

    private static async Task PostExecutePontoEspelho(HttpContextBase ctx) 
    {
        Log.Out($"Receiving {ctx.Request.Method} request for {ctx.Route} by {ctx.Request.Source.IpAddress}:{ctx.Request.Source.Port}");
        
        string res;

        var req = JsonSerializer.Deserialize<BodyDefault>(ctx.Request.DataAsString);

        try
        {
            
        }
        catch (System.Exception)
        {
            res = JsonSerializer.Serialize(new Response() {
                Error = true,
                Status = 501,
                Message = "An internal serval error occurred which stopped the completion of the request.",
                Options = []
            });
            ctx.Response.StatusCode = 501;            
        }
        finally
        {
            res = JsonSerializer.Serialize(new Response() {
                Error = false,
                Status = 201,
                Message = "Schedule was successfully triggered in the server.",
                Options = []
            });
            ctx.Response.StatusCode = 201;
        }

        Log.Out($"Response was: {ctx.Response.ResponseSent}");
        await ctx.Response.Send(res);
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

    ~Route()
    {
        Dispose(false);
    }
}