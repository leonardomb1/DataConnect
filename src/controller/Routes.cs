using System.Text.Json;
using DataConnect.Shared;
using DataConnect.Etl.Sql;
using DataConnect.Models;
using DataConnect.Shared.Converter;
using WatsonWebserver.Core;
using WatsonWebserver.Lite;
using WatsonWebserver.Lite.Extensions.HostBuilderExtension;
using System.Data;

namespace DataConnect.Controller;

public class Route(int port, string conStr) : IDisposable
{
    private bool _disposed;
    private readonly int _port = port;
    private readonly string _connection = conStr;
    private readonly WebserverLite _server = new HostBuilder("*", port, false, NotFound)
            .MapStaticRoute(WatsonWebserver.Core.HttpMethod.GET, "/api", GetRoutes)
            .MapStaticRoute(WatsonWebserver.Core.HttpMethod.POST, "/api/simple_auth", (HttpContextBase ctx) => SimpleAuthJobRoute(ctx, conStr))
            .MapStaticRoute(WatsonWebserver.Core.HttpMethod.POST, "/api/sql", GetRoutes)
            .Build();

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

    private static async Task SimpleAuthJobRoute(HttpContextBase ctx, string conStr) 
    {
        Log.Out($"Receiving {ctx.Request.Method} request for {ctx.Route} by {ctx.Request.Source.IpAddress}:{ctx.Request.Source.Port}");
        

        var list = new List<KeyValuePair<string, string>>
        {
            KeyValuePair.Create("pag", $"{obj.DestinationTableName}"),
            KeyValuePair.Create("cmd", "get"),
            KeyValuePair.Create("dtde", $"{DateTime.Today.AddDays(-4):dd/MM/yyyy}"),
            KeyValuePair.Create("dtate", $"{DateTime.Today:dd/MM/yyyy}"),
            KeyValuePair.Create("start", "1"),
            KeyValuePair.Create("page", "1"),
        };

        dynamic ret = await RestTemplate.TemplatePostMethod(ctx, "SimpleAuthBodyRequestAsync", [
            KeyValuePair.Create("user", "integracao"),
            KeyValuePair.Create("token", Encryption.Sha256($"{obj.Options[0]}{DateTime.Today:dd/MM/yyyy}")),
            list
        ]);

        DataTable table = DynamicObjConvert.FromInnerJsonToDataTable(ret, "itens");

        using var sql = new SqlServerCall(conStr);
        await sql.CreateTable(obj.DestinationTableName, table, obj.SysName, "DWExtract");
        await sql.BulkInsert(table, obj.DestinationTableName, obj.SysName, "DWExtract");
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