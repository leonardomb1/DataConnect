using System.Text.Json;
using DataConnect.Shared;
using DataConnect.Etl.Http;
using DataConnect.Etl.Sql;
using WatsonWebserver.Core;
using WatsonWebserver.Lite;
using WatsonWebserver.Lite.Extensions.HostBuilderExtension;
using System.Data;
using DataConnect.Shared.Converter;
using System.Reflection;

namespace DataConnect.Controller;

public class Route(int port, string conStr) : IDisposable
{
    private bool _disposed;
    private readonly WebserverLite _server = new HostBuilder("*", port, false, NotFound)
            .MapStaticRoute(WatsonWebserver.Core.HttpMethod.GET, "/api", GetRoutes)
            .MapStaticRoute(WatsonWebserver.Core.HttpMethod.POST, "/api/ponto_espelho", (HttpContextBase ctx) => PostExecutePontoEspelho(ctx, conStr))
            .MapStaticRoute(WatsonWebserver.Core.HttpMethod.POST, "/api/sql", GetRoutes)
            .Build();
    private readonly int _port = port;
    private readonly string _connection = conStr;

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

    private static async Task PostExecutePontoEspelho(HttpContextBase ctx, string conStr) 
    {
        Log.Out($"Receiving {ctx.Request.Method} request for {ctx.Route} by {ctx.Request.Source.IpAddress}:{ctx.Request.Source.Port}");
        
        DataTable ret = await TemplatePostMethod(ctx, "NoAuthRequestAsync");

        using var sql = new SqlServerCall(conStr);

        await sql.BulkInsert(ret, "teste", "DWExtract");
    }

    private static async Task<DataTable> TemplatePostMethod(HttpContextBase ctx, string method)
    {
        string res = "";
        DataTable ret = new();

        try
        {
            if (!JsonValidate.IsValid(ctx.Request.DataAsString)) throw new Exception();
            try
            {
                var req = JsonSerializer.Deserialize<BodyDefault>(ctx.Request.DataAsString);
                using var client = new HttpClient();
                using var tasker = new HttpSender(req!.ConnectionInfo, client);
                
                MethodInfo execute = typeof(HttpSender).GetMethod(method)!;

                dynamic data = await Task<dynamic>.Factory.StartNew(() => execute.Invoke(tasker, null)!).Result;

                ret = DynamicObjConvert.JsonDynamicToDataTable(data);

                res = JsonSerializer.Serialize(new Response() {
                    Error = false,
                    Status = 201,
                    Message = "Schedule was successfully triggered in the server.",
                    Options = []
                });

                ctx.Response.StatusCode = 201;
            }
            catch (Exception ex)
            {
                res = JsonSerializer.Serialize(new Response() {
                    Error = true,
                    Status = 500,
                    Message = "An internal serval error occurred which stopped the completion of the request.",
                    Options = []
                });
                Log.Out($"Error occured after request {ex.Message}");
                ctx.Response.StatusCode = 500;            
            }
        }
        catch
        {
            res = JsonSerializer.Serialize(new Response() {
                Error = true,
                Status = 400,
                Message = "Bad Request",
                Options = []
            });
            ctx.Response.StatusCode = 400;  
        }
        finally
        {
            Log.Out($"Response was: {ctx.Response.ResponseSent}");
            await ctx.Response.Send(res);
        }

        return ret;
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