using DataConnect.Shared;
using DataConnect.Models;
using WatsonWebserver.Core;
using WatsonWebserver.Lite;
using DataConnect.Routes;
using DataConnect.Etl.Http;
using DataConnect.Validator;

namespace DataConnect.Controller;

public class Server : IDisposable
{
    private bool _disposed;
    private readonly int _port;
    private readonly bool _ssl = false;
    private readonly WebserverLite _server;

    public Server(int port,
                  string conStr,
                  string database,
                  int threadPagination,
                  int threadTimeout,
                  int packetSize,
                  string authSecret,
                  int maxTableCount,
                  IHttpClientFactory clientFactory)
    {
        _port = port;

        _server = new WebserverLite(new WebserverSettings("*", _port, _ssl), NotFound);
        _server.Routes.AuthenticateRequest = (HttpContextBase ctx) => Authenticate(ctx, authSecret);
        _server.Routes.PostAuthentication.Static.Add(WatsonWebserver.Core.HttpMethod.GET, "/api", GetRoutes);
        _server.Routes.PostAuthentication.Static.Add(WatsonWebserver.Core.HttpMethod.POST, "/api/custom/ponto_espelho", (HttpContextBase ctx) => {
                var httpSender = new HttpSender(clientFactory);
                return StouApi.StouEspelho(ctx, conStr, database, threadPagination, threadTimeout, httpSender);
        });
        _server.Routes.PostAuthentication.Static.Add(WatsonWebserver.Core.HttpMethod.POST, "/api/custom/configuracao_competencia", (HttpContextBase ctx) => {
                var httpSender = new HttpSender(clientFactory);
                return StouApi.StouBasic(ctx, conStr, database, httpSender);
        });
        _server.Routes.PostAuthentication.Static.Add(WatsonWebserver.Core.HttpMethod.POST, "/api/custom/ponto_assinatura_espelho", (HttpContextBase ctx) => {
                var httpSender = new HttpSender(clientFactory);
                return StouApi.StouAssinaturaEspelho(ctx, conStr, database, httpSender);
        });
        _server.Routes.PostAuthentication.Static.Add(WatsonWebserver.Core.HttpMethod.POST, "/api/sql", async (HttpContextBase ctx) => {
                await DBDataTransfer.ScheduledMssql(ctx, conStr, maxTableCount, packetSize, authSecret);
        });
        _server.Routes.PostAuthentication.Parameter.Add(WatsonWebserver.Core.HttpMethod.GET, "/api/sql/{systemId}/{scheduleId}", async (HttpContextBase ctx) => {
                await DBDataTransfer.GetScheduleByScheduleId(ctx, conStr);
        });
    }

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
        
        await Response.NotFound(ctx);
        
        Log.Out($"Response to {ctx.Guid} was: {ctx.Response.StatusCode} - {ctx.Response.StatusDescription}");
    }

    public static async Task Authenticate(HttpContextBase ctx, string authSecret)
    {
        string validate = Encryption.Sha256(authSecret + $"{DateTime.Today:dd/MM/yyyy}");
        try
        {
            if(!RequestValidate.ContainsCorrectHeader(ctx, ["token"])) await NotAuthorized(ctx);
            string token = ctx.Request.Headers.GetValues("token")![0];
            if (token != validate) await NotAuthorized(ctx);
        }
        catch (Exception)
        {
            await NotAuthorized(ctx);
        }
    }

    private static async Task NotAuthorized(HttpContextBase ctx)
    {
        await Response.Unauthorized(ctx);
        Log.Out($"Unauthorized request by {ctx.Request.Source.IpAddress}:{ctx.Request.Source.Port}.");
    }

    private static async Task GetRoutes(HttpContextBase ctx) 
    {
        Log.Out(
            $"Receiving {ctx.Request.Method} request for {ctx.Request.Url.RawWithoutQuery} " +
            $"by {ctx.Request.Source.IpAddress}:{ctx.Request.Source.Port}"
        );
        
        string[] routes = [
            "POST /api/custom/ponto_espelho",
            "POST /api/custom/configuracao_competencia",
            "POST /api/custom/ponto_assinatura_espelho",
            "POST /api/sql",
            "GET  /api"
        ];
        
        await Response.SendAsString(ctx, false, "Refer to the following options.", 200, null, routes);
        
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