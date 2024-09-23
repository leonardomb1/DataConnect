using DataConnect.Shared;
using DataConnect.Types;
using DataConnect.Models;
using DataConnect.Controller;
using WatsonWebserver.Core;
using DataConnect.Etl.Templates;
using System.Text.Json;

namespace DataConnect.Routes;

public static class DBDataTransfer
{
    public static async Task<int> ScheduledMssql(HttpContextBase ctx, string conStr, int threadPagination, int packetSize)
    {       
        Result<BodyDefault, int> request = await RestTemplate.RequestStart(ctx);
        if (!request.IsOk) return Constants.MethodFail;
        
        using var requestBody = request.Value;

        if (!int.TryParse(requestBody.Options[0], out int scheduleId)) 
            return Constants.MethodFail;
        
        if (!int.TryParse(requestBody.Options[1], out int systemId)) 
            return Constants.MethodFail;

        var exchange = await MssqlDataTransfer.ExchangeData(conStr, scheduleId, systemId, threadPagination, packetSize);

        if (exchange != Constants.MethodFail) {      
            string res = JsonSerializer.Serialize(new Response() {
                Error = false,
                Message = "The schedule ran successfully in the server.",
                Status = 201,
                Options = []
            });

            ctx.Response.StatusCode = 201;
            await ctx.Response.Send(res);
            
            return Constants.MethodSuccess;
        } else {
            string res = JsonSerializer.Serialize(new Response() {
                Error = true,
                Message = "The schedule has failed in the server.",
                Status = 501,
                Options = []
            });

            ctx.Response.StatusCode = 501;
            await ctx.Response.Send(res);
            
            return Constants.MethodFail;
        }
    }
}