using DataConnect.Shared;
using DataConnect.Types;
using DataConnect.Models;
using DataConnect.Controller;
using WatsonWebserver.Core;
using DataConnect.Etl.Templates;

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

        await MssqlDataTransfer.ExchangeData(conStr, scheduleId, systemId, threadPagination, packetSize);

        
        return Constants.MethodSuccess;
    }
}