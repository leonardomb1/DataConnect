using WatsonWebserver.Core;
using DataConnect.Shared;
using DataConnect.Models;
using DataConnect.Etl.Templates;
using DataConnect.Etl.Sql;
using DataConnect.Validator;

namespace DataConnect.Routes;

public static class DBDataTransfer
{
    public static async Task ScheduledMssql(HttpContextBase ctx, string conStr, int maxTableCount, int packetSize, string authSecret)
    {
        Log.Out(
            $"Receiving {ctx.Request.Method} request for {ctx.Request.Url.RawWithoutQuery} " + 
            $"by {ctx.Request.Source.IpAddress}:{ctx.Request.Source.Port}"
        ); 

        var request = RequestValidate.GetDeserialized<BodyScheduled>(ctx.Request.DataAsString);
        if (!request.IsOk) {
            await Response.BadRequest(ctx);
            return;
        }
        
        using var requestBody = request.Value;

        if (!int.TryParse(requestBody.ScheduleId.ToString(), out int scheduleId)) {
            await Response.BadRequest(ctx);
            return;
        }
        
        if (!int.TryParse(requestBody.SystemId.ToString(), out int systemId)) {
            await Response.BadRequest(ctx);
            return;
        }

        var exchangeAttempt = await MssqlDataTransfer.ExchangeData(conStr, requestBody.ScheduleId, requestBody.SystemId, maxTableCount, packetSize, authSecret);
        
        if (!exchangeAttempt.IsOk) {
            if (exchangeAttempt.Error.IsPartialSuccess ?? false) {
                await Response.MultiStatus(
                    ctx, 
                    [
                        new Response() { 
                            Error = false, 
                            Message = "OK",
                            Status = 200,
                        },
                        new Response() { 
                            Error = true, 
                            Message = exchangeAttempt.Error.ExceptionMessage,
                            Status = 500,
                        },
                    ]
                );
                return;
            } else {
                await Response.InternalServerError(ctx);
                return;
            }
        }

        await Response.Ok(ctx);
    }

    public static async Task GetScheduleByScheduleId(HttpContextBase ctx, string conStr)
    {
        Log.Out(
            $"Receiving {ctx.Request.Method} request for {ctx.Request.Url.RawWithoutQuery} " + 
            $"by {ctx.Request.Source.IpAddress}:{ctx.Request.Source.Port}"
        );

        string systemId = ctx.Request.Url.Parameters["systemId"] ?? "n/a";
        if (systemId == "n/a") {
            await Response.BadRequest(ctx);
            return;
        }
        
        string scheduleId = ctx.Request.Url.Parameters["scheduleId"] ?? "n/a";
        if (scheduleId == "n/a") {
            await Response.BadRequest(ctx);
            return;
        }
        
        if(!int.TryParse(systemId, out int parsedSystemId) || !int.TryParse(scheduleId, out int parsedScheduleId)) {
            await Response.BadRequest(ctx);
            return;
        }

        using SqlServerCall serverCall = new(conStr);

        var getDataAttempt = await serverCall.GetTableDataFromServer(
            @$"SELECT 
                    EXT.*,
                    SI.NM_SISTEMA,
                    SI.DS_CONSTRING
                FROM DWController..DW_EXTLIST AS EXT WITH(NOLOCK)
                INNER JOIN DWController..DW_SISTEMAS AS SI WITH(NOLOCK)
                    ON  SI.ID_DW_SISTEMA = EXT.ID_DW_SISTEMA
                WHERE   EXT.ID_DW_SISTEMA = {parsedSystemId} AND
                        EXT.ID_DW_AGENDADOR = {parsedScheduleId};
            "
        );
        if (!getDataAttempt.IsOk) {
            await Response.InternalServerError(ctx);
            return;
        }

        List<ExtractionMetadata> metadata = ExtractionMetadata.ConvertFromDataTable(getDataAttempt.Value);

        await Response.SendAsString(ctx, false, "OK", 200, [.. metadata]);
        metadata.ForEach(x => x.Dispose());
        metadata.Clear();
    }
}