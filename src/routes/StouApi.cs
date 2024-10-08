using System.Data;
using WatsonWebserver.Core;
using DataConnect.Etl.Sql;
using DataConnect.Etl.Converter;
using DataConnect.Etl.Templates;
using DataConnect.Etl.Http;
using DataConnect.Models;
using DataConnect.Shared;
using DataConnect.Controller;
using DataConnect.Types;
using DataConnect.Validator;

namespace DataConnect.Routes;

public static class StouApi
{
    private static readonly string _innerJsonName = "itens";
    public static async Task StouEspelho(HttpContextBase ctx,
                                              string conStr,
                                              string database,
                                              int threadPagination,
                                              int threadTimeout,
                                              HttpSender sender) 
    {
        Log.Out(
            $"Receiving {ctx.Request.Method} request for {ctx.Request.Url.RawWithoutQuery} " + 
            $"by {ctx.Request.Source.IpAddress}:{ctx.Request.Source.Port}"
        ); 

        var request = RequestValidate.GetDeserialized<BodyDefault>(ctx.Request.DataAsString);
        if (!request.IsOk) {
            await Response.BadRequest(ctx);
            return;
        }
              
        if (!int.TryParse(request.Value.Options[4], out int lookBackTime)) {
            await Response.BadRequest(ctx);
            return;
        }
        var filteredDate = $"{DateTime.Today.AddDays(-lookBackTime):dd/MM/yyyy}";

        var firstReturn = await RestTemplate.TemplateRequestHandler(
            ctx,
            async () => await sender.SimpleAuthBodyRequestAsync(
                BuildPayload(
                    request.Value.Options, 
                    request.Value.DestinationTableName, 
                    filteredDate, 
                    ["dtde", "dtate"], 
                    1
                ),
                System.Net.Http.HttpMethod.Post,
                request.Value.ConnectionInfo
            )
        );
        if (!firstReturn.IsOk) {
            await Response.InternalServerError(ctx);
            Log.Out($"Error while attempting to send request, {firstReturn.Error.ExceptionMessage}");
        }
        var test = firstReturn.Value[_innerJsonName]!;
        var table = DynamicObjConvert.JsonToDataTable(test);
        if (!table.IsOk) {
            await Response.InternalServerError(ctx);
            Log.Out($"Error while attempting to process JSON return, {table.Error.ExceptionMessage}");
            return;
        }
        table.Value.Clear();

        SqlServerCall firstCall = new(conStr);
        var create = await firstCall.CreateTable(table.Value, request.Value.DestinationTableName, request.Value.SysName, database);
        if (!create.IsOk) {
            await Response.InternalServerError(ctx);
            Log.Out($"Error while attempting to send request, {create.Error.ExceptionMessage}");
            return;
        }
        firstCall.Dispose();

        int pageCount = firstReturn.Value["totalCount"]?.GetValue<int>() ?? 0;

        Log.Out(
            $"Starting extraction job {ctx.Request.Guid} for {request.Value.DestinationTableName}\n" +
            $"  - Looking back since: {filteredDate}\n" +
            $"  - Page count: {pageCount}"
        );

        // Extração em multi-thread da API.
        // Realiza-se chamada em threads para cada página até o limite em variável de ambiente.
        var extract = await PaginatedExtractTemplate.PaginatedApiToSqlDatabase(
            ctx,
            async (page) => await sender.SimpleAuthBodyRequestAsync(
                BuildPayload(
                    request.Value.Options, 
                    request.Value.DestinationTableName, 
                    filteredDate, 
                    ["dtde", "dtate"],
                    page
                ),
                System.Net.Http.HttpMethod.Post,
                request.Value.ConnectionInfo
            ),
            table.Value,
            conStr,
            _innerJsonName,
            request.Value.SysName,
            request.Value.DestinationTableName,
            pageCount,
            threadPagination,
            threadTimeout
        );

        if(!extract.IsOk) {
            await Response.MultiStatus(
                ctx,
                [
                    new Response() {
                        Error = false,
                        Message = "OK",
                        Status = 200
                    },
                    new Response() {
                        Error = true,
                        Message = $"Error while attempting extraction : {extract.Error.ExceptionMessage}, occured {extract.Error.ErrorCount} times",
                        Status = 500
                    }
                ]
            );
            Log.Out($"The extraction job has finished with errors, total error count {extract.Error.ErrorCount}");
            return;
        }

        Log.Out(
            $"Extraction job {ctx.Request.Guid} has been completed."
        );

        await Response.Ok(ctx);
    }

    public static async Task StouAssinaturaEspelho(HttpContextBase ctx, string conStr, string database, HttpSender sender)
    {
        Log.Out(
            $"Receiving {ctx.Request.Method} request for {ctx.Request.Url.RawWithoutQuery} " + 
            $"by {ctx.Request.Source.IpAddress}:{ctx.Request.Source.Port}"
        ); 

        var request = RequestValidate.GetDeserialized<BodyDefault>(ctx.Request.DataAsString);
        if (!request.IsOk) {
            await Response.BadRequest(ctx);
            return;
        }
              
        if (!int.TryParse(request.Value.Options[4], out int lookBackTime)) {
            await Response.BadRequest(ctx);
            return;
        }

        var filteredDate = $"{DateTime.Today.AddDays(-lookBackTime):dd/MM/yyyy}";

        Log.Out(
            $"Starting extraction job {ctx.Request.Guid} for {request.Value.DestinationTableName}\n" +
            $"  - Looking back since: {filteredDate}"
        );

        var res = await RestTemplate.TemplateRequestHandler(
            ctx,
            async () => await sender.SimpleAuthBodyRequestAsync(
                BuildPayload(
                    request.Value.Options, 
                    request.Value.DestinationTableName, 
                    filteredDate, 
                    ["dtinicio", "dtfim"]
                ),
                System.Net.Http.HttpMethod.Post,
                request.Value.ConnectionInfo
            )
        );
        if (!res.IsOk) {
            await Response.InternalServerError(ctx);
            Log.Out($"Error while attempting to send request, {res.Error.ExceptionMessage}");
            return;
        }

        Result<DataTable, Error> table = DynamicObjConvert.JsonToDataTable(res.Value[_innerJsonName]!.AsObject());
        if (!table.IsOk) {
            await Response.InternalServerError(ctx);
            Log.Out($"Error while attempting to process JSON return, {table.Error.ExceptionMessage}");
            return;
        }

        using SqlServerCall serverCall = new(conStr);
        var create = await serverCall.CreateTable(table.Value, request.Value.DestinationTableName, request.Value.SysName, database);
        if (!create.IsOk) {
            await Response.InternalServerError(ctx);
            Log.Out($"Error while attempting to create table in server, {create.Error.ExceptionMessage}");
            return;
        }

        var insert = await serverCall.BulkInsert(table.Value, request.Value.DestinationTableName, request.Value.SysName, database);
        if (!insert.IsOk) {
            await Response.InternalServerError(ctx);
            Log.Out($"Error while attempting to send data to server, {insert.Error.ExceptionMessage}");
            return;
        }

        Log.Out(
            $"Extraction job {ctx.Request.Guid} has been completed."
        );

        await Response.Ok(ctx);
    }

    public static async Task StouBasic(HttpContextBase ctx, string conStr, string database, HttpSender sender)
    {
        Log.Out(
            $"Receiving {ctx.Request.Method} request for {ctx.Request.Url.RawWithoutQuery} " + 
            $"by {ctx.Request.Source.IpAddress}:{ctx.Request.Source.Port}"
        ); 

        var request = RequestValidate.GetDeserialized<BodyDefault>(ctx.Request.DataAsString);
        if (!request.IsOk) {
            await Response.BadRequest(ctx);
            return;
        }
        
        Log.Out(
            $"Starting extraction job {ctx.Request.Guid} for {request.Value.DestinationTableName}\n"
        );

       var res = await RestTemplate.TemplateRequestHandler(
            ctx,
            async () => await sender.SimpleAuthBodyRequestAsync(
                BuildPayload(
                    request.Value.Options, 
                    request.Value.DestinationTableName, 
                    "01/01/1900", 
                    ["a1", "a2"]
                ), 
                System.Net.Http.HttpMethod.Post,
                request.Value.ConnectionInfo
            )
        );
        if (!res.IsOk) {
            await Response.InternalServerError(ctx);
            Log.Out($"Error while attempting to send request, {res.Error.ExceptionMessage}");
            return;
        }

        Result<DataTable, Error> table = DynamicObjConvert.JsonToDataTable(res.Value[_innerJsonName]!.AsObject());
        if (!table.IsOk) {
            await Response.InternalServerError(ctx);
            Log.Out($"Error while attempting to process JSON return, {table.Error.ExceptionMessage}");
            return;
        }
        
        using SqlServerCall serverCall = new(conStr);
        var create = await serverCall.CreateTable(table.Value, request.Value.DestinationTableName, request.Value.SysName, database);
        if (!create.IsOk) {
            await Response.InternalServerError(ctx);
            Log.Out($"Error while attempting to create table in server, {create.Error.ExceptionMessage}");
            return;
        }

        var insert = await serverCall.BulkInsert(table.Value, request.Value.DestinationTableName, request.Value.SysName, database);
        if (!insert.IsOk) {
            await Response.InternalServerError(ctx);
            Log.Out($"Error while attempting to send data to server, {insert.Error.ExceptionMessage}");
            return;
        }
        Log.Out(
            $"Extraction job {ctx.Request.Guid} has been completed."
        );

        await Response.Ok(ctx);
    }

    private static List<KeyValuePair<string, string>> BuildPayload(string[] options,
                                                                   string destinationTableName,
                                                                   string filteredDate,
                                                                   string[] nameSchema,
                                                                   int? page = null) =>
        [
            KeyValuePair.Create(options[0], options[1]),
            KeyValuePair.Create(options[2], Encryption.Sha256($"{options[3]}{DateTime.Today:dd/MM/yyyy}")),
            KeyValuePair.Create("pag", destinationTableName),
            KeyValuePair.Create("cmd", "get"),
            KeyValuePair.Create(nameSchema[0], filteredDate),
            KeyValuePair.Create(nameSchema[1], $"{DateTime.Today:dd/MM/yyyy}"),
            KeyValuePair.Create("start", "1"),
            KeyValuePair.Create("page", $"{page}")
        ];
}