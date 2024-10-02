using System.Data;
using WatsonWebserver.Core;
using System.Text.Json.Nodes;
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
    public static async Task StouEspelho(HttpContextBase ctx,
                                              string conStr,
                                              string database,
                                              int threadPagination,
                                              int threadTimeout,
                                              HttpSender sender) 
    {
        var request = RequestValidate.GetDeserialized<BodyDefault>(ctx.Request.DataAsString);
        if (!request.IsOk) {
            await Response.BadRequest(ctx);
            return;
        }
        
        using var requestBody = request.Value;
        
        if (!int.TryParse(requestBody.Options[4], out int lookBackTime)) {
            await Response.BadRequest(ctx);
            return;
        }
        var filteredDate = $"{DateTime.Today.AddDays(-lookBackTime):dd/MM/yyyy}";
        
        // Realiza-se primeira chamada para resgatar quantidade de páginas.
        var firstReturn = await RestTemplate.TemplateRequestHandler(ctx, sender, "SimpleAuthBodyRequestAsync", [
            BuildPayload(
                requestBody.Options, 
                requestBody.DestinationTableName, 
                filteredDate, 
                ["dtde", "dtate"], 
                1
            ), 
            System.Net.Http.HttpMethod.Post,
            request.Value.ConnectionInfo
        ]);
        if (!firstReturn.IsOk) {
            await Response.InternalServerError(ctx);
            Log.Out($"Error while attempting to send request, {firstReturn.Error.ExceptionMessage}");
        }

        Result<DataTable, Error> tableAttempt = DynamicObjConvert.FromInnerJsonToDataTable(firstReturn.Value, "itens");
        if (!tableAttempt.IsOk) {
            await Response.InternalServerError(ctx);
            Log.Out($"Error while attempting to process JSON return, {tableAttempt.Error.ExceptionMessage}");
        }

        using DataTable table = tableAttempt.Value;
        tableAttempt.Value.Dispose();
        table.Rows.Clear();

        SqlServerCall firstCall = new(conStr);
        await firstCall.CreateTable(table, requestBody.DestinationTableName, requestBody.SysName, database);
        firstCall.Dispose();

        Result<JsonObject, Error> firstJson = RequestValidate.GetDeserialized<JsonObject>(firstReturn.Value);
        if (!firstJson.IsOk) {
            await Response.InternalServerError(ctx);
            Log.Out($"Error while attempting to send request, {firstJson.Error.ExceptionMessage}");
        }

        int pageCount = firstJson.Value["totalCount"]?.GetValue<int>() ?? 0;

        Log.Out(
            $"Starting extraction job {ctx.Request.Guid} for {requestBody.DestinationTableName}\n" +
            $"  - Looking back since: {filteredDate}\n" +
            $"  - Page count: {pageCount}"
        );

        // Extração em multi-thread da API.
        // Realiza-se chamada em threads para cada página até o limite em variável de ambiente.
        const string ApiAuthMethod = "SimpleAuthBodyRequestAsync";
        const string InnerProp = "itens";
        var extract = await PaginatedExtractTemplate.PaginatedApiToSqlDatabase(
            ctx,
            sender,
            conStr,
            requestBody,
            table,
            page => BuildPayload(
                requestBody.Options, 
                requestBody.DestinationTableName, 
                filteredDate, 
                ["dtde", "dtate"], 
                page
            ),
            threadPagination,
            pageCount,
            ApiAuthMethod,
            InnerProp,
            threadTimeout,
            request.Value.ConnectionInfo,
            database
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
        var request = RequestValidate.GetDeserialized<BodyDefault>(ctx.Request.DataAsString);
        if (!request.IsOk) {
            await Response.BadRequest(ctx);
            return;
        }
        
        using var requestBody = request.Value;
        
        if (!int.TryParse(requestBody.Options[4], out int lookBackTime)) {
            await Response.BadRequest(ctx);
            return;
        }
        var filteredDate = $"{DateTime.Today.AddDays(-lookBackTime):dd/MM/yyyy}";

        Log.Out(
            $"Starting extraction job {ctx.Request.Guid} for {requestBody.DestinationTableName}\n" +
            $"  - Looking back since: {filteredDate}"
        );

        Result<dynamic, Error> res = await RestTemplate.TemplateRequestHandler(ctx, sender, "SimpleAuthBodyRequestAsync", [
            BuildPayload(
                requestBody.Options, 
                requestBody.DestinationTableName, 
                filteredDate, 
                ["dtinicio", "dtfim"]
            ), 
            System.Net.Http.HttpMethod.Post,
            request.Value.ConnectionInfo
        ]);
        if (!res.IsOk) {
            await Response.InternalServerError(ctx);
            Log.Out($"Error while attempting to send request, {res.Error.ExceptionMessage}");
        }

        Result<DataTable, Error> tableAttempt = DynamicObjConvert.FromInnerJsonToDataTable(res.Value, "itens");
        if (!tableAttempt.IsOk) {
            await Response.InternalServerError(ctx);
            Log.Out($"Error while attempting to process JSON return, {tableAttempt.Error.ExceptionMessage}");
        }

        using DataTable table = tableAttempt.Value;
        tableAttempt.Value.Dispose();
        
        using SqlServerCall serverCall = new(conStr);
        var create = await serverCall.CreateTable(table, requestBody.DestinationTableName, requestBody.SysName, database);
        if (!create.IsOk) {
            await Response.InternalServerError(ctx);
            Log.Out($"Error while attempting to create table in server, {create.Error.ExceptionMessage}");
        }

        var insert = await serverCall.BulkInsert(table, requestBody.DestinationTableName, requestBody.SysName, database);
        if (!insert.IsOk) {
            await Response.InternalServerError(ctx);
            Log.Out($"Error while attempting to send data to server, {insert.Error.ExceptionMessage}");
        }

        Log.Out(
            $"Extraction job {ctx.Request.Guid} has been completed."
        );

        await Response.Ok(ctx);
    }

    public static async Task StouBasic(HttpContextBase ctx, string conStr, string database, HttpSender sender)
    {
        var request = RequestValidate.GetDeserialized<BodyDefault>(ctx.Request.DataAsString);
        if (!request.IsOk) {
            await Response.BadRequest(ctx);
            return;
        }
        
        using var requestBody = request.Value;

        Log.Out(
            $"Starting extraction job {ctx.Request.Guid} for {requestBody.DestinationTableName}\n"
        );

        Result<dynamic, Error> res = await RestTemplate.TemplateRequestHandler(ctx, sender, "SimpleAuthBodyRequestAsync", [
            BuildPayload(
                requestBody.Options, 
                requestBody.DestinationTableName, 
                "01/01/1900", 
                ["a1", "a2"]
            ), 
            System.Net.Http.HttpMethod.Post,
            request.Value.ConnectionInfo
        ]);
        if (!res.IsOk) {
            await Response.InternalServerError(ctx);
            Log.Out($"Error while attempting to send request, {res.Error.ExceptionMessage}");
        }

        Result<DataTable, Error> tableAttempt = DynamicObjConvert.FromInnerJsonToDataTable(res.Value, "itens");
        if (!tableAttempt.IsOk) {
            await Response.InternalServerError(ctx);
            Log.Out($"Error while attempting to process JSON return, {tableAttempt.Error.ExceptionMessage}");
        }

        using DataTable table = tableAttempt.Value;
        tableAttempt.Value.Dispose();
        
        using SqlServerCall serverCall = new(conStr);
        var create = await serverCall.CreateTable(table, requestBody.DestinationTableName, requestBody.SysName, database);
        if (!create.IsOk) {
            await Response.InternalServerError(ctx);
            Log.Out($"Error while attempting to create table in server, {create.Error.ExceptionMessage}");
        }

        var insert = await serverCall.BulkInsert(table, requestBody.DestinationTableName, requestBody.SysName, database);
        if (!insert.IsOk) {
            await Response.InternalServerError(ctx);
            Log.Out($"Error while attempting to send data to server, {insert.Error.ExceptionMessage}");
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