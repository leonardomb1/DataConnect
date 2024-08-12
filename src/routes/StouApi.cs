using DataConnect.Shared;
using DataConnect.Etl.Sql;
using DataConnect.Controller;
using DataConnect.Shared.Converter;
using WatsonWebserver.Core;
using System.Data;
using DataConnect.Types;
using DataConnect.Models;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace DataConnect.Routes;

public static class StouApi
{
    public static async Task<int> StouEspelho(HttpContextBase ctx, string conStr, string database) 
    {
        Result<BodyDefault, int> requestResult = await RestTemplate.RequestStart(ctx);
        if (!requestResult.IsOk) return ReturnedValues.MethodFail;
        
        var obj = requestResult.Value;
        if (obj.Options.Length <= 5 || !int.TryParse(obj.Options[5], out int lookBackTime)) 
            return ReturnedValues.MethodFail;
        
        var filteredDate =
            obj.Options[4] == "Incremental" ?  
            $"{DateTime.Today.AddDays(-lookBackTime):dd/MM/yyyy}" :
            $"{DateTime.Today.AddDays(-lookBackTime):dd/MM/yyyy}";
        
        using var client = new HttpClient();

        Result<dynamic, int> firstReturn = await RestTemplate.TemplatePostMethod(ctx, client, "SimpleAuthBodyRequestAsync", [
            BuildPayload(obj.Options, obj.DestinationTableName, filteredDate, 1)
        ]);
        if (!firstReturn.IsOk) return ReturnedValues.MethodFail;

        using DataTable table = DynamicObjConvert.FromInnerJsonToDataTable(firstReturn.Value, "itens");
        table.Rows.Clear();

        JsonObject firstJson = JsonSerializer.Deserialize<JsonObject>(firstReturn.Value);
        int pageCount = firstJson["totalCount"]?.GetValue<int>() ?? 0;

        List<Task> tasks = [];

        Log.Out(
            $"Starting extraction job {ctx.Request.Guid} for {obj.DestinationTableName}\n" +
            $"  - Looking back since: {filteredDate}\n" +
            $"  - Page count: {pageCount}\n" +
            $"  - Estimated size: {pageCount * lookBackTime} lines"
        );

        using var semaphore = new SemaphoreSlim(Environment.ProcessorCount);
        using var sql = new SqlServerCall(conStr);
        
        for (int pageIter = 1; pageIter <= pageCount; pageIter += Environment.ProcessorCount + 1)
        {
            for (int i = 0; i <= Environment.ProcessorCount; i++)
            {
                int page = pageIter + i;
                await semaphore.WaitAsync();
                tasks.Add(
                    Task.Run(async () => {
                        return await RestTemplate.TemplatePostMethod(ctx, client, "SimpleAuthBodyRequestAsync", [
                           BuildPayload(obj.Options, obj.DestinationTableName, filteredDate, page)
                        ]);
                    }).ContinueWith(thread => {
                        if(thread.IsFaulted || (thread.IsCompleted & !thread.Result.IsOk)) {
                            Log.Out(
                                $"Thread ID {thread.Id}, at status {thread.Status} " + 
                                $"returned error: {thread.Exception?.Message ?? "Generic Bad Request Handled Error"}"
                            );
                        } else {
                            var res = thread.Result.Value;
                            using DataTable data = DynamicObjConvert.FromInnerJsonToDataTable(res, "itens");
                            table.Merge(data, true, MissingSchemaAction.Ignore);
                        }
                        thread.Dispose();
                        semaphore.Release();
                }));    
            }
            await Task.WhenAll(tasks);
            
            await sql.CreateTable(table, obj.DestinationTableName, obj.SysName, database);
            await sql.BulkInsert(table, obj.DestinationTableName, obj.SysName, database);

            tasks.Clear();
            table.Rows.Clear();

            await Task.Delay(500);
        }

        Log.Out(
            $"Extraction job {ctx.Request.Guid} has succeeded."
        );

        return ReturnedValues.MethodSuccess;
    }
    private static List<KeyValuePair<string, string>> BuildPayload(string[] options,
                                                                   string destinationTableName,
                                                                   string filteredDate,
                                                                   int page) =>
        [
            KeyValuePair.Create($"{options[0]}", $"{options[1]}"),
            KeyValuePair.Create($"{options[2]}", Encryption.Sha256($"{options[3]}{DateTime.Today:dd/MM/yyyy}")),
            KeyValuePair.Create("pag", destinationTableName),
            KeyValuePair.Create("cmd", "get"),
            KeyValuePair.Create("dtde", filteredDate),
            KeyValuePair.Create("dtate", $"{DateTime.Today:dd/MM/yyyy}"),
            KeyValuePair.Create("start", "1"),
            KeyValuePair.Create("page", $"{page}")
        ];
}