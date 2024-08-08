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
    public static async Task<int> PostPontoEspelho(HttpContextBase ctx, string conStr) 
    {
        Result<BodyDefault, int> requestResult = await RestTemplate.RequestStart(ctx);
        if (!requestResult.IsOk) return ReturnedValues.MethodFail;
        
        var obj = requestResult.Value;

        var filteredDate =
            obj.Options[4] == "Incremental" ?  
            $"{DateTime.Today.AddDays(-4):dd/MM/yyyy}" :
            $"{DateTime.Today.AddDays(-410):dd/MM/yyyy}";

        var firstList = new List<KeyValuePair<string, string>>
        {
            KeyValuePair.Create("pag", $"{obj.DestinationTableName}"),
            KeyValuePair.Create("cmd", "get"),
            KeyValuePair.Create("dtde", $"{filteredDate}"),
            KeyValuePair.Create("dtate", $"{DateTime.Today:dd/MM/yyyy}"),
            KeyValuePair.Create("start", "1"),
            KeyValuePair.Create("page", "1"),
        };

        Result<dynamic, int> firstReturn = await RestTemplate.TemplatePostMethod(ctx, "SimpleAuthBodyRequestAsync", [
            KeyValuePair.Create($"{obj.Options[0]}", $"{obj.Options[1]}"),
            KeyValuePair.Create($"{obj.Options[2]}", Encryption.Sha256($"{obj.Options[3]}{DateTime.Today:dd/MM/yyyy}")),
            firstList
        ]);
        if (!firstReturn.IsOk) return ReturnedValues.MethodFail;
        firstList.Clear();

        JsonObject firstJson = JsonSerializer.Deserialize<JsonObject>(firstReturn.Value);
        int pageCount = firstJson["totalCount"]?.GetValue<int>() ?? 0;

        List<Task> tasks = [];

        using DataTable table = new();
        using var semaphore = new SemaphoreSlim(Environment.ProcessorCount);
        using var sql = new SqlServerCall(conStr);
        
        for (int pageIter = 1; pageIter <= pageCount; pageIter += Environment.ProcessorCount + 1)
        {
            for (int i = 0; i <= Environment.ProcessorCount; i++)
            {
                if (pageIter >= pageCount) break;
                int page = pageIter + i;
                
                await semaphore.WaitAsync();
                tasks.Add(
                    Task.Run<Result<dynamic, int>>(async () => {
                        var list = new List<KeyValuePair<string, string>>
                        {
                            KeyValuePair.Create("pag", $"{obj.DestinationTableName}"),
                            KeyValuePair.Create("cmd", "get"),
                            KeyValuePair.Create("dtde", filteredDate),
                            KeyValuePair.Create("dtate", $"{DateTime.Today:dd/MM/yyyy}"),
                            KeyValuePair.Create("start", "1"),
                            KeyValuePair.Create("page", $"{page}"),
                        };
                        Result<dynamic, int> job = await RestTemplate.TemplatePostMethod(ctx, "SimpleAuthBodyRequestAsync", [
                            KeyValuePair.Create($"{obj.Options[0]}", $"{obj.Options[1]}"),
                            KeyValuePair.Create($"{obj.Options[2]}", Encryption.Sha256($"{obj.Options[3]}{DateTime.Today:dd/MM/yyyy}")),
                            list
                        ]);
                        return job;
                    }).ContinueWith(thread => {
                        if(thread.IsFaulted || (thread.IsCompleted & !thread.Result.IsOk)) {
                            Log.Out($"Thread ID {thread.Id}, at status {thread.Status} returned error: {thread.Exception?.Message ?? "Generic Bad Request Handled Error"}");
                        } else {
                            using DataTable parsedData = DynamicObjConvert.FromInnerJsonToDataTable(thread.Result.Value, "itens");
                            table.Merge(parsedData);
                        }
                        thread.Dispose();
                        semaphore.Release();
                }));    
            }
            await Task.WhenAll(tasks);
            tasks.Clear();
            
            await Task.Delay(500);
            await sql.CreateTable(obj.DestinationTableName, table, obj.SysName, "DWExtract");
            await sql.BulkInsert(table, obj.DestinationTableName, obj.SysName, "DWExtract");
            table.Clear();
        }

        return ReturnedValues.MethodSuccess;
    }
}