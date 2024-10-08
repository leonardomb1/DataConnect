using System.Data;
using System.Threading.Channels;
using DataConnect.Etl.Sql;
using DataConnect.Shared;
using DataConnect.Types;
using DataConnect.Controller;
using DataConnect.Models;
using WatsonWebserver.Core;
using System.Text.Json.Nodes;
using DataConnect.Etl.Converter;

namespace DataConnect.Etl.Templates;

public static class PaginatedExtractTemplate
{
    public static async Task<Result<int, Error>> PaginatedApiToSqlDatabase(HttpContextBase ctx,
                                                                           Func<int, Task<Result<JsonObject, Error>>> getter,
                                                                           DataTable table,
                                                                           string conStr,
                                                                           string innerProp,
                                                                           string sysName,
                                                                           string tableName,
                                                                           int pageCount,
                                                                           int threadPagination,
                                                                           int threadTimeout)
    {
        using var serverCall = new SqlServerCall(conStr);
        var options = new ParallelOptions { MaxDegreeOfParallelism = threadPagination };
        var channel = Channel.CreateBounded<DataTable>(capacity: threadPagination);
        int errCount = 0, passCountFetch = 0;

        var fetch = Task.Run(async () => {
            await Parallel.ForEachAsync(Enumerable.Range(1, pageCount), options, async (page, token) => {
                Result<JsonObject, Error> res;
                int attempt = 0;

                do
                {
                    attempt++;
                    res = await RestTemplate.TemplateRequestHandler(
                        ctx,
                        () => getter(page)
                    );

                    if (!res.IsOk) {
                        Log.Out($"Error at page {page}, trying again. Attempt {attempt} - out of 5");
                    }
                } while (!res.IsOk && attempt < 5);

                if(!res.IsOk && attempt >= 5) {
                    errCount++;
                    Log.Out($"Maximum error count reached at page: {page}, error: {res.Error.ExceptionMessage}");
                    return;
                }

                if (res.IsOk) {
                    var columnMap = table.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToArray();

                    var rawDataAttempt = DynamicObjConvert.JsonToDataTable(res.Value[innerProp]!);
                    if (!rawDataAttempt.IsOk) {
                        errCount++;
                        Log.Out($"Error occured at page : {page}, message: {rawDataAttempt.Error.ExceptionMessage}");
                        return;
                    }

                    using DataTable clone = table.Clone();
                    clone.Merge(rawDataAttempt.Value, false, MissingSchemaAction.Add);

                    await channel.Writer.WriteAsync(clone.DefaultView.ToTable(false, columnMap), token);
                }

                passCountFetch++;

                if (passCountFetch >= 100) {
                    Log.Out(
                        $"Received API Responses for pages {page - passCountFetch} - {page}." + 
                        " Deserialized JSONs were added to the channel for consumption."
                    );
                    passCountFetch = 0;
                }
                await Task.Delay(threadTimeout, token);
            });

            channel.Writer.Complete();
        });

        var insert = Task.Run(async () => {
            Result<int, Error> bulkInsert;
            int attempt = 0;

            while (await channel.Reader.WaitToReadAsync())
            {
                using var groupedTable = new DataTable();

                for (int i = 0; i < 20 && channel.Reader.TryRead(out var dataTable); i++)
                {
                    groupedTable.Merge(dataTable);
                    dataTable.Dispose();
                }

                do
                {
                    attempt++;
                    bulkInsert = await serverCall.BulkInsert(groupedTable, tableName, sysName);
                    if (!bulkInsert.IsOk) {
                        Log.Out($"Error while attempting to transfer data: {bulkInsert.Error.ExceptionMessage}. Attempt {attempt} - out of 5");
                    }
                } while (!bulkInsert.IsOk && attempt < 5);

                if (!bulkInsert.IsOk) {
                    errCount++;
                    Log.Out($"Table insert attempt has reached maximum attempt count. Table: {sysName}.{tableName}");
                    return;
                }                
            }
        });

        await Task.WhenAll(fetch, insert);

        if(errCount > 0) {
            return new Error {
                ExceptionMessage = "The data extraction attempt was partially successful.",
                IsPartialSuccess = true,
                ErrorCount = errCount
            };
        }

        return Constants.MethodSuccess;
    }
}