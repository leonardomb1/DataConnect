using DataConnect.Etl.Sql;
using DataConnect.Shared;
using DataConnect.Etl.Converter;
using DataConnect.Types;
using DataConnect.Controller;
using System.Data;
using WatsonWebserver.Core;
using DataConnect.Models;
using System.Threading.Channels;
using DataConnect.Etl.Http;

namespace DataConnect.Etl.Templates;

public static class PaginatedExtractTemplate
{
    public static async Task PaginatedApiToSqlDatabase(HttpContextBase ctx,
                                                       HttpSender sender,
                                                       string conStr,
                                                       BodyDefault obj,
                                                       DataTable table,
                                                       Func<int, List<KeyValuePair<string, string>>> listBuilder,
                                                       int threadPagination,
                                                       int pageCount,
                                                       string apiAuthMethod,
                                                       string innerProp,
                                                       int threadTimeout,
                                                       string uri,
                                                       string? database = null)
    {
        using var serverCall = new SqlServerCall(conStr);
        var options = new ParallelOptions { MaxDegreeOfParallelism = threadPagination };
        var channel = Channel.CreateBounded<DataTable>(capacity: threadPagination * 2);

        var fetch = Task.Run(async () => {
            await Parallel.ForEachAsync(Enumerable.Range(1, pageCount), options, async (page, token) => {
                Result<dynamic, int> res;
                int attempt = 0;
                do
                {
                    res = await RestTemplate.TemplateRequestHandler(
                        ctx, sender, apiAuthMethod, 
                        [listBuilder(page), System.Net.Http.HttpMethod.Post, uri]
                    );

                    if (!res.IsOk) Log.Out($"Error at page {page}, trying again. Attempt {attempt} - out of 5");
                    attempt++;
                } while (!res.IsOk || attempt < 5);

                if (res.IsOk)
                {
                    var columnMap = table.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToArray();
                    using DataTable rawData = DynamicObjConvert.FromInnerJsonToDataTable(res.Value, innerProp);
                    using DataTable clone = table.Clone();
                    clone.Merge(rawData, false, MissingSchemaAction.Add);
                    await channel.Writer.WriteAsync(clone.DefaultView.ToTable(false, columnMap), token);
                }
                else
                {
                    Log.Out($"Extraction failed at page {page}, observed error was: {res.Error}");
                }

                await Task.Delay(threadTimeout, token);
            });

            channel.Writer.Complete();
        });

        var insert = Task.Run(async () => {
            await foreach (var dataTable in channel.Reader.ReadAllAsync())
            {
                await serverCall.BulkInsert(dataTable, obj.DestinationTableName, obj.SysName, database);
                dataTable.Dispose();
            }
        });

        await Task.WhenAll(fetch, insert);
    }
}