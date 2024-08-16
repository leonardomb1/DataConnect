using DataConnect.Etl.Sql;
using DataConnect.Shared;
using DataConnect.Etl.Converter;
using DataConnect.Types;
using DataConnect.Controller;
using System.Data;
using WatsonWebserver.Core;
using DataConnect.Models;
using System.Collections.Concurrent;

namespace DataConnect.Etl.Templates;

public static class ExtractTemplate
{
    /// <summary>
    /// Método estático para reutilização em APIs páginadas. A competência deste método é chamar a API, com a
    /// autenticação específicada, e enviar ao banco de dados.
    /// 
    /// Passando um número total de páginas, o algoritmo abaixo realizará iteração em pacotes de tarefas, estas
    /// estando em threads distintas, até chegar a última página. 
    /// </summary>
    /// <param name="ctx">Contexto de conexão com o Cliente.</param>
    /// <param name="serverCall">Objeto que coordena a conexão com o servidor SQL.</param>
    /// <param name="obj">Objeto que armazena a requisição do Cliente.</param>
    /// <param name="table">Tabela a qual deverá ser usada como base para requisição.
    /// As requisições usaram esta tabela para deserializar o JSON.</param>
    /// <param name="listBuilder">Função que deverá ser passada para construir a chamada POST à API. necessário ter callback
    /// passando "page" como parametro. Ex: page => func(page).</param>
    /// <param name="threadPagination">Limite de requisições simultâneas de páginas da API.</param>
    /// <param name="pageCount">Número total de páginas da API.</param>
    /// <param name="apiAuthMethod">Nome do método que realizará a chamada à API.</param>
    /// <param name="database">Nome do banco de dados para extração.</param>
    public static async Task PaginatedApiToSqlDatabase(HttpContextBase ctx,
                                                                            string conStr,
                                                                            BodyDefault obj,
                                                                            DataTable table,
                                                                            Func<int, List<KeyValuePair<string, string>>> listBuilder,
                                                                            int threadPagination,
                                                                            int pageCount,
                                                                            string apiAuthMethod,
                                                                            string innerProp,
                                                                            string? database = null)
    {
        var tasks = new ConcurrentBag<Task>();
        
        using var serverCall = new SqlServerCall(conStr);
        using var semaphore = new SemaphoreSlim(threadPagination);
        using var queue = new BlockingCollection<DataTable>();

        using var fetcherTasks = Task.Run(async () => {
            var fetch = Enumerable.Range(1, pageCount)
                .Select(page => Task.Run(async () => {
                    await semaphore.WaitAsync();
                    Result<dynamic, int> res;
                    int attempt = 0;
                    try {
                        do
                        {
                            res = await RestTemplate.TemplateRequestHandler(ctx, apiAuthMethod, 
                                [listBuilder(page), System.Net.Http.HttpMethod.Post] 
                            );
                            if (!res.IsOk) Log.Out($"Error at page {page}, trying again. Attempt {attempt} - out of 5");
                            attempt++;
                        } while(!res.IsOk || attempt < 5);

                        if (res.IsOk) {
                            using var cloneTable = table.Clone();
                            using var data = DynamicObjConvert.FromInnerJsonToDataTable(res.Value, innerProp); 
                            cloneTable.Merge(data, false, MissingSchemaAction.Ignore);
                            queue.Add(cloneTable);
                        }
                        return res;
                    } catch (Exception ex) {
                        Log.Out($"Extraction failed at page {page}, error observed was: {ex.Message}");
                        return ReturnedValues.MethodFail;
                    } finally {
                        semaphore.Release();
                    }
                }).ContinueWith(thread => thread.Dispose())
            );

            await Task.WhenAll(fetch);
            queue.CompleteAdding();
        });

        using var insertTasks = Task.Run(async () =>
        {
            foreach (var dataTable in queue.GetConsumingEnumerable())
            {
                await serverCall.BulkInsert(dataTable, obj.DestinationTableName, obj.SysName, database);
                dataTable.Dispose();
            }
        });

        await Task.WhenAll(fetcherTasks, insertTasks);
        tasks.Clear();
    }
}