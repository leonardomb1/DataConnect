using DataConnect.Etl.Sql;
using DataConnect.Shared;
using DataConnect.Etl.Converter;
using DataConnect.Types;
using DataConnect.Controller;
using System.Data;
using WatsonWebserver.Core;
using DataConnect.Models;

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
                                                                            SqlServerCall serverCall,
                                                                            BodyDefault obj,
                                                                            DataTable table,
                                                                            Func<int, List<KeyValuePair<string, string>>> listBuilder,
                                                                            int threadPagination,
                                                                            int pageCount,
                                                                            string apiAuthMethod,
                                                                            string? database = null)
    {
        using var semaphore = new SemaphoreSlim(threadPagination);
        var tasks = new List<Task>();

        for (int pageIter = 1; pageIter <= pageCount; pageIter += threadPagination + 1)
        {
            // Cria-se uma lista de tarefas, da página atual até o limite de paralelismo definido.
            var currentTasks = Enumerable.Range(pageIter, Math.Min(threadPagination, pageCount - pageIter + 1))
                .Select(async page => {
                    
                    await semaphore.WaitAsync();
                    Result<dynamic, int> res;
                    int attempt = 0;
                    try {
                        do
                        {
                            res = await RestTemplate.TemplatePostMethod(ctx, apiAuthMethod, 
                                [listBuilder(page)]
                            );
                            if (!res.IsOk) Log.Out($"Error at page {page}, trying again. Attempt {attempt + 1} - out of 5");
                            attempt++;
                        } while(!res.IsOk || attempt < 5);

                        if (res.IsOk) {
                            using var data = DynamicObjConvert.FromInnerJsonToDataTable(res.Value, "itens");
                            // Necessário realizar lock na tabela para segurança de recursos.
                            lock (table) {
                                table.Merge(data, true, MissingSchemaAction.Ignore);
                            }
                        }
                        return res;
                    } catch (Exception ex) {
                        Log.Out($"Extraction failed at page {page}, error observed was: {ex.Message}");
                        return ReturnedValues.MethodFail;
                    } finally {
                        semaphore.Release();
                    }
            });

            tasks.AddRange(currentTasks);
            await Task.WhenAll(tasks);
            tasks.Clear();
                
            await serverCall.CreateTable(table, obj.DestinationTableName, obj.SysName, database);
            await serverCall.BulkInsert(table, obj.DestinationTableName, obj.SysName, database);

            table.Rows.Clear();
            await Task.Delay(500);
        }

        if (tasks.Count > 0)
        {
            await Task.WhenAll(tasks);
            await serverCall.CreateTable(table, obj.DestinationTableName, obj.SysName, database);
            await serverCall.BulkInsert(table, obj.DestinationTableName, obj.SysName, database);
            tasks.ForEach(t => t.Dispose());
            tasks.Clear();
        }
    }
}