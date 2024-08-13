using DataConnect.Shared;
using DataConnect.Etl.Sql;
using DataConnect.Controller;
using DataConnect.Etl.Converter;
using WatsonWebserver.Core;
using System.Data;
using DataConnect.Types;
using DataConnect.Models;
using System.Text.Json;
using System.Text.Json.Nodes;
using DataConnect.Etl.Templates;

namespace DataConnect.Routes;

public static class StouApi
{
    /// <summary>
    /// Método que encapsula a rota "ponto_espelho" da API da STOU. É uma API paginada, portanto, é
    /// necessário realizar requisições em diversas threads para ter ganho de processamento.
    /// </summary>
    /// <param name="ctx">Contexto da conexão com cliente.</param>
    /// <param name="conStr">Cadeia de conexão com servidor SQL.</param>
    /// <param name="database">Banco de dados de extração.</param>
    /// <param name="threadPagination">Limite de requisições simultâneas de páginas da API.</param>
    /// <returns></returns>
    public static async Task<int> StouEspelho(HttpContextBase ctx,
                                              string conStr,
                                              string database,
                                              int threadPagination) 
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

        Result<dynamic, int> firstReturn = await RestTemplate.TemplatePostMethod(ctx, "SimpleAuthBodyRequestAsync", [
            BuildPayload(obj.Options, obj.DestinationTableName, filteredDate, 1)
        ]);
        if (!firstReturn.IsOk) return ReturnedValues.MethodFail;

        using DataTable table = DynamicObjConvert.FromInnerJsonToDataTable(firstReturn.Value, "itens");
        table.Rows.Clear();

        JsonObject firstJson = JsonSerializer.Deserialize<JsonObject>(firstReturn.Value);
        int pageCount = firstJson["totalCount"]?.GetValue<int>() ?? 0;

        Log.Out(
            $"Starting extraction job {ctx.Request.Guid} for {obj.DestinationTableName}\n" +
            $"  - Looking back since: {filteredDate}\n" +
            $"  - Page count: {pageCount}\n" +
            $"  - Estimated size: {pageCount * lookBackTime} lines"
        );

        // Extração em multi-thread da API.
        // Realiza-se chamada em threads para cada página até o limite em variável de ambiente.
        using SqlServerCall serverCall = new(conStr);
        await ExtractTemplate.PaginatedApiToSqlDatabase(
            ctx,
            serverCall,
            obj,
            table,
            page => BuildPayload(obj.Options, obj.DestinationTableName, filteredDate, page),
            threadPagination,
            pageCount,
            "SimpleAuthBodyRequestAsync", // Requisição de autenticação simples
            database
        );

        Log.Out(
            $"Extraction job {ctx.Request.Guid} has been completed."
        );

        return ReturnedValues.MethodSuccess; 
    }

    /// <summary>
    /// Método para construção de requisição paginada das APIs da STOU.
    /// Token de autenticação segue lógica de hash: string dada por STOU + Data Atual, em SHA256.
    /// </summary>
    /// <param name="options">Opções de envio dinâmicas, utilizado para autenticação</param>
    /// <param name="destinationTableName">Normalmente é o nome do WebService.</param>
    /// <param name="filteredDate">Data de Corte</param>
    /// <param name="page">Paginação da API</param>
    /// <returns>Lista chaves e valores para construção de JSON</returns>
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