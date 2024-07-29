using System.Data;
using System.Text.Json.Nodes;
using DataConnect.Shared.Converter;

namespace DataConnect.Etl.Extract;

public static class ApiCall
{
    public static async Task PaginatedApiExtract(Func<dynamic> extractMethod,
                                                 Func<JsonArray, int> publishMethod,
                                                 string pageAtrName)
    {
        dynamic firstResult = await extractMethod();

        int pageCount = firstResult;

        JsonArray result = [];

        List<Task> tasks = [];

        for(int i = 0; i < pageCount; i++)
        {
            for(int j = 0; j < Environment.ProcessorCount; j++)
            {
                tasks.Add(Task.Run(async () => {
                    result.Add(await extractMethod());
                }));
            }
            await Task.WhenAll(tasks);
            tasks.Clear();
            
            await Task.Run(() => publishMethod(result));

            result.Clear();

            await Task.Delay(500);
        }
    }
}