using System.Text.Json.Nodes;

namespace DataConnect.Etl;

public class HttpExtract
{
    public static async Task PaginatedApiExtract(Func<dynamic> extractMethod,
                                                          Func<JsonArray, int> publishMethod,
                                                          string pageAtrName)
    {
        dynamic firstResult = Task.Run(async () => await extractMethod());

        int pageCount = firstResult;

        JsonArray result = [];

        List<Task> tasks = [];

        for(int i = 0; i < pageCount; i++)
        {
            for(int j = 0; j < Environment.ProcessorCount; j++)
            {
                tasks.Add(Task.Run(async () => {
                    result += await extractMethod();
                }));
            }
            await Task.WhenAll(tasks);
            tasks.Clear();
            
            await Task.Run(() => publishMethod(result));

            result.Clear();

            Console.WriteLine(i + 1);
            await Task.Delay(500);
        }
    }
}