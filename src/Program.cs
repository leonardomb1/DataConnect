using Microsoft.Extensions.DependencyInjection;
using DataConnect.Etl.Http;
using DataConnect.Etl;

public class Program
{
    public static void Main(string[] args)
    {
        var serviceProvider = new ServiceCollection()
            .AddHttpClient()
            .BuildServiceProvider();

        var httpClientFactory = serviceProvider.GetRequiredService<IHttpClientFactory>();

        using var sender = new HttpSender("https://jsonplaceholder.typicode.com/todos/1", httpClientFactory);

        var user = new KeyValuePair<string, string>("Username", "myUsername");
        var password = new KeyValuePair<string, string>("Password", "myPassword");

        var content = new[]
        {
            new KeyValuePair<string, string>("key1", "value1"),
            new KeyValuePair<string, string>("key2", "value2")
        };

        try
        {
            HttpExtract.PaginatedApiExtract(() => sender.NoAuthRequestAsync(), "page");

            Console.WriteLine("Response:");
            Console.WriteLine(response.Result);
        }
        catch (Exception ex)
        {
            Console.WriteLine("Error:");
            Console.WriteLine(ex);
        }
    }
}