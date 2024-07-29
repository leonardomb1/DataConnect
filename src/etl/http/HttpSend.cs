using System.Net.Http.Json;
using System.Text;
using System.Text.Json;

namespace DataConnect.Etl.Http;

public class HttpSender(string requestUri,
                        HttpClient httpClient) : IDisposable
{
    private bool _disposed;
    private readonly string _requestUri = requestUri
        ?? throw new ArgumentNullException(requestUri, nameof(requestUri));
    private readonly HttpClient _httpClient = httpClient
        ?? throw new ArgumentNullException(requestUri, nameof(requestUri));

    private HttpRequestMessage HttpSimpleAuth(KeyValuePair<string, string> user,
                                             KeyValuePair<string, string> password)
    {
        var request = new HttpRequestMessage(HttpMethod.Get, _requestUri);
        request.Headers.Add(user.Key, user.Value);
        request.Headers.Add(password.Key, password.Value);

        return request;
    }

    private static HttpRequestMessage AddRequestContent(HttpRequestMessage request,
                                                 KeyValuePair<string, string>[] content)
    {
        ArgumentNullException.ThrowIfNull(content);     

        var body = JsonSerializer.Serialize(content.ToDictionary(x => x.Key, x => x.Value));
        request.Content = new StringContent(body, Encoding.UTF8, "application/json");

        return request; 
    }

    private async Task<dynamic> GetRequestAsync(HttpRequestMessage request)
    {
        var client = _httpClient;

        using var response = await client.SendAsync(request);
        var bodyObj = await response.Content.ReadFromJsonAsync<dynamic>();

        return bodyObj!;
    }

    public async Task<dynamic> SimpleAuthBodyRequestAsync(KeyValuePair<string, string> user,
                                         KeyValuePair<string, string> password,
                                         KeyValuePair<string, string>[] content)
    {
        var request = AddRequestContent(HttpSimpleAuth(user, password), content);
        return await GetRequestAsync(request);
    }

    public async Task<dynamic> NoAuthRequestAsync() 
    {
        return await GetRequestAsync(new HttpRequestMessage(HttpMethod.Get, _requestUri));
    }
    
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    private void Dispose(bool disposing)
    {
        if (!_disposed) {
            return;
        }

        if (disposing) {
           // Objetos para limpeza de memória.
        }

        _disposed = true;
    }

    ~HttpSender()
    {
        Dispose(false);
    }
}