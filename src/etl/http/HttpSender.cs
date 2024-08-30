using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using DataConnect.Shared;
using DataConnect.Types;

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
                                                 List<KeyValuePair<string, string>> content)
    {
        ArgumentNullException.ThrowIfNull(content);     

        var body = JsonSerializer.Serialize(content.ToDictionary(x => x.Key, x => x.Value));
        request.Content = new StringContent(body, Encoding.UTF8, "application/json");

        return request; 
    }

    private async Task<Result<dynamic, int>> GetRequestAsync(HttpRequestMessage requestTemplate, HttpMethod method)
    {
        var client = _httpClient;
        int maxRetries = 5;
        var delay = TimeSpan.FromMilliseconds(1000);

        for (int attempt = 0; attempt < maxRetries; attempt++) {
            try
            {
                using HttpRequestMessage req = new(requestTemplate.Method, requestTemplate.RequestUri) {
                    Content = requestTemplate.Content,
                    Method = method
                };

                foreach (var header in requestTemplate.Headers) {
                    req.Headers.TryAddWithoutValidation(header.Key, header.Value);
                }
                
                using var response = await client.SendAsync(req);
                response.EnsureSuccessStatusCode();
                var bodyObj = await response.Content.ReadFromJsonAsync<dynamic>();

                return bodyObj!;             
            }
            catch (Exception ex) when (attempt < maxRetries - 1)
            {
                Log.Out($"Request did not succeed, trying again. Observed exception was : {ex.Message}");
                await Task.Delay(delay);
                delay += TimeSpan.FromMilliseconds(100);
            }
        }
        return Constants.MethodFail;
    }

    public async Task<dynamic> SimpleAuthBodyRequestAsync(List<KeyValuePair<string, string>> payload, HttpMethod method)
    {
        if (payload == null || payload.Count < 2)
            throw new ArgumentException("Insufficient parameters provided");

        var user =  payload[0];
        var password = payload[1];
        var content = payload.Skip(2).ToList();
        
        var requestContent = AddRequestContent(HttpSimpleAuth(user, password), content);
        var req = await GetRequestAsync(requestContent, method); 

        if (req.IsOk) {
            return req.Value;
        } else throw new Exception("Maximum ammount of retries reached");  
    }

    public async Task<dynamic> NoAuthRequestAsync(List<KeyValuePair<string, string>> payload, HttpMethod method)
    {
        var content = payload;

        var requestContent = AddRequestContent(new HttpRequestMessage(method, _requestUri), content);
        var req = await GetRequestAsync(requestContent, method);
        if (req.IsOk) {
            return req.Value;
        } else throw new Exception("Maximum ammount of retries reached");
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