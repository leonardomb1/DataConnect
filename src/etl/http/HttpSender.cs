using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using DataConnect.Shared;
using DataConnect.Types;

namespace DataConnect.Etl.Http;

public class HttpSender : IDisposable
{
    private bool _disposed;
    private readonly IHttpClientFactory _clientFactory;
    private readonly HttpClient _httpClient;

    public HttpSender(IHttpClientFactory httpClientFactory)
    {       
        _clientFactory = httpClientFactory 
            ?? throw new ArgumentNullException(nameof(httpClientFactory), "HTTP Factory has not been passed to Sender.");

        _httpClient = _clientFactory.CreateClient();
    }

    private static HttpRequestMessage HttpSimpleAuth(KeyValuePair<string, string> user,
                                             KeyValuePair<string, string> password,
                                             string uri)
    {
        var request = new HttpRequestMessage(HttpMethod.Get, uri);
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

                req.Headers.ConnectionClose = true;

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
                Log.Out(
                    $"Request did not succeed, trying again. Observed exception was : {ex.Message}" +
                    $"\n     - Source at: {ex.Source}"
                );
                await Task.Delay(delay);
                delay += TimeSpan.FromMilliseconds(100);
            }
        }
        return Constants.MethodFail;
    }

    public async Task<dynamic> SimpleAuthBodyRequestAsync(List<KeyValuePair<string, string>> payload, HttpMethod method, string uri)
    {
        if (payload == null || payload.Count < 2)
            throw new ArgumentException("Insufficient parameters provided");

        var user =  payload[0];
        var password = payload[1];
        var content = payload.Skip(2).ToList();
        
        var requestContent = AddRequestContent(HttpSimpleAuth(user, password, uri), content);
        var req = await GetRequestAsync(requestContent, method); 

        if (req.IsOk) {
            return req.Value;
        } else throw new Exception("Maximum ammount of retries reached");  
    }

    public async Task<dynamic> NoAuthRequestAsync(List<KeyValuePair<string, string>> payload, HttpMethod method, string uri)
    {
        var content = payload;

        var requestContent = AddRequestContent(new HttpRequestMessage(method, uri), content);
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
           _httpClient?.Dispose();
        }

        _disposed = true;
    }

    ~HttpSender()
    {
        Dispose(false);
    }
}