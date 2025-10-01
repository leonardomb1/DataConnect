using DataConnect.Features.HealthCheck.Models;
using Microsoft.Extensions.Configuration;

namespace DataConnect.Features.HealthCheck.Services;

public class HealthCheckService : IHealthCheckService
{
    private readonly IConfiguration _configuration;

    public HealthCheckService(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    public async Task<HealthCheckResponse> GetHealthAsync()
    {
        var response = new HealthCheckResponse();

        try
        {
            // You could add database connectivity checks, etc. here
            var connectionString = _configuration.GetConnectionString("DefaultConnection");

            response.Details = new Dictionary<string, object>
            {
                ["database_configured"] = !string.IsNullOrEmpty(connectionString),
                ["auth_configured"] = !string.IsNullOrEmpty(_configuration["Auth:Secret"]),
                ["uptime"] = Environment.TickCount64
            };

            return await Task.FromResult(response);
        }
        catch (Exception ex)
        {
            response.Status = "Unhealthy";
            response.Details = new Dictionary<string, object>
            {
                ["error"] = ex.Message
            };
            return response;
        }
    }
}