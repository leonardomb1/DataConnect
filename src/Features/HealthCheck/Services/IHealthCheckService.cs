using DataConnect.Features.HealthCheck.Models;

namespace DataConnect.Features.HealthCheck.Services;

public interface IHealthCheckService
{
    Task<HealthCheckResponse> GetHealthAsync();
}