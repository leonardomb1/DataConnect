using DataConnect.Features.HealthCheck.Services;

namespace DataConnect.Features.Api;

public static class HealthCheckEndpoints
{
    public static void MapHealthCheckEndpoints(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapGet("/health", async (IHealthCheckService healthCheckService) =>
        {
            var health = await healthCheckService.GetHealthAsync();
            return Results.Ok(health);
        })
        .WithName("HealthCheck")
        .WithOpenApi(op => new(op)
        {
            Summary = "Get application health status",
            Description = "Returns the current health status of the application"
        });

        endpoints.MapGet("/api", () => new
        {
            Routes = new[]
            {
                "POST /api/extract/paginated",
                "POST /api/extract/simple",
                "POST /api/extract/basic",
                "GET  /api",
                "GET  /health"
            },
            Version = "1.0.2",
            Message = "DataConnect API - Data Extraction Service"
        })
        .WithName("ApiInfo")
        .WithOpenApi(op => new(op)
        {
            Summary = "Get API information",
            Description = "Lists all available API endpoints"
        });
    }
}