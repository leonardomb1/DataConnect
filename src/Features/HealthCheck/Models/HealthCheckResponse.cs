namespace DataConnect.Features.HealthCheck.Models;

public class HealthCheckResponse
{
    public string Status { get; set; } = "Healthy";
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
    public string Version { get; set; } = "1.0.2";
    public Dictionary<string, object>? Details { get; set; }
}