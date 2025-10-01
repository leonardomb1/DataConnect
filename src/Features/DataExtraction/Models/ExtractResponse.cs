namespace DataConnect.Features.DataExtraction.Models;

public class ExtractResponse
{
    public bool Success { get; set; }
    public string Message { get; set; } = string.Empty;
    public int? RecordsProcessed { get; set; }
    public int? PagesProcessed { get; set; }
    public int? ErrorCount { get; set; }
    public DateTime StartTime { get; set; }
    public DateTime? EndTime { get; set; }
    public TimeSpan? Duration => EndTime?.Subtract(StartTime);
    public string? JobId { get; set; }
    public List<string>? Errors { get; set; }
}