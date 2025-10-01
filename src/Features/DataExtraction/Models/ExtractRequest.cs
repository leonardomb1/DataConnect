namespace DataConnect.Features.DataExtraction.Models;

public class ExtractRequest
{
    public required string[] Options { get; set; }
    public required string DestinationTableName { get; set; }
    public required string SysName { get; set; }
    public required ConnectionInfo ConnectionInfo { get; set; }
}

public class ConnectionInfo
{
    public required string Url { get; set; }
    public string? Username { get; set; }
    public string? Password { get; set; }
    public Dictionary<string, string>? Headers { get; set; }
}

// Legacy model mapping for backwards compatibility
public class BodyDefault
{
    public required string ConnectionInfo { get; set; }
    public required string DestinationTableName { get; set; }
    public required string SysName { get; set; }
    public required string[] Options { get; set; }
}