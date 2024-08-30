namespace DataConnect.Models;

public class ExtractionMetadata 
{
    public required int ExtractId {get; set;}
    public required string TableName {get; set;}
    public required int ScheduleId {get; set;}
    public required int SystemId {get; set;}
    public required char TableType {get; set;}
    public string? ColumnName {get; set;}
    public int? LookBackValue {get; set;}
    public string? IndexName {get; set;}
}