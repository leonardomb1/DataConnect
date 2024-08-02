namespace DataConnect.Models;

public class BodyDefault
{
    public required string ConnectionInfo {get; set;}
    public required string DestinationTableName {get; set;}
    public required string SysName {get; set;}
    public required string[] Options {get; set;}
}