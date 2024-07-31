namespace DataConnect.Controller;

public class Response
{
    public required int Status {get; set;}
    public required bool Error {get; set;}
    public required string Message {get; set;}
    public List<string>? Options {get; set;}
}