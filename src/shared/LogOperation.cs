namespace DataConnect.Shared;

public class Log
{
    private readonly string _logPrefix = $"[{DateTime.Now}]:: ";

    public void Out(string message,
                    [System.Runtime.CompilerServices.CallerMemberName] string? callerMethod = null)
    {
        var log = _logPrefix + $"[{callerMethod}] > " + message;
        Console.WriteLine(log);
    }
}