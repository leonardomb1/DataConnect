namespace DataConnect.Shared;

public static class Log
{
    public static void Out(string message,
                    [System.Runtime.CompilerServices.CallerMemberName] string? callerMethod = null)
    {
        string _logPrefix = $"[{DateTime.Now}]::";
        string log = _logPrefix + $"[{callerMethod}] > " + message;
        Console.WriteLine(log);
    }
}