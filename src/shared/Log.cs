using DataConnect.Etl.Sql;
using Microsoft.Data.SqlClient;

namespace DataConnect.Shared;

public static class Log
{
    public static void Out(string message,
                    [System.Runtime.CompilerServices.CallerMemberName] string? callerMethod = null)
    {
        string log = LogPrefix() + $"[{callerMethod}] > " + message;
        Console.WriteLine(log);
    }

    private static string LogPrefix() => $"[{DateTime.Now:dd/MM/yyyy HH:mm:ss:fff}]::";
}