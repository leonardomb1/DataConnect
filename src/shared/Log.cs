using DataConnect.Etl.Sql;
using Microsoft.Data.SqlClient;

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

    public static async Task ToServer(string message,
                                int executionId,
                                int extractionId,
                                int operationId,
                                SqlServerCall homeCall,
                    [System.Runtime.CompilerServices.CallerMemberName] string? callerMethod = null)
    {
        string _logPrefix = $"[{DateTime.Now}]::";
        string log = _logPrefix + $"[{callerMethod}]::[LOGGED] > " + message;

        await homeCall.ExecuteCommand(
            new SqlCommand(
                @$"INSERT INTO DWController..DW_LOG
                VALUES({executionId}, {operationId}, '{message}', {Constants.MethodSuccess}, {extractionId}, DEFAULT)"
            )
        );
        Console.WriteLine(log);
    }
}