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

    public static async Task ToServer(string message,
                                int executionId,
                                int extractionId,
                                int operationId,
                                SqlServerCall homeCall,
                    [System.Runtime.CompilerServices.CallerMemberName] string? callerMethod = null)
    {
        string log = LogPrefix() + $"[{callerMethod}]::[LOGGED] > " + message;

        var res = await homeCall.ExecuteCommand(
            new SqlCommand(
                @$"INSERT INTO DWController..DW_LOG
                VALUES({executionId}, {operationId}, '{message}', {Constants.MethodSuccess}, {extractionId}, DEFAULT)"
            )
        );
        if(!res.IsOk) {
            string error = LogPrefix() + $"[{callerMethod}] > Error ocurred while attempting to send log data to server: {res.Error.ExceptionMessage}";
            Console.WriteLine(error);
            return;
        }

        Console.WriteLine(log);
    }

    private static string LogPrefix() => $"[{DateTime.Now:dd/MM/yyyy HH:mm:ss:fff}]::";
}