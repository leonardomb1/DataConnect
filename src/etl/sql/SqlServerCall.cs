using System.Data;
using DataConnect.Models;
using DataConnect.Shared;
using DataConnect.Types;
using Microsoft.Data.SqlClient;

namespace DataConnect.Etl.Sql;

public class SqlServerCall : IDisposable
{
    private bool _disposed;
    private readonly SqlConnection _connection;

    public SqlServerCall(string conStr)
    {
        _connection = new SqlConnection(conStr);
        _connection.OpenAsync().GetAwaiter().GetResult();
    }

    public async Task<Result<int, Error>> BulkInsert(DataTable table,
                                    string tableName,
                                    string sysName,
                                    string? database = null)
    {
        await ChangeDatabase(database);

        using SqlBulkCopy bulkCopy = new(_connection)
        {
            BulkCopyTimeout = 1000,
            DestinationTableName = $"{sysName.ToUpper()}.{tableName.ToUpper()}"
        };

        try
        {
            await bulkCopy.WriteToServerAsync(table);
            return Constants.MethodSuccess;
        }
        catch (Exception ex)
        {
            return new Error { ExceptionMessage = ex.Message };
        }
    }

    public async Task<Result<int, Error>> CreateTable(DataTable table, string tableName, string sysName, string? database = null)
    {
        if (!await IsCreated(tableName, database))
        {
            await ChangeDatabase(database);

            Log.Out($"Failed to find table {sysName.ToUpper()}.{tableName.ToUpper()}, proceeding with creation...");
            using SqlCommand cmd = new("", _connection);

            string createTableQuery = $"CREATE TABLE {sysName.ToUpper()}.{tableName.ToUpper()} (";

            foreach (DataColumn column in table.Columns)
            {
                createTableQuery += $"[{column.ColumnName}] {MapDataType(column.DataType)}, ";
            }
            createTableQuery += $"ID_DW_{tableName.ToUpper()} INT NOT NULL IDENTITY(1,1) CONSTRAINT IX_{tableName.ToUpper()}_SK PRIMARY KEY CLUSTERED, ";
            createTableQuery += $"DT_UPDATE_{tableName.ToUpper()} DATETIME NOT NULL CONSTRAINT CK_UPDATE_{tableName.ToUpper()} DEFAULT(GETDATE()));";
            cmd.CommandText = createTableQuery;

            try
            {
                await cmd.ExecuteNonQueryAsync();
                return Constants.MethodSuccess;
            }
            catch (Exception ex)
            {
                return new Error() { ExceptionMessage = ex.Message };
            }
        }
        else
        {
            Log.Out($"Table {tableName.ToUpper()} already exists.");
            return Constants.MethodSuccess;
        }
    }

    private static string MapDataType(Type type)
    {
        return type switch
        {
            _ when type == typeof(string) => "NVARCHAR(MAX)",
            _ when type == typeof(int) => "INT",
            _ when type == typeof(long) => "BIGINT",
            _ when type == typeof(float) => "FLOAT",
            _ when type == typeof(double) => "FLOAT",
            _ when type == typeof(decimal) => "DECIMAL(18, 2)",
            _ when type == typeof(bool) => "BIT",
            _ when type == typeof(DateTime) => "DATETIME",
            _ => "NVARCHAR(MAX)"
        };
    }

    public async Task<Result<DataTable, Error>> GetTableDataFromServer(string query, string? database = null)
    {
        await ChangeDatabase(database);

        DataTable data = new();

        using SqlCommand cmd = new()
        {
            Connection = _connection,
            CommandText = query
        };

        try
        {
            var reader = await cmd.ExecuteReaderAsync();
            data.Load(reader);
            return data;
        }
        catch (Exception ex)
        {
            return new Error() { ExceptionMessage = ex.Message };
        }
    }

    public async Task<Result<int, Error>> ExecuteCommand(SqlCommand cmd, string? database = null)
    {
        await ChangeDatabase(database);

        cmd.Connection = _connection;

        try
        {
            await cmd.ExecuteNonQueryAsync();
            return Constants.MethodSuccess;
        }
        catch (Exception ex)
        {
            return new Error() { ExceptionMessage = ex.Message };
        }
    }

    public async Task<Result<int, Error>> ReadPacketFromServer(string query,
                                                                 int packetSize,
                                                                 string tableName,
                                                                 string sysName,
                                                                 SqlServerCall homeServer,
                                                                 int executionId,
                                                                 int extractionId,
                                                                 string? database = null)
    {
        using var cmd = new SqlCommand()
        {
            CommandText = query,
            Connection = _connection,
            CommandTimeout = 1200
        };

        using var reader = await cmd.ExecuteReaderAsync();
        using var table = new DataTable();
        int logLineCount = 0;

        try
        {
            for (int i = 0; i < reader.FieldCount; i++)
            {
                table.Columns.Add(reader.GetName(i), reader.GetFieldType(i));
            }

            while (await reader.ReadAsync())
            {
                logLineCount++;
                DataRow row = table.NewRow();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    row[i] = reader.GetValue(i);
                }
                table.Rows.Add(row);

                if (logLineCount == 10000)
                {
                    Log.Out(
                        $"Reading packet data from reader array:\n" +
                        $"  - Current table {sysName}.{tableName} line count: {table.Rows.Count} lines."
                    );

                    logLineCount = 0;
                }

                if (table.Rows.Count >= packetSize)
                {
                    Log.Out(
                        $"Inserting packet data from reader array:\n" +
                        $"  - Current table {sysName}.{tableName} line count: {table.Rows.Count} lines."
                    );

                    Result<int, Error> innerInsert;
                    int attempt = 0;

                    do
                    {
                        attempt++;
                        innerInsert = await homeServer.BulkInsert(table, tableName, sysName, database);
                        if (!innerInsert.IsOk)
                        {
                            Log.Out(
                                $"Error while attempting to transfer data from packet: {innerInsert.Error.ExceptionMessage}.\n" +
                                $"- Attempt {attempt} - out of 5\n    - Table: {sysName}.{tableName}"
                            );
                        }
                    } while (!innerInsert.IsOk && attempt < 5);

                    if (!innerInsert.IsOk)
                    {
                        Log.Out(
                            $"Table insert attempt has reached maximum attempt count. Table: {sysName}.{tableName}"
                        );
                        return new Error() { ExceptionMessage = $"Error attempting to insert data to server: {innerInsert.Error.ExceptionMessage}" };
                    }

                    logLineCount = 0;
                    table.Clear();
                }
            }

            if (table.Rows.Count > 0)
            {
                Result<int, Error> outerInsert;
                int attempt = 0;

                do
                {
                    attempt++;
                    outerInsert = await homeServer.BulkInsert(table, tableName, sysName, database);
                    if (!outerInsert.IsOk)
                    {
                        Log.Out(
                            $"Error while attempting to transfer data from packet: {outerInsert.Error.ExceptionMessage}.\n" +
                            $"- Attempt {attempt} - out of 5\n    - Table: {sysName}.{tableName}"
                        );
                    }
                } while (!outerInsert.IsOk && attempt < 5);

                if (!outerInsert.IsOk)
                {
                    Log.Out(
                        $"Table insert attempt has reached maximum attempt count. Table: {sysName}.{tableName}"
                    );
                    return new Error() { ExceptionMessage = $"Error attempting to insert data to server: {outerInsert.Error.ExceptionMessage}" };
                }
            }
            return Constants.MethodSuccess;
        }
        catch (Exception ex)
        {
            Log.Out(
                $"Error while attempting insert: {ex.Message}"
            );
            return new Error() { ExceptionMessage = ex.Message };
        }
        finally
        {
            if (reader != null && !reader.IsClosed)
            {
                reader.Close();
            }
        }
    }

    public async Task<Result<int, Error>> GetScalarDataFromServer(string query, string? database = null)
    {
        await ChangeDatabase(database);

        using SqlCommand cmd = new()
        {
            CommandText = query,
            Connection = _connection
        };

        try
        {
            var ret = await cmd.ExecuteScalarAsync() ?? "0";
            return int.Parse(ret.ToString()!);
        }
        catch (Exception ex)
        {
            Log.Out($"Error while fetching data from server: {ex.Message}, command text: {cmd.CommandText}");
            return new Error() { ExceptionMessage = ex.Message };
        }
    }

    private async Task ChangeDatabase(string? database = null)
    {
        if (database != null)
        {
            await _connection.ChangeDatabaseAsync(database);
        }
    }

    private async Task<bool> IsCreated(string tableName, string? database = null)
    {
        await ChangeDatabase(database);

        using SqlCommand cmd = new()
        {
            CommandText = $"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{tableName.ToUpper()}'",
            Connection = _connection
        };

        var ret = cmd.ExecuteScalar() ?? "0";
        return int.Parse(ret.ToString()!) == Constants.MethodSuccess;
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    private void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            return;
        }

        if (disposing)
        {
            _connection.Close();
            _connection.Dispose();
        }

        _disposed = true;
    }

    ~SqlServerCall()
    {
        Dispose(false);
    }
}