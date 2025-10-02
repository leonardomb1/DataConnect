using System.Data;
using DataConnect.Core.Models;
using DataConnect.Core.Common;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace DataConnect.Infrastructure.Database;

public class SqlServerCall : IDisposable, IAsyncDisposable
{
    private bool _disposed;
    private readonly SqlConnection _connection;
    private readonly ILogger<SqlServerCall>? _logger;
    private bool _isInitialized;

    private SqlServerCall(string conStr, ILogger<SqlServerCall>? logger = null)
    {
        _logger = logger;
        _connection = new SqlConnection(conStr);
    }

    public static async Task<SqlServerCall> CreateAsync(string conStr, ILogger<SqlServerCall>? logger = null)
    {
        var instance = new SqlServerCall(conStr, logger);
        await instance.InitializeAsync();
        return instance;
    }

    private async Task InitializeAsync()
    {
        if (_isInitialized) return;

        _logger?.LogDebug("Opening SQL connection to {DataSource}", _connection.DataSource);
        await _connection.OpenAsync();
        _logger?.LogDebug("SQL connection opened to {DataSource}/{Database}",
            _connection.DataSource, _connection.Database);
        _isInitialized = true;
    }

    public async Task<Result<int, Error>> BulkInsert(DataTable table,
                                    string tableName,
                                    string sysName,
                                    string? database = null)
    {
        await ChangeDatabase(database);

        var destinationTable = $"{sysName.ToUpper()}.{tableName.ToUpper()}";
        _logger?.LogDebug("BulkInsert to {Table}: {RowCount} rows, {ColumnCount} columns",
            destinationTable, table.Rows.Count, table.Columns.Count);

        using SqlBulkCopy bulkCopy = new(_connection)
        {
            BulkCopyTimeout = 300,  // 5 minutes timeout for large batches
            DestinationTableName = destinationTable,
            BatchSize = 1000,
            EnableStreaming = true
        };

        try
        {
            await bulkCopy.WriteToServerAsync(table);
            _logger?.LogDebug("BulkInsert completed successfully to {Table}", destinationTable);
            return Constants.MethodSuccess;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "BulkInsert failed to {Table}. Rows: {Rows}, Columns: {Columns}",
                destinationTable, table.Rows.Count, table.Columns.Count);
            return new Error { ExceptionMessage = ex.Message };
        }
    }

    public async Task<Result<int, Error>> CreateTable(DataTable table, string tableName, string sysName, string? database = null)
    {
        await ChangeDatabase(database);

        // Ensure schema exists
        var schemaResult = await EnsureSchemaExists(sysName);
        if (!schemaResult.IsOk)
        {
            return schemaResult.Error;
        }

        _logger?.LogDebug("Checking if table {Schema}.{TableName} exists", sysName.ToUpper(), tableName.ToUpper());

        if (!await IsCreated(tableName, database))
        {
            _logger?.LogInformation("Table {Schema}.{TableName} not found, creating with {ColumnCount} columns",
                sysName.ToUpper(), tableName.ToUpper(), table.Columns.Count);

            using SqlCommand cmd = new("", _connection);
            cmd.CommandTimeout = 300; // 5 minutes for large table creation

            string createTableQuery = $"CREATE TABLE {sysName.ToUpper()}.{tableName.ToUpper()} (";

            foreach (DataColumn column in table.Columns)
            {
                createTableQuery += $"[{column.ColumnName}] {MapDataType(column.DataType)}, ";
            }
            createTableQuery += $"ID_DW_{tableName.ToUpper()} INT NOT NULL IDENTITY(1,1) CONSTRAINT IX_{tableName.ToUpper()}_SK PRIMARY KEY CLUSTERED, ";
            createTableQuery += $"DT_UPDATE_{tableName.ToUpper()} DATETIME NOT NULL CONSTRAINT CK_UPDATE_{tableName.ToUpper()} DEFAULT(GETDATE()));";
            cmd.CommandText = createTableQuery;

            _logger?.LogDebug("Executing CREATE TABLE statement ({Length} chars)", createTableQuery.Length);

            try
            {
                await cmd.ExecuteNonQueryAsync();
                _logger?.LogInformation("Table {Schema}.{TableName} created successfully", sysName.ToUpper(), tableName.ToUpper());
                return Constants.MethodSuccess;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to create table {Schema}.{TableName}", sysName.ToUpper(), tableName.ToUpper());
                return new Error() { ExceptionMessage = ex.Message };
            }
        }
        else
        {
            _logger?.LogInformation("Table {Schema}.{TableName} already exists, skipping creation", sysName.ToUpper(), tableName.ToUpper());
            return Constants.MethodSuccess;
        }
    }

    private async Task<Result<int, Error>> EnsureSchemaExists(string schemaName)
    {
        var schemaUpper = schemaName.ToUpper();

        using SqlCommand checkCmd = new()
        {
            CommandText = "SELECT COUNT(*) FROM sys.schemas WHERE name = @SchemaName",
            Connection = _connection,
            CommandTimeout = 30
        };
        checkCmd.Parameters.AddWithValue("@SchemaName", schemaUpper);

        try
        {
            var result = await checkCmd.ExecuteScalarAsync();
            var count = result != null ? (int)result : 0;
            if (count == 0)
            {
                _logger?.LogInformation("Schema {Schema} does not exist, creating it", schemaUpper);

                using SqlCommand createCmd = new()
                {
                    CommandText = $"CREATE SCHEMA {schemaUpper}",
                    Connection = _connection,
                    CommandTimeout = 30
                };

                await createCmd.ExecuteNonQueryAsync();
                _logger?.LogInformation("Schema {Schema} created successfully", schemaUpper);
            }
            else
            {
                _logger?.LogDebug("Schema {Schema} already exists", schemaUpper);
            }

            return Constants.MethodSuccess;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Failed to ensure schema {Schema} exists", schemaUpper);
            return new Error { ExceptionMessage = ex.Message };
        }
    }

    private static string MapDataType(Type type)
    {
        return type switch
        {
            _ when type == typeof(string) => "NVARCHAR(500)",
            _ when type == typeof(int) => "INT",
            _ when type == typeof(long) => "BIGINT",
            _ when type == typeof(float) => "FLOAT",
            _ when type == typeof(double) => "FLOAT",
            _ when type == typeof(decimal) => "DECIMAL(18, 2)",
            _ when type == typeof(bool) => "BIT",
            _ when type == typeof(DateTime) => "DATETIME",
            _ => "NVARCHAR(500)"
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
                    _logger?.LogInformation("Reading packet data from reader array: Current table {Schema}.{TableName} line count: {LineCount} lines.",
                        sysName, tableName, table.Rows.Count);

                    logLineCount = 0;
                }

                if (table.Rows.Count >= packetSize)
                {
                    _logger?.LogInformation("Inserting packet data from reader array: Current table {Schema}.{TableName} line count: {LineCount} lines.",
                        sysName, tableName, table.Rows.Count);

                    Result<int, Error> innerInsert;
                    int attempt = 0;

                    do
                    {
                        attempt++;
                        innerInsert = await homeServer.BulkInsert(table, tableName, sysName, database);
                        if (!innerInsert.IsOk)
                        {
                            _logger?.LogWarning("Error while attempting to transfer data from packet: {Error}. Attempt {Attempt} - out of 5. Table: {Schema}.{TableName}",
                                innerInsert.Error.ExceptionMessage, attempt, sysName, tableName);
                        }
                    } while (!innerInsert.IsOk && attempt < 5);

                    if (!innerInsert.IsOk)
                    {
                        _logger?.LogError("Table insert attempt has reached maximum attempt count. Table: {Schema}.{TableName}", sysName, tableName);
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
                        _logger?.LogWarning("Error while attempting to transfer data from packet: {Error}. Attempt {Attempt} - out of 5. Table: {Schema}.{TableName}",
                            outerInsert.Error.ExceptionMessage, attempt, sysName, tableName);
                    }
                } while (!outerInsert.IsOk && attempt < 5);

                if (!outerInsert.IsOk)
                {
                    _logger?.LogError("Table insert attempt has reached maximum attempt count. Table: {Schema}.{TableName}", sysName, tableName);
                    return new Error() { ExceptionMessage = $"Error attempting to insert data to server: {outerInsert.Error.ExceptionMessage}" };
                }
            }
            return Constants.MethodSuccess;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error while attempting insert");
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
            _logger?.LogError(ex, "Error while fetching data from server, command text: {CommandText}", cmd.CommandText);
            return new Error() { ExceptionMessage = ex.Message };
        }
    }

    private async Task ChangeDatabase(string? database = null)
    {
        if (database != null)
        {
            _logger?.LogDebug("Changing database to {Database}", database);
            await _connection.ChangeDatabaseAsync(database);
            _logger?.LogDebug("Database changed to {Database}", _connection.Database);
        }
    }

    private async Task<bool> IsCreated(string tableName, string? database = null)
    {
        await ChangeDatabase(database);

        using SqlCommand cmd = new()
        {
            CommandText = $"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{tableName.ToUpper()}'",
            Connection = _connection,
            CommandTimeout = 30
        };

        try
        {
            var ret = await cmd.ExecuteScalarAsync() ?? "0";
            bool exists = int.Parse(ret.ToString()!) == Constants.MethodSuccess;
            _logger?.LogDebug("Table existence check for {TableName}: {Exists}", tableName.ToUpper(), exists);
            return exists;
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Error checking if table {TableName} exists, assuming it doesn't", tableName.ToUpper());
            return false;
        }
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    private void Dispose(bool disposing)
    {
        if (_disposed)
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

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        await _connection.CloseAsync();
        await _connection.DisposeAsync();
        _disposed = true;

        GC.SuppressFinalize(this);
    }

    ~SqlServerCall()
    {
        Dispose(false);
    }
}