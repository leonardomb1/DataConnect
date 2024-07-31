using System.Data;
using DataConnect.Shared;
using Microsoft.Data.SqlClient;

namespace DataConnect.Etl.Sql;

public class SqlServerCall(string conStr) : IDisposable
{
    private bool _disposed;
    private readonly SqlConnection _connection = new(conStr);

    public async Task<int> BulkInsert(DataTable table,
                                    string tableName,
                                    string sysName,
                                    string? database = null)
    {
        _connection.Open();
        
        if (database != null) 
        {
            _connection.ChangeDatabase(database);
        }
        
        using SqlBulkCopy bulkCopy = new(_connection) 
        {
            BulkCopyTimeout = 1000,
            DestinationTableName = $"{sysName}.{tableName}"
        };

        await bulkCopy.WriteToServerAsync(table);
        Log.Out($"API return has been written into the table: {sysName}.{tableName}");
        _connection.Close();
        return ReturnedValues.MethodSuccess;
    }

    public async Task CreateTable(string tableName,
                                  DataTable table,
                                  string sysName,
                                  string? database = null)
    {
        if (!IsCreated(tableName, database))
        {
            Log.Out($"Failed to find table {sysName}.{tableName}, proceeding with creation...");
            using SqlCommand command = new("", _connection);

            string createTableQuery = $"CREATE TABLE {sysName}.{tableName} (";

            foreach (DataColumn column in table.Columns)
            {
                createTableQuery += $"[{column.ColumnName}] NVARCHAR(MAX), ";
            }
            createTableQuery += $"ID_DW_{tableName} INT IDENTITY(1,1) CONSTRAINT IX_{tableName}_SK PRIMARY KEY, ";
            createTableQuery += $"DT_UPDATE_{tableName} DATETIME CONSTRAINT CK_UPDATE_{tableName} DEFAULT(GETDATE()));";
            command.CommandText = createTableQuery;
            await command.ExecuteNonQueryAsync();
        } else {
            Log.Out($"Table {tableName} already exists.");
        }
        _connection.Close();
    }

    private bool IsCreated(string tableName, string? database = null)
    {
        _connection.Open();
        if (database != null) 
        {
            _connection.ChangeDatabase(database);
        }        

        using SqlCommand cmd = new() {
            CommandText = $"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{tableName}'",
            Connection = _connection
        };

        var ret = cmd.ExecuteScalar() ?? "0";
        return int.Parse(ret.ToString()!) == ReturnedValues.MethodSuccess;
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    private void Dispose(bool disposing)
    {
        if (!_disposed) {
            return;
        }

        if (disposing) {
           _connection.Dispose();
        }

        _disposed = true;
    }

    ~SqlServerCall()
    {
        Dispose(false);
    }
}