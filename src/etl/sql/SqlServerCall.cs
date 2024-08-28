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
        GetConnection();
        
        if (database != null) 
        {
            _connection.ChangeDatabase(database);
        }
        
        using SqlBulkCopy bulkCopy = new(_connection) 
        {
            BulkCopyTimeout = 1000,
            DestinationTableName = $"{sysName.ToUpper()}.{tableName.ToUpper()}"
        };
            
        try {
            await bulkCopy.WriteToServerAsync(table);
            Log.Out($"API return has been written into the table: {sysName.ToUpper()}.{tableName.ToUpper()}");
            return Constants.MethodSuccess;
        } catch (SqlException ex) {
            Log.Out($"Error while attempting insert: {ex.Message}");
            return Constants.MethodFail;
        } finally {
            _connection.Close();
        }
    }

    public async Task CreateTable(DataTable table,
                                  string tableName,
                                  string sysName,
                                  string? database = null)
    {
        if (!IsCreated(tableName, database))
        {
            GetConnection();
            
            Log.Out($"Failed to find table {sysName.ToUpper()}.{tableName.ToUpper()}, proceeding with creation...");
            using SqlCommand cmd = new("", _connection);

            string createTableQuery = $"CREATE TABLE {sysName.ToUpper()}.{tableName.ToUpper()} (";

            foreach (DataColumn column in table.Columns)
            {
                createTableQuery += $"[{column.ColumnName}] NVARCHAR(MAX), ";
            }
            createTableQuery += $"ID_DW_{tableName.ToUpper()} INT NOT NULL IDENTITY(1,1) CONSTRAINT IX_{tableName.ToUpper()}_SK PRIMARY KEY, ";
            createTableQuery += $"DT_UPDATE_{tableName.ToUpper()} DATETIME NOT NULL CONSTRAINT CK_UPDATE_{tableName.ToUpper()} DEFAULT(GETDATE()));";
            cmd.CommandText = createTableQuery;
            await cmd.ExecuteNonQueryAsync();
        } else {
            Log.Out($"Table {tableName.ToUpper()} already exists.");
        }
        _connection.Close();
    }

    public DataTable GetTableFromServer(string database, SqlCommand cmd, string[]? options = null)
    {
        GetConnection();

        if (database != null)
        {
            _connection.ChangeDatabase(database);
        }      

        DataTable data = new() {
            TableName = options?[0] ?? "Table"
        };

        using SqlDataAdapter adapter = new(cmd);
        adapter.Fill(data);

        _connection.Close();

        return data;
    }

    public void ExecuteCommand(SqlCommand cmd)
    {
        GetConnection();
        cmd.ExecuteNonQuery();
        _connection.Close();
    }

    public void CleanDestination(string sysName, string tableName, object[] options, string stmt)
    {
        GetConnection();

        using SqlCommand cmd = new() {
            Connection = _connection
        };

        int lineCount = GetLineCount(sysName, tableName);

        switch (lineCount, options[0])
        {
            case (> 0, Constants.Incremental):
                cmd.CommandText = stmt;
                cmd.Parameters.AddWithValue("@NOMEIND", options[1]);
                cmd.Parameters.AddWithValue("@TABELA", options[2]);
                cmd.Parameters.AddWithValue("@VL_CORTE", options[3].ToString());
                cmd.Parameters.AddWithValue("@COL_DT", options[4]);
                cmd.ExecuteNonQuery();

                Log.Out($"Table {tableName}");
                break;
            case (0, Constants.Incremental):
                cmd.CommandText = $"TRUNCATE TABLE {sysName}.{tableName};";
                cmd.ExecuteNonQuery();
                break;            
            case (_, Constants.Total):
                cmd.CommandText = $"TRUNCATE TABLE {sysName}.{tableName};";
                cmd.ExecuteNonQuery();
                break;
            default:
                break;
        }

        cmd.ExecuteNonQuery();

        _connection.Close();
    }
    private int GetLineCount(string sysName, string tableName)
    {
        GetConnection();

        using SqlCommand cmd = new() {
            CommandText = $"SELECT COUNT(1) FROM {sysName.ToUpper()}.{tableName.ToUpper()} WITH(NOLOCK);",
            Connection = _connection
        };

        var ret = cmd.ExecuteScalar() ?? "0";

        _connection.Close();
        return int.Parse(cmd.ToString()!);
    }

    private void GetConnection()
    {
        if (_connection.State != ConnectionState.Open) {
            _connection.Open();
        }
    }

    private bool IsCreated(string tableName, string? database = null)
    {
        GetConnection();

        if (database != null) 
        {
            _connection.ChangeDatabase(database);
        }        

        using SqlCommand cmd = new() {
            CommandText = $"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{tableName.ToUpper()}'",
            Connection = _connection
        };

        var ret = cmd.ExecuteScalar() ?? "0";
        _connection.Close();

        return int.Parse(ret.ToString()!) == Constants.MethodSuccess;
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