using System.Data;
using DataConnect.Shared;
using DataConnect.Types;
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
        await ChangeDatabase(database);
        
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
            EndConnection();
        }
    }

    public async Task CreateTable(DataTable table,
                                  string tableName,
                                  string sysName,
                                  string? database = null)
    {
        if (!await IsCreated(tableName, database))
        {
            GetConnection();
            await ChangeDatabase(database);
            
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
        EndConnection();
    }

    public async Task<DataTable> GetTableDataFromServer(string query, string? database = null)
    {
        GetConnection();
        await ChangeDatabase(database);

        DataTable data = new();

        using SqlCommand cmd = new() {
            Connection = _connection,
            CommandText = query
        };

        var reader = await cmd.ExecuteReaderAsync();

        data.Load(reader);

        EndConnection();
        return data;
    }

    public async Task ExecuteCommand(SqlCommand cmd, bool stayAlive = false, string? database = null)
    {
        GetConnection();
        await ChangeDatabase(database);     

        cmd.Connection = _connection;

        try {
            await cmd.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {      
            Log.Out($"Error while executing command: {ex.Message}");
        }
        finally {
            if (!stayAlive) EndConnection();
        }
    }

    public async Task<Result<int, dynamic>> ReadPacketFromServer(string query,
                                                                 int packetSize,
                                                                 string tableName,
                                                                 string sysName,
                                                                 SqlServerCall homeServer,
                                                                 int executionId,
                                                                 int extractionId,
                                                                 bool stayAlive = false,
                                                                 string? database = null)
    {
        GetConnection();
        
        using SqlDataReader reader = new SqlCommand() {
            CommandText = query,
            Connection = _connection
        }.ExecuteReader();

        using var table = new DataTable();

        try
        {
            for (int i = 0; i < reader.FieldCount; i++)
            {
                table.Columns.Add(reader.GetName(i), reader.GetFieldType(i));
            }

            while (reader.ReadAsync().Result)
            {
                DataRow row = table.NewRow();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    row[i] = reader.GetValue(i);
                }
                table.Rows.Add(row);
            
                if (table.Rows.Count >= packetSize)
                {
                    await BulkInsert(table, tableName, sysName, database);
                    table.Clear();
                }
            }

            if (table.Rows.Count > 0) {
                await homeServer.BulkInsert(table, tableName, sysName, database);
            }
            return Constants.MethodSuccess;
        }
        catch (Exception ex)
        {
            await Log.ToServer(
                $"Error while attempting insert: {ex.Message}",
                executionId,
                extractionId,
                Constants.LogErrorExecute,
                homeServer
            );
            return Constants.MethodFail;
        } finally {
            if (reader!= null &&!reader.IsClosed)
            {
                reader.Close();
            }
            if (!stayAlive) EndConnection();
        }
    }

    public async Task<Result<int, dynamic>> GetScalarDataFromServer(string query, bool stayAlive = false, string? database = null)
    {
        GetConnection();
        await ChangeDatabase(database);

        using SqlCommand cmd = new() {
            CommandText = query,
            Connection = _connection
        };

        try {
            var ret = await cmd.ExecuteScalarAsync() ?? "0";
            return int.Parse(ret.ToString()!);
        }
        catch (Exception ex)
        {      
            Log.Out($"Error while fetching data from server: {ex.Message}, command text: {cmd.CommandText}");
            return Constants.MethodFail;
        }
        finally {
            if (!stayAlive) EndConnection();
        }
    }

    private void GetConnection()
    {
        try
        {
            if (_connection.State != ConnectionState.Open && _connection.State != ConnectionState.Connecting) {
                _connection.Open();
            }   
        }
        catch (Exception ex)
        {
            Log.Out($"Error while attempting to open a connection, error: {ex}");
        }
    }

    private void EndConnection()
    {
        if (_connection.State == ConnectionState.Open) {
            _connection.Close();
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
        GetConnection();
        await ChangeDatabase(database);      

        using SqlCommand cmd = new() {
            CommandText = $"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{tableName.ToUpper()}'",
            Connection = _connection
        };

        var ret = cmd.ExecuteScalar() ?? "0";

        EndConnection();
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