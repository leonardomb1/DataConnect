using System.Data;
using Microsoft.Data.SqlClient;

namespace DataConnect.Etl.Sql;

public class SqlServerCall(string conStr) : IDisposable
{
    private bool _disposed;
    private readonly SqlConnection _connection = new(conStr);

    public async Task BulkInsert(DataTable table,
                                    string tableName,
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
            DestinationTableName = tableName
        };

        await bulkCopy.WriteToServerAsync(table);
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