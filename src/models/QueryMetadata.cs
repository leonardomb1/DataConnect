using System.Data;

namespace DataConnect.Models;

public class QueryMetadata : IDisposable
{
    private bool _disposed;
    public required int QueryId {get; set;}
    public required int SystemId {get; set;}
    public required char QueryType {get; set;}
    public required string QueryText {get; set;}

    public static List<QueryMetadata> ConvertFromDataTable(DataTable table)
    {
        var list = new List<QueryMetadata>();

        foreach (DataRow row in table.Rows)
        {
            var obj = new QueryMetadata
            {
                QueryId = row.Field<int>("ID_DW_CONSULTA"),
                SystemId = row.Field<int>("ID_DW_SISTEMA"),
                QueryType = row.Field<string>("TP_CONSULTA")![0]!,
                QueryText = row.Field<string>("DS_CONSULTA")!,
            };

            list.Add(obj);
        }

        return list;
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
            //
        }

        _disposed = true;
    }

    ~QueryMetadata()
    {
        Dispose(false);
    }
}