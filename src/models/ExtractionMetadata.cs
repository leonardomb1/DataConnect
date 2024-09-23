using System.Data;

namespace DataConnect.Models;

public class ExtractionMetadata : IDisposable
{
    private bool _disposed;
    public required int ExtractId {get; set;}
    public required string TableName {get; set;}
    public required int ScheduleId {get; set;}
    public required int SystemId {get; set;}
    public required char TableType {get; set;}
    public string? ColumnName {get; set;}
    public int? LookBackValue {get; set;}
    public string? IndexName {get; set;}
    public required string SystemName {get; set;}
    public required string ConnectionString {get; set;}

    public static List<ExtractionMetadata> ConvertFromDataTable(DataTable table)
    {
        var list = new List<ExtractionMetadata>();

        foreach (DataRow row in table.Rows)
        {
            var obj = new ExtractionMetadata
            {
                ExtractId = row.Field<int>("ID_DW_EXTLIST"),
                TableName = row.Field<string>("NM_TABELA")!,
                ScheduleId = row.Field<int>("ID_DW_AGENDADOR"),
                SystemId = row.Field<int>("ID_DW_SISTEMA"),
                TableType = row.Field<string>("TP_TABELA")![0]!,
                ColumnName = row.Field<string?>("NM_COLUNA") ?? "",
                LookBackValue = row.Field<int?>("VL_INC_TABELA") ?? 0,
                IndexName = row.Field<string?>("NM_INDIC") ?? "",
                SystemName = row.Field<string>("NM_SISTEMA")!,
                ConnectionString = row.Field<string>("DS_CONSTRING")!
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

    ~ExtractionMetadata()
    {
        Dispose(false);
    }    
}