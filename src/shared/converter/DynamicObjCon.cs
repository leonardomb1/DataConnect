using System.Data;
using System.Text.Json;

namespace DataConnect.Shared.Converter;

public static class DynamicObjConvert
{
    public static DataTable JsonDynamicToDataTable(dynamic obj)
    {
        ArgumentNullException.ThrowIfNull(obj, nameof(obj));

        var table = new DataTable();

        JsonElement data = JsonSerializer.Deserialize<JsonElement>(obj);
        var firstObjProp = data.EnumerateObject();

        foreach (var element in firstObjProp)
        {
            table.Columns.Add(element.Name);
        }

        DataRow lin = table.NewRow();
        foreach (var item in firstObjProp)
        {
            lin[item.Name] = item.Value;
        }
        table.Rows.Add(lin);
        
        return table;
    }
}