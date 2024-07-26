using System.Data;

namespace DataConnect.Shared.Converter;

public static class DynamicObjConvert
{
    public static DataTable FromJsonToDataTable(dynamic obj)
    {
        ArgumentNullException.ThrowIfNull(obj, nameof(obj));

        var table = new DataTable();

        if (obj is IEnumerable<dynamic>)
        {
            foreach (var item in obj)
            {
                var row = table.NewRow();
                foreach (var prop in item.GetType().GetProperties())
                {
                    table.Columns.Add(prop.Name, prop.GetType());
                    row[prop.Name] = prop.GetValue(item);
                }
                table.Rows.Add(row);
            }
        } else
        {
            var row = table.NewRow();
            foreach (var prop in obj.GetType().GetProperties())
            {
                table.Columns.Add(prop.Name, prop.GetType());
                row[prop.Name] = prop.GetValue(obj);
            }
            table.Rows.Add(row);
        }

        return table;
    }
}