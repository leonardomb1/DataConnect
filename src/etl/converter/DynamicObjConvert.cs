using System.Data;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace DataConnect.Etl.Converter;

public static class DynamicObjConvert
{
    public static DataTable FromInnerJsonToDataTable(dynamic obj, string prop)
    {
        ArgumentNullException.ThrowIfNull(obj, nameof(obj));

        var table = new DataTable();

        // Deserialize once, instead of dynamically each time
        JsonNode json = JsonSerializer.Deserialize<JsonObject>(obj);
        JsonArray jsonList = json[prop]!.AsArray();

        // Predefine columns (assuming the first row contains all keys)
        if (jsonList.Count > 0)
        {
            var firstRow = jsonList[0]!.AsObject();
            foreach (var element in firstRow)
            {
                table.Columns.Add(element!.Key);
            }
        }

        table.BeginLoadData(); // Start bulk data load

        // Process rows and populate the DataTable
        foreach (var node in jsonList)
        {
            DataRow row = table.NewRow();

            foreach (var element in node!.AsObject())
            {
                var value = element.Value ?? JsonNode.Parse($"\"\""); // Handle null values

                if (value!.GetValueKind() != JsonValueKind.Array && value.GetValueKind() != JsonValueKind.Object)
                {
                    row[element.Key] = value.GetValue<string>(); // Adjust to specific types if necessary
                }
                else
                {
                    row[element.Key] = value.ToJsonString(); // Handle complex types as JSON strings
                }
            }

            table.Rows.Add(row); // Add row to DataTable
        }

        table.EndLoadData(); // End bulk data load
        
        return table;
    }

}