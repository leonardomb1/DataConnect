using System.Data;
using System.Text.Json;
using DataConnect.Models;
using DataConnect.Types;


namespace DataConnect.Etl.Converter;

public static class DynamicObjConvert
{
    public static Result<DataTable, Error> FromInnerJsonToDataTable(dynamic obj, string prop)
    {
        ArgumentNullException.ThrowIfNull(obj, nameof(obj));

        string json = Convert.ToString(obj);
        var table = new DataTable();

        try {
            JsonDocument document = JsonDocument.Parse(json);
            JsonElement root = document.RootElement.GetProperty(prop);

            if (root.ValueKind == JsonValueKind.Array)
            {
                foreach (JsonElement element in root.EnumerateArray())
                {
                    if (table.Columns.Count == 0)
                    {
                        foreach (JsonProperty property in element.EnumerateObject())
                        {
                            table.Columns.Add(property.Name);
                        }
                    }

                    DataRow row = table.NewRow();
                    foreach (JsonProperty property in element.EnumerateObject())
                    {
                        if (!table.Columns.Contains(property.Name)) {
                            table.Columns.Add(property.Name);
                        }
                        row[property.Name] = property.Value.ToString();
                    }
                    table.Rows.Add(row);
                }
            }

            return table;
        } catch (Exception ex) {
            return new Error() { ExceptionMessage = $"Error while attempting to parse JSON into DataTable, {ex.Message}" };
        }
    } 
}