using System.Data;
using System.Text.Json.Nodes;
using DataConnect.Models;
using DataConnect.Types;


namespace DataConnect.Etl.Converter;

public static class DynamicObjConvert
{
    public static Result<DataTable, Error> JsonToDataTable(JsonNode obj)
    {
        JsonArray array = obj.AsArray();
        JsonObject firstObject = array[0]!.AsObject();

        var table = new DataTable();

        try {
            foreach (var property in firstObject) {
                table.Columns.Add(property.Key);
            }

            foreach (JsonObject json in array.Cast<JsonObject>()) {
                DataRow row = table.NewRow();
                foreach (var property in json) {
                    if (!table.Columns.Contains(property.Key)) {
                        table.Columns.Add(property.Key);
                    }
                    row[property.Key] = property.Value?.ToString() ?? "";
                }
                table.Rows.Add(row);
            }
            return table;
        } catch (Exception ex) {
            return new Error() { ExceptionMessage = $"Error while attempting to parse JSON into DataTable, {ex.Message}" };
        }
    } 
}