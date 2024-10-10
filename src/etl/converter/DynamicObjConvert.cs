using System.Data;
using System.Text.Json.Nodes;
using DataConnect.Models;
using DataConnect.Types;


namespace DataConnect.Etl.Converter;

public static class DynamicObjConvert
{
    public static Result<DataTable, Error> JsonToDataTable(JsonNode obj, int fieldCharLimit)
    {
        try {
            JsonArray array = obj.AsArray();
            JsonObject firstObject = array[0]!.AsObject();

            var table = new DataTable();
            foreach (var property in firstObject) {
                table.Columns.Add(property.Key);
            }

            foreach (JsonObject json in array.Cast<JsonObject>()) {
                DataRow row = table.NewRow();
                foreach (var property in json) {
                    if (!table.Columns.Contains(property.Key)) {
                        table.Columns.Add(property.Key);
                    }
                    var value = property.Value?.ToString() ?? "";
                    if (value.Length > fieldCharLimit) {
                        value = value[..fieldCharLimit];
                    }
                    row[property.Key] = value;
                }

                foreach (DataColumn column in table.Columns) {
                    if (!json.ContainsKey(column.ColumnName)) {
                        row[column.ColumnName] = DBNull.Value;
                    }
                }

                table.Rows.Add(row);
            }
            return table;
        } catch (Exception ex) {
            return new Error() { ExceptionMessage = $"Error while attempting to parse JSON into DataTable, {ex.Message}" };
        }
    }
}