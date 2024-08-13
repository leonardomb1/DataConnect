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

        JsonNode json = JsonSerializer.Deserialize<JsonObject>(obj);
        JsonArray jsonList = json[prop]!.AsArray();

        foreach (var element in jsonList.FirstOrDefault()!.AsObject())
        {
            table.Columns.Add(element!.Key);
        }

        foreach (var node in jsonList)
        {
            DataRow lin = table.NewRow();

            foreach (var element in node!.AsObject())
            {
                var value = element.Value ?? JsonNode.Parse($"\"\"");
                if (!table.Columns.Contains(element.Key))
                {
                    table.Columns.Add(element.Key);
                }

                if (value!.GetValueKind() != JsonValueKind.Array && value!.GetValueKind() != JsonValueKind.Object)
                {
                    lin[element.Key] = value.GetValue<dynamic>();
                } else {
                    lin[element.Key] = value.ToJsonString();
                }
            }

            table.Rows.Add(lin);
        }
         
        return table;
    }
}