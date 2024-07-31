using System.Data;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace DataConnect.Shared.Converter;

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
        DataRow lin = table.NewRow();

        foreach (var node in jsonList)
        {
            foreach (var element in node!.AsObject())
            {
                var value = element.Value ?? JsonNode.Parse($"\"\"");
                if (value!.GetValueKind() != JsonValueKind.Array && value!.GetValueKind() != JsonValueKind.Object)
                {
                    lin[element.Key] = value.GetValue<dynamic>();
                }
                lin[element.Key] = value.ToJsonString();
            }
        }
        
        table.Rows.Add(lin);
        
        return table;
    }
}