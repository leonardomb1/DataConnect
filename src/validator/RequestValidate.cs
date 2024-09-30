using Microsoft.IdentityModel.Tokens;
using System.Text.Json;
using System.Text.Json.Nodes;
using DataConnect.Models;
using DataConnect.Types;
using WatsonWebserver.Core;

namespace DataConnect.Validator;

public static class RequestValidate
{
    public static Result<BodyDefault, Error> GetBodyDefault(dynamic json)
    {
        if(IsValidDeserialized(json))
        {
            return JsonSerializer.Deserialize<BodyDefault>(json);
        }
        else {
            return new Error { ExceptionMessage = "Error occured when trying to interpret JSON input." };
        }
    }

    public static Result<JsonObject, Error> GetJsonObject(dynamic json)
    {
        try {
            JsonObject obj = JsonSerializer.Deserialize<JsonObject>(json);
            return obj;
        } catch (Exception ex) {
            return new Error() { ExceptionMessage = $"Error occured after attempting to deserialize JSON, {ex.Message}"};
        }
    }

    public static bool ContainsCorrectHeader(HttpContextBase ctx, string[] headers) => 
        headers.Select(ctx.Request.HeaderExists).All(x => x == true);
    
    public static bool IsValidJson(dynamic json)
    {
        string value = Convert.ToString(json);

        if (value.IsNullOrEmpty()) return false;

        try
        {
            JsonDocument.Parse(value);
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }
    public static bool IsValidDeserialized(dynamic json)
    {
        try
        {
            JsonSerializer.Deserialize<BodyDefault>(json);
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }
}