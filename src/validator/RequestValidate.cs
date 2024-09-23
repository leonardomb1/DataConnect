using Microsoft.IdentityModel.Tokens;
using System.Text.Json;
using DataConnect.Models;
using DataConnect.Types;
using DataConnect.Shared;
using WatsonWebserver.Core;

namespace DataConnect.Validator;

public static class RequestValidate
{
    public static Result<BodyDefault, int> GetBodyDefault(dynamic json)
    {
        if(IsValidDeserialized(json))
        {
            return JsonSerializer.Deserialize<BodyDefault>(json);
        }
        else {
            return Constants.MethodFail;
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