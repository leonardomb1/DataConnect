using Microsoft.IdentityModel.Tokens;
using System.Text.Json;
using DataConnect.Models;
using DataConnect.Types;
using WatsonWebserver.Core;

namespace DataConnect.Validator;

public static class RequestValidate
{

    public static Result<T, Error> GetDeserialized<T>(string json) {
    if(IsValidDeserialized<T>(json))
        {
            return JsonSerializer.Deserialize<T>(json)!;
        }
        else {
            return new Error { ExceptionMessage = "Error occured when trying to interpret JSON input." };
        }
    }

    public static bool ContainsCorrectHeader(HttpContextBase ctx, string[] headers) => 
        headers.Select(ctx.Request.HeaderExists).All(x => x == true);
    
    public static bool IsValidJson(string json)
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
    public static bool IsValidDeserialized<T>(string json)
    {
        try
        {
            JsonSerializer.Deserialize<T>(json);
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }
}