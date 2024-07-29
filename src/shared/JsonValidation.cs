using Microsoft.IdentityModel.Tokens;
using System.Text.Json;

namespace DataConnect.Shared;

public static class JsonValidate
{
    public static bool IsValid(dynamic json)
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
}