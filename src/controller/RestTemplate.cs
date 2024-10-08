using DataConnect.Shared;
using DataConnect.Validator;
using DataConnect.Models;
using DataConnect.Types;
using WatsonWebserver.Core;
using System.Text.Json.Nodes;

namespace DataConnect.Controller;
public static class RestTemplate
{
    public static async Task<Result<JsonObject, Error>> TemplateRequestHandler(HttpContextBase ctx, Func<Task<Result<JsonObject, Error>>> getter)
    {
        try
        {
            if (RequestValidate.IsValidDeserialized<BodyDefault>(ctx.Request.DataAsString))
            {
                var result = RequestValidate.GetDeserialized<BodyDefault>(ctx.Request.DataAsString);
                if (!result.IsOk) {
                    await Response.BadRequest(ctx);
                    return new Error() { ExceptionMessage = "JSON is not in correct format." };
                }
             
                var callback = await getter();
                if (!callback.IsOk) {
                    return callback.Error;
                }

                JsonObject jsonReturn = callback.Value;
                
                return jsonReturn;
            } else {
                await Response.BadRequest(ctx);
                return new Error() { ExceptionMessage = "Not a valid JSON." };
            }
        }
        catch (Exception ex)
        {
            await Response.InternalServerError(ctx);
            Log.Out($"Error occured after request {ex.Message}");
            return new Error() { ExceptionMessage = $"Error occured after request {ex.Message}" };
        }
    }
}
