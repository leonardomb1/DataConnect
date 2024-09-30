using DataConnect.Etl.Http;
using DataConnect.Shared;
using DataConnect.Validator;
using DataConnect.Models;
using DataConnect.Types;
using WatsonWebserver.Core;
using System.Reflection;

namespace DataConnect.Controller;
public static class RestTemplate
{
    public static async Task<Result<dynamic, Error>> TemplateRequestHandler(HttpContextBase ctx, HttpSender httpSender, string method, object?[] param)
    {
        try
        {
            if (RequestValidate.IsValidDeserialized(ctx.Request.DataAsString))
            {
                var result = RequestValidate.GetBodyDefault(ctx.Request.DataAsString);
                if (!result.IsOk) {
                    await Response.BadRequest(ctx);
                    return new Error() { ExceptionMessage = "JSON is not in correct format." };
                }

                MethodInfo execute = typeof(HttpSender).GetMethod(method)!;                
                dynamic jsonReturn = await Task<dynamic>.Factory.StartNew(() => execute.Invoke(httpSender, param)!).Result;
                
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
