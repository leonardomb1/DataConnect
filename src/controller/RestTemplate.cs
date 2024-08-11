using System.Text.Json;
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
    public static async Task<Result<dynamic, int>> TemplatePostMethod(HttpContextBase ctx, string method, object?[] param)
    {
        string res = "";

        try
        {
            if (RequestValidate.IsValidDeserialized(ctx.Request.DataAsString))
            {
                Result<BodyDefault, int> result = RequestValidate.GetBodyDefault(ctx.Request.DataAsString);

                if (!result.IsOk)
                {
                    res = JsonSerializer.Serialize(new Response() {
                        Error = true,
                        Status = 400,
                        Message = "Bad Request",
                        Options = []
                    });
                    ctx.Response.StatusCode = 400;
                    await ctx.Response.Send(res);
                    return ReturnedValues.MethodFail;
                }

                var req = result.Value;
                using var client = new HttpClient();
                using var tasker = new HttpSender(req.ConnectionInfo, client);

                MethodInfo execute = typeof(HttpSender).GetMethod(method)!;
                
                dynamic jsonReturn = await Task<dynamic>.Factory.StartNew(() => execute.Invoke(tasker, param)!).Result;

                res = JsonSerializer.Serialize(new Response() {
                    Error = false,
                    Status = 201,
                    Message = "Schedule was successfully triggered in the server.",
                    Options = []
                });

                ctx.Response.StatusCode = 201;
                await ctx.Response.Send(res);
                return jsonReturn;
            } else {
                res = JsonSerializer.Serialize(new Response() {
                    Error = true,
                    Status = 400,
                    Message = "Bad Request",
                    Options = []
                });
                ctx.Response.StatusCode = 400;
                await ctx.Response.Send(res);
                return ReturnedValues.MethodFail;
            }
        }
        catch (Exception ex)
        {
            res = JsonSerializer.Serialize(new Response() {
                Error = true,
                Status = 500,
                Message = "An internal serval error occurred which stopped the completion of the request.",
                Options = []
            });
            Log.Out($"Error occured after request {ex.Message}");
            ctx.Response.StatusCode = 500;
            return ReturnedValues.MethodFail;
        }

    }

    public static async Task<Result<BodyDefault, int>> RequestStart(HttpContextBase ctx)
    {
        Log.Out(
            $"Receiving {ctx.Request.Method} request for {ctx.Request.Url.RawWithoutQuery} " + 
            $"by {ctx.Request.Source.IpAddress}:{ctx.Request.Source.Port}"
        );
    
        var attempt = RequestValidate.GetBodyDefault(ctx.Request.DataAsString);

        if (attempt.IsOk) {
            return attempt.Value;
        } else {
            string res = JsonSerializer.Serialize(new Response() {
                Error = true,
                Status = 400,
                Message = "Bad Request",
                Options = []
            });
            ctx.Response.StatusCode = 400;

            await ctx.Response.Send(res);
            return ReturnedValues.MethodFail;
        }
    }
}
