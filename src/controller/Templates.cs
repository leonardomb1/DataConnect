using System.Text.Json;
using DataConnect.Etl.Http;
using DataConnect.Shared;
using DataConnect.Validator;
using DataConnect.Models;
using WatsonWebserver.Core;
using System.Reflection;

namespace DataConnect.Controller;
public static class RestTemplate
{
    public static async Task<dynamic> TemplatePostMethod(HttpContextBase ctx, string method, object?[] param)
    {
        string res = "";
        dynamic ret = "";

        try
        {
            if (!RequestValidate.IsValidDeserialized(ctx.Request.DataAsString)) throw new Exception();
            try
            {
                var req = JsonSerializer.Deserialize<BodyDefault>(ctx.Request.DataAsString);
                using var client = new HttpClient();
                using var tasker = new HttpSender(req!.ConnectionInfo, client);
                
                MethodInfo execute = typeof(HttpSender).GetMethod(method)!;

                ret = await Task<dynamic>.Factory.StartNew(() => execute.Invoke(tasker, param)!).Result;

                res = JsonSerializer.Serialize(new Response() {
                    Error = false,
                    Status = 201,
                    Message = "Schedule was successfully triggered in the server.",
                    Options = []
                });

                ctx.Response.StatusCode = 201;
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
            }
        }
        catch
        {
            res = JsonSerializer.Serialize(new Response() {
                Error = true,
                Status = 400,
                Message = "Bad Request",
                Options = []
            });
            ctx.Response.StatusCode = 400;  
        }
        finally
        {
            Log.Out($"Response was: {ctx.Response.ResponseSent}");
            await ctx.Response.Send(res);
        }

        return ret;
    }
}