using System.Text.Json;
using System.Text.Json.Serialization;
using WatsonWebserver.Core;

namespace DataConnect.Models;

public class Response : IDisposable
{
    private bool _disposed;
    public required int Status {get; set;}
    public required bool Error {get; set;}
    public required string Message {get; set;}
    public string[]? Options {get; set;}
    public object[]? Inner {get; set;}

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    public static async Task SendAsString(HttpContextBase ctx, bool hasError, string msg, int statusCode, object[]? inner = null, string[]? strings = null)
    {
        var serializerOptions = new JsonSerializerOptions() {
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        };

        string res = JsonSerializer.Serialize(new Response() {
                Error = hasError,
                Message = msg,
                Status = statusCode,
                Inner = inner,
                Options = strings
        }, serializerOptions);

        ctx.Response.StatusCode = statusCode;
        await ctx.Response.Send(res);
    }

    public static async Task BadRequest(HttpContextBase ctx)
    {
        string res = JsonSerializer.Serialize(new Response() {
            Error = true,
            Message = "Bad Request",
            Status = 400
        });

        ctx.Response.StatusCode = 400;
        await ctx.Response.Send(res);
    }

    public static async Task NotFound(HttpContextBase ctx)
    {
        string res = JsonSerializer.Serialize(new Response() {
            Error = true,
            Message = "Not Found",
            Status = 404
        });

        ctx.Response.StatusCode = 404;
        await ctx.Response.Send(res);
    }

    public static async Task InternalServerError(HttpContextBase ctx)
    {
        string res = JsonSerializer.Serialize(new Response() {
            Error = true,
            Message = "Internal Server Error",
            Status = 500
        });

        ctx.Response.StatusCode = 500;
        await ctx.Response.Send(res);  
    }

    public static async Task MultiStatus(HttpContextBase ctx, Response[] inner)
    {
        string res = JsonSerializer.Serialize(new Response() {
            Error = true,
            Message = "Multi Status",
            Status = 207,
            Inner = inner
        });

        ctx.Response.StatusCode = 207;
        await ctx.Response.Send(res);  
    }

    public static async Task Ok(HttpContextBase ctx)
    {
        string res = JsonSerializer.Serialize(new Response() {
            Error = false,
            Message = "OK",
            Status = 200
        });

        ctx.Response.StatusCode = 200;
        await ctx.Response.Send(res);  
    }

    public static async Task Unauthorized(HttpContextBase ctx)
    {
        string res = JsonSerializer.Serialize(new Response() {
            Error = true,
            Message = "Not Authorized",
            Status = 401
        });

        ctx.Response.StatusCode = 401;
        await ctx.Response.Send(res);  
    }

    private void Dispose(bool disposing)
    {
        if (!_disposed) {
            return;
        }

        if (disposing) {
            //
        }

        _disposed = true;
    }

    ~Response()
    {
        Dispose(false);
    }
}
