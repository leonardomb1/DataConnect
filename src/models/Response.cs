using System.Text.Json;
using System.Text.Json.Serialization;
using WatsonWebserver.Core;

namespace DataConnect.Models;

public class Response : IDisposable
{
    private bool _disposed;
    private static readonly JsonSerializerOptions _options = new() { DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull };
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
        string res = JsonSerializer.Serialize(new Response() {
                Error = hasError,
                Message = msg,
                Status = statusCode,
                Inner = inner,
                Options = strings
        }, _options);

        ctx.Response.StatusCode = statusCode;
        await ctx.Response.Send(res);
    }

    public static async Task BadRequest(HttpContextBase ctx)
    {
        string res = JsonSerializer.Serialize(new Response() {
            Error = true,
            Message = "Bad Request",
            Status = 400
        }, _options);

        ctx.Response.StatusCode = 400;
        await ctx.Response.Send(res);
    }

    public static async Task NotFound(HttpContextBase ctx)
    {
        string res = JsonSerializer.Serialize(new Response() {
            Error = true,
            Message = "Not Found",
            Status = 404
        }, _options);

        ctx.Response.StatusCode = 404;
        await ctx.Response.Send(res);
    }

    public static async Task InternalServerError(HttpContextBase ctx)
    {
        string res = JsonSerializer.Serialize(new Response() {
            Error = true,
            Message = "Internal Server Error",
            Status = 500
        }, _options);

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
        }, _options);

        ctx.Response.StatusCode = 207;
        await ctx.Response.Send(res);  
    }

    public static async Task Ok(HttpContextBase ctx)
    {
        string res = JsonSerializer.Serialize(new Response() {
            Error = false,
            Message = "OK",
            Status = 200
        }, _options);

        ctx.Response.StatusCode = 200;
        await ctx.Response.Send(res);  
    }

    public static async Task Unauthorized(HttpContextBase ctx)
    {
        string res = JsonSerializer.Serialize(new Response() {
            Error = true,
            Message = "Not Authorized",
            Status = 401
        }, _options);

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
