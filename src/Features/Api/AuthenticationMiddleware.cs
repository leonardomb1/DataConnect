using DataConnect.Features.Authentication.Services;

namespace DataConnect.Features.Api;

public class AuthenticationMiddleware
{
    private readonly RequestDelegate _next;
    private readonly IConfiguration _configuration;

    public AuthenticationMiddleware(RequestDelegate next, IConfiguration configuration)
    {
        _next = next;
        _configuration = configuration;
    }

    public async Task InvokeAsync(HttpContext context, IAuthenticationService authService)
    {
        if (context.Request.Path.StartsWithSegments("/api") && !context.Request.Path.StartsWithSegments("/api/health"))
        {
            var authSecret = _configuration["Auth:Secret"];
            if (string.IsNullOrEmpty(authSecret))
            {
                context.Response.StatusCode = 500;
                await context.Response.WriteAsync("Auth secret not configured");
                return;
            }

            var token = context.Request.Headers["token"].FirstOrDefault();
            if (string.IsNullOrEmpty(token) || !authService.ValidateToken(token, authSecret))
            {
                context.Response.StatusCode = 401;
                await context.Response.WriteAsync("Unauthorized");
                return;
            }
        }

        await _next(context);
    }
}