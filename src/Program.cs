using DataConnect.Features.DataExtraction.Services;
using DataConnect.Features.Authentication.Services;
using DataConnect.Features.HealthCheck.Services;
using DataConnect.Infrastructure.Json;
using DataConnect.Features.Api;

var builder = WebApplication.CreateBuilder(args);

// Add feature services
builder.Services.AddHttpClient();
builder.Services.AddScoped<IJsonDataTableConverter, JsonDataTableConverter>();
builder.Services.AddScoped<IDataExtractionService, DataExtractionService>();
builder.Services.AddScoped<IAuthenticationService, AuthenticationService>();
builder.Services.AddScoped<IHealthCheckService, HealthCheckService>();

// Add OpenAPI/Swagger
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
    c.SwaggerDoc("v1", new() { Title = "DataConnect API", Version = "v1.0.2" });
});

// Add logging
builder.Logging.ClearProviders();
builder.Logging.AddConsole();

var app = builder.Build();

// Configure pipeline
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI(c => c.SwaggerEndpoint("/swagger/v1/swagger.json", "DataConnect API v1.0.2"));
}

// Add authentication middleware
app.UseMiddleware<AuthenticationMiddleware>();

// Map feature endpoints
app.MapDataExtractionEndpoints();
app.MapHealthCheckEndpoints();

app.Run();