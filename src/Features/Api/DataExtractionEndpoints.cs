using Microsoft.AspNetCore.Mvc;
using DataConnect.Features.DataExtraction.Models;
using DataConnect.Features.DataExtraction.Services;

namespace DataConnect.Features.Api;

public static class DataExtractionEndpoints
{
    public static void MapDataExtractionEndpoints(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapPost("/api/extract/paginated", async (
            [FromBody] ExtractRequest request,
            [FromServices] IDataExtractionService extractionService,
            CancellationToken cancellationToken) =>
        {
            if (!int.TryParse(request.Options[4], out int lookBackTime))
            {
                return Results.BadRequest("Invalid lookback time in options[4]");
            }

            var filteredDate = DateTime.Today.AddDays(-lookBackTime).ToString("dd/MM/yyyy");
            var result = await extractionService.ExtractPaginatedDataAsync(
                request, filteredDate, ["dtde", "dtate"], cancellationToken);

            return result.Success
                ? Results.Ok(result)
                : Results.BadRequest(result);
        })
        .WithName("ExtractPaginatedData")
        .WithOpenApi(op => new(op)
        {
            Summary = "Extract paginated data from external API",
            Description = "Performs paginated data extraction with concurrent processing"
        });

        endpoints.MapPost("/api/extract/simple", async (
            [FromBody] ExtractRequest request,
            [FromServices] IDataExtractionService extractionService,
            CancellationToken cancellationToken) =>
        {
            if (!int.TryParse(request.Options[4], out int lookBackTime))
            {
                return Results.BadRequest("Invalid lookback time in options[4]");
            }

            var filteredDate = DateTime.Today.AddDays(-lookBackTime).ToString("dd/MM/yyyy");
            var result = await extractionService.ExtractSimpleDataAsync(
                request, filteredDate, ["dtinicio", "dtfim"], cancellationToken);

            return result.Success
                ? Results.Ok(result)
                : Results.BadRequest(result);
        })
        .WithName("ExtractSimpleData")
        .WithOpenApi(op => new(op)
        {
            Summary = "Extract simple data from external API",
            Description = "Performs single-request data extraction"
        });

        endpoints.MapPost("/api/extract/basic", async (
            [FromBody] ExtractRequest request,
            [FromServices] IDataExtractionService extractionService,
            CancellationToken cancellationToken) =>
        {
            var result = await extractionService.ExtractBasicDataAsync(request, cancellationToken);

            return result.Success
                ? Results.Ok(result)
                : Results.BadRequest(result);
        })
        .WithName("ExtractBasicData")
        .WithOpenApi(op => new(op)
        {
            Summary = "Extract basic configuration data",
            Description = "Extracts basic configuration data without date filtering"
        });
    }
}