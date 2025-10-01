using DataConnect.Features.DataExtraction.Models;

namespace DataConnect.Features.DataExtraction.Services;

public interface IDataExtractionService
{
    Task<ExtractResponse> ExtractPaginatedDataAsync(ExtractRequest request,
        string filteredDate,
        string[] nameSchema,
        CancellationToken cancellationToken = default);

    Task<ExtractResponse> ExtractSimpleDataAsync(ExtractRequest request,
        string filteredDate,
        string[] nameSchema,
        CancellationToken cancellationToken = default);

    Task<ExtractResponse> ExtractBasicDataAsync(ExtractRequest request,
        CancellationToken cancellationToken = default);
}