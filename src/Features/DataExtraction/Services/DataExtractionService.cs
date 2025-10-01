using System.Data;
using System.Text.Json.Nodes;
using DataConnect.Features.DataExtraction.Models;
using DataConnect.Infrastructure.Json;
using DataConnect.Infrastructure.Http;
using DataConnect.Infrastructure.Database;
using DataConnect.Core.Models;
using DataConnect.Core.Common;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;

namespace DataConnect.Features.DataExtraction.Services;

public class DataExtractionService : IDataExtractionService
{
    private readonly IJsonDataTableConverter _jsonConverter;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly IConfiguration _configuration;
    private readonly ILogger<DataExtractionService> _logger;
    private readonly string _innerJsonName = "itens";

    public DataExtractionService(
        IJsonDataTableConverter jsonConverter,
        IHttpClientFactory httpClientFactory,
        IConfiguration configuration,
        ILogger<DataExtractionService> logger)
    {
        _jsonConverter = jsonConverter;
        _httpClientFactory = httpClientFactory;
        _configuration = configuration;
        _logger = logger;
    }

    public async Task<ExtractResponse> ExtractPaginatedDataAsync(ExtractRequest request,
        string filteredDate,
        string[] nameSchema,
        CancellationToken cancellationToken = default)
    {
        var response = new ExtractResponse
        {
            StartTime = DateTime.UtcNow,
            JobId = Guid.NewGuid().ToString()
        };

        try
        {
            _logger.LogInformation("Starting paginated extraction for table {TableName}", request.DestinationTableName);

            var httpSender = new HttpSender(_httpClientFactory);
            var conStr = _configuration.GetConnectionString("DefaultConnection")!;
            var database = _configuration["Database:Name"];
            var fieldCharLimit = _configuration.GetValue<int>("Database:FieldCharLimit", 500);

            // Get first page to determine total count and create table schema
            var firstPageResult = await httpSender.SimpleAuthBodyRequestAsync(
                BuildPayload(request.Options, request.DestinationTableName, filteredDate, nameSchema, 1),
                HttpMethod.Post,
                request.ConnectionInfo);

            if (!firstPageResult.IsOk)
            {
                response.Success = false;
                response.Message = $"Failed to fetch first page: {firstPageResult.Error.ExceptionMessage}";
                return response;
            }

            var innerJson = firstPageResult.Value[_innerJsonName]!;
            var tableResult = await _jsonConverter.ConvertToDataTableAsync(innerJson, fieldCharLimit);

            if (!tableResult.IsOk)
            {
                response.Success = false;
                response.Message = $"Failed to convert JSON to DataTable: {tableResult.Error.ExceptionMessage}";
                return response;
            }

            var table = tableResult.Value;
            table.Clear(); // Clear data, keep schema

            // Create table in database
            using var sqlServer = new SqlServerCall(conStr);
            var createResult = await sqlServer.CreateTable(table, request.DestinationTableName, request.SysName, database);

            if (!createResult.IsOk)
            {
                response.Success = false;
                response.Message = $"Failed to create database table: {createResult.Error.ExceptionMessage}";
                return response;
            }

            var pageCount = firstPageResult.Value["totalCount"]?.GetValue<int>() ?? 0;

            _logger.LogInformation("Processing {PageCount} pages for table {TableName}", pageCount, request.DestinationTableName);

            // Process all pages
            var threadPagination = _configuration.GetValue<int>("Processing:ThreadPagination", 4);
            var threadTimeout = _configuration.GetValue<int>("Processing:ThreadTimeout", 1000);

            var processedPages = 0;
            var errorCount = 0;
            var recordsProcessed = 0;

            var semaphore = new SemaphoreSlim(threadPagination);
            var tasks = new List<Task<PageProcessingResult>>();

            for (int page = 1; page <= pageCount; page++)
            {
                if (cancellationToken.IsCancellationRequested)
                    break;

                var pageNumber = page;
                var task = ProcessPageAsync(semaphore, pageNumber, request, filteredDate, nameSchema,
                    httpSender, sqlServer, table, fieldCharLimit, cancellationToken);

                tasks.Add(task);
            }

            var results = await Task.WhenAll(tasks);

            processedPages = results.Count(r => r.Success);
            errorCount = results.Count(r => !r.Success);
            recordsProcessed = results.Where(r => r.Success).Sum(r => r.RecordsCount);

            response.EndTime = DateTime.UtcNow;
            response.Success = errorCount == 0;
            response.PagesProcessed = processedPages;
            response.ErrorCount = errorCount;
            response.RecordsProcessed = recordsProcessed;
            response.Message = errorCount == 0
                ? $"Successfully processed {processedPages} pages with {recordsProcessed} records"
                : $"Processed {processedPages} pages with {errorCount} errors";

            _logger.LogInformation("Completed paginated extraction for {TableName}. Pages: {ProcessedPages}, Errors: {ErrorCount}",
                request.DestinationTableName, processedPages, errorCount);

            return response;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Critical error during paginated extraction for {TableName}", request.DestinationTableName);
            response.Success = false;
            response.Message = $"Critical error: {ex.Message}";
            response.EndTime = DateTime.UtcNow;
            return response;
        }
    }

    private async Task<PageProcessingResult> ProcessPageAsync(
        SemaphoreSlim semaphore,
        int pageNumber,
        ExtractRequest request,
        string filteredDate,
        string[] nameSchema,
        HttpSender httpSender,
        SqlServerCall sqlServer,
        DataTable tableSchema,
        int fieldCharLimit,
        CancellationToken cancellationToken)
    {
        await semaphore.WaitAsync(cancellationToken);

        try
        {
            var payload = BuildPayload(request.Options, request.DestinationTableName, filteredDate, nameSchema, pageNumber);
            var result = await httpSender.SimpleAuthBodyRequestAsync(payload, HttpMethod.Post, request.ConnectionInfo);

            if (!result.IsOk)
            {
                _logger.LogWarning("Failed to fetch page {PageNumber}: {Error}", pageNumber, result.Error.ExceptionMessage);
                return new PageProcessingResult { Success = false, PageNumber = pageNumber };
            }

            var pageData = await _jsonConverter.ConvertToDataTableAsync(result.Value[_innerJsonName]!, fieldCharLimit);

            if (!pageData.IsOk)
            {
                _logger.LogWarning("Failed to convert page {PageNumber} data: {Error}", pageNumber, pageData.Error.ExceptionMessage);
                return new PageProcessingResult { Success = false, PageNumber = pageNumber };
            }

            var insertResult = await sqlServer.BulkInsert(pageData.Value, request.DestinationTableName, request.SysName);

            if (!insertResult.IsOk)
            {
                _logger.LogWarning("Failed to insert page {PageNumber} data: {Error}", pageNumber, insertResult.Error.ExceptionMessage);
                return new PageProcessingResult { Success = false, PageNumber = pageNumber };
            }

            return new PageProcessingResult
            {
                Success = true,
                PageNumber = pageNumber,
                RecordsCount = pageData.Value.Rows.Count
            };
        }
        finally
        {
            semaphore.Release();
        }
    }

    public async Task<ExtractResponse> ExtractSimpleDataAsync(ExtractRequest request,
        string filteredDate,
        string[] nameSchema,
        CancellationToken cancellationToken = default)
    {
        var response = new ExtractResponse
        {
            StartTime = DateTime.UtcNow,
            JobId = Guid.NewGuid().ToString()
        };

        try
        {
            _logger.LogInformation("Starting simple extraction for table {TableName}", request.DestinationTableName);

            var httpSender = new HttpSender(_httpClientFactory);
            var conStr = _configuration.GetConnectionString("DefaultConnection")!;
            var database = _configuration["Database:Name"];
            var fieldCharLimit = _configuration.GetValue<int>("Database:FieldCharLimit", 500);

            var payload = BuildPayload(request.Options, request.DestinationTableName, filteredDate, nameSchema);
            var result = await httpSender.SimpleAuthBodyRequestAsync(payload, HttpMethod.Post, request.ConnectionInfo);

            if (!result.IsOk)
            {
                response.Success = false;
                response.Message = $"Failed to fetch data: {result.Error.ExceptionMessage}";
                return response;
            }

            var tableResult = await _jsonConverter.ConvertToDataTableAsync(result.Value[_innerJsonName]!, fieldCharLimit);

            if (!tableResult.IsOk)
            {
                response.Success = false;
                response.Message = $"Failed to convert JSON: {tableResult.Error.ExceptionMessage}";
                return response;
            }

            using var sqlServer = new SqlServerCall(conStr);
            var createResult = await sqlServer.CreateTable(tableResult.Value, request.DestinationTableName, request.SysName, database);

            if (!createResult.IsOk)
            {
                response.Success = false;
                response.Message = $"Failed to create table: {createResult.Error.ExceptionMessage}";
                return response;
            }

            var insertResult = await sqlServer.BulkInsert(tableResult.Value, request.DestinationTableName, request.SysName, database);

            if (!insertResult.IsOk)
            {
                response.Success = false;
                response.Message = $"Failed to insert data: {insertResult.Error.ExceptionMessage}";
                return response;
            }

            response.EndTime = DateTime.UtcNow;
            response.Success = true;
            response.RecordsProcessed = tableResult.Value.Rows.Count;
            response.Message = $"Successfully processed {response.RecordsProcessed} records";

            _logger.LogInformation("Completed simple extraction for {TableName} with {RecordsCount} records",
                request.DestinationTableName, response.RecordsProcessed);

            return response;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during simple extraction for {TableName}", request.DestinationTableName);
            response.Success = false;
            response.Message = $"Error: {ex.Message}";
            response.EndTime = DateTime.UtcNow;
            return response;
        }
    }

    public async Task<ExtractResponse> ExtractBasicDataAsync(ExtractRequest request, CancellationToken cancellationToken = default)
    {
        return await ExtractSimpleDataAsync(request, "01/01/1900", ["a1", "a2"], cancellationToken);
    }

    private static List<KeyValuePair<string, string>> BuildPayload(string[] options,
        string destinationTableName,
        string filteredDate,
        string[] nameSchema,
        int? page = null)
    {
        var payload = new List<KeyValuePair<string, string>>
        {
            KeyValuePair.Create(options[0], options[1]),
            KeyValuePair.Create(options[2], Encryption.Sha256($"{options[3]}{DateTime.Today:dd/MM/yyyy}")),
            KeyValuePair.Create("pag", destinationTableName),
            KeyValuePair.Create("cmd", "get"),
            KeyValuePair.Create(nameSchema[0], filteredDate),
            KeyValuePair.Create(nameSchema[1], $"{DateTime.Today:dd/MM/yyyy}"),
            KeyValuePair.Create("start", "1")
        };

        if (page.HasValue)
        {
            payload.Add(KeyValuePair.Create("page", page.Value.ToString()));
        }

        return payload;
    }

    private class PageProcessingResult
    {
        public bool Success { get; set; }
        public int PageNumber { get; set; }
        public int RecordsCount { get; set; }
    }
}