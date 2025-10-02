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
    private readonly ILoggerFactory _loggerFactory;
    private readonly string _innerJsonName = "itens";

    public DataExtractionService(
        IJsonDataTableConverter jsonConverter,
        IHttpClientFactory httpClientFactory,
        IConfiguration configuration,
        ILogger<DataExtractionService> logger,
        ILoggerFactory loggerFactory)
    {
        _jsonConverter = jsonConverter;
        _httpClientFactory = httpClientFactory;
        _configuration = configuration;
        _logger = logger;
        _loggerFactory = loggerFactory;
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
            _logger.LogInformation("=== Starting paginated extraction for table: {TableName} ===", request.DestinationTableName);
            _logger.LogInformation("Extraction parameters - FilteredDate: {FilteredDate}, LookBackDays: {LookBackDays}",
                filteredDate, request.Options[4]);

            var httpSender = new HttpSender(_httpClientFactory);
            var conStr = _configuration.GetConnectionString("DefaultConnection")!;
            var database = _configuration["Database:Name"];
            var fieldCharLimit = _configuration.GetValue<int>("Database:FieldCharLimit", 500);

            _logger.LogInformation("Phase 1/5: Fetching first page to determine total count");
            // Get first page to determine total count
            var firstPageResult = await httpSender.SimpleAuthBodyRequestAsync(
                BuildPayload(request.Options, request.DestinationTableName, filteredDate, nameSchema, 1),
                HttpMethod.Post,
                request.ConnectionInfo);

            if (!firstPageResult.IsOk)
            {
                _logger.LogError("Failed to fetch first page: {Error}", firstPageResult.Error.ExceptionMessage);
                response.Success = false;
                response.Message = $"Failed to fetch first page: {firstPageResult.Error.ExceptionMessage}";
                return response;
            }

            var pageCount = firstPageResult.Value["totalCount"]?.GetValue<int>() ?? 0;
            _logger.LogInformation("Total pages to process: {PageCount}", pageCount);

            // Phase 2: Sample multiple pages to discover complete schema
            _logger.LogInformation("Phase 2/5: Sampling pages to discover full schema");
            var sampleSize = Math.Min(200, pageCount); // Sample up to 200 pages for better coverage
            var masterSchema = await SamplePagesForSchemaAsync(
                httpSender, request, filteredDate, nameSchema, pageCount, sampleSize, fieldCharLimit, cancellationToken);

            if (masterSchema == null)
            {
                response.Success = false;
                response.Message = "Failed to sample pages for schema discovery";
                return response;
            }

            _logger.LogInformation("Schema discovery complete: {ColumnCount} columns found across {SampleSize} sampled pages",
                masterSchema.Columns.Count, sampleSize);

            _logger.LogInformation("Phase 3/5: Creating database table {SysName}.{TableName} in database {Database}",
                request.SysName, request.DestinationTableName, database);
            // Create table in database
            await using var sqlServer = await SqlServerCall.CreateAsync(conStr, _loggerFactory.CreateLogger<SqlServerCall>());
            var createResult = await sqlServer.CreateTable(masterSchema, request.DestinationTableName, request.SysName, database);

            if (!createResult.IsOk)
            {
                _logger.LogError("Failed to create database table: {Error}", createResult.Error.ExceptionMessage);
                response.Success = false;
                response.Message = $"Failed to create database table: {createResult.Error.ExceptionMessage}";
                return response;
            }

            _logger.LogInformation("Phase 4/5: Processing {PageCount} pages with {ThreadCount} concurrent threads",
                pageCount, _configuration.GetValue<int>("Processing:ThreadPagination", 4));

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
                    httpSender, conStr, masterSchema, fieldCharLimit, cancellationToken);

                tasks.Add(task);
            }

            // Add progress reporting
            var progressReporter = Task.Run(async () =>
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    await Task.Delay(10000, cancellationToken); // Report every 10 seconds
                    var completed = tasks.Count(t => t.IsCompleted);
                    var succeeded = tasks.Where(t => t.IsCompleted && !t.IsFaulted).Count(t => t.Result.Success);
                    _logger.LogInformation("Progress: {Completed}/{Total} pages processed, {Succeeded} succeeded",
                        completed, tasks.Count, succeeded);
                }
            }, cancellationToken);

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

            var duration = response.EndTime.Value - response.StartTime;
            _logger.LogInformation("=== Extraction completed for {TableName} ===", request.DestinationTableName);
            _logger.LogInformation("Results - Pages: {Pages}/{Total}, Records: {Records}, Errors: {Errors}, Duration: {Duration:mm\\:ss}",
                processedPages, pageCount, recordsProcessed, errorCount, duration);

            if (errorCount > 0)
            {
                _logger.LogWarning("{ErrorCount} pages failed during extraction", errorCount);
            }

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
        string connectionString,
        DataTable tableSchema,
        int fieldCharLimit,
        CancellationToken cancellationToken)
    {
        await semaphore.WaitAsync(cancellationToken);
        var startTime = DateTime.UtcNow;

        try
        {
            _logger.LogDebug("Processing page {PageNumber}", pageNumber);

            var payload = BuildPayload(request.Options, request.DestinationTableName, filteredDate, nameSchema, pageNumber);
            var result = await httpSender.SimpleAuthBodyRequestAsync(payload, HttpMethod.Post, request.ConnectionInfo);

            if (!result.IsOk)
            {
                _logger.LogWarning("Page {PageNumber} - Fetch failed: {Error}", pageNumber, result.Error.ExceptionMessage);
                return new PageProcessingResult { Success = false, PageNumber = pageNumber };
            }

            var pageData = await _jsonConverter.ConvertToDataTableAsync(result.Value[_innerJsonName]!, fieldCharLimit);

            if (!pageData.IsOk)
            {
                _logger.LogWarning("Page {PageNumber} - Conversion failed: {Error}", pageNumber, pageData.Error.ExceptionMessage);
                return new PageProcessingResult { Success = false, PageNumber = pageNumber };
            }

            var recordCount = pageData.Value.Rows.Count;
            var originalColumns = pageData.Value.Columns.Count;

            // Pad DataTable to match master schema (add missing columns with NULL)
            PadDataTableToSchema(pageData.Value, tableSchema);

            if (originalColumns != tableSchema.Columns.Count)
            {
                _logger.LogDebug("Page {PageNumber}: Padded from {OriginalColumns} to {MasterColumns} columns",
                    pageNumber, originalColumns, tableSchema.Columns.Count);
            }

            // Create a new SQL connection for this page to avoid concurrency issues
            await using var sqlServer = await SqlServerCall.CreateAsync(connectionString, _loggerFactory.CreateLogger<SqlServerCall>());
            var insertResult = await sqlServer.BulkInsert(pageData.Value, request.DestinationTableName, request.SysName);

            if (!insertResult.IsOk)
            {
                _logger.LogWarning("Page {PageNumber} - Insert failed: {Error}", pageNumber, insertResult.Error.ExceptionMessage);
                return new PageProcessingResult { Success = false, PageNumber = pageNumber };
            }

            var duration = (DateTime.UtcNow - startTime).TotalMilliseconds;

            // Only log slow pages or every 100th page
            if (duration > 5000 || pageNumber % 100 == 0)
            {
                _logger.LogInformation("Page {PageNumber} completed - {Records} records in {Duration:F0}ms",
                    pageNumber, recordCount, duration);
            }

            return new PageProcessingResult
            {
                Success = true,
                PageNumber = pageNumber,
                RecordsCount = recordCount
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
            _logger.LogInformation("=== Starting simple extraction for table: {TableName} ===", request.DestinationTableName);
            _logger.LogInformation("FilteredDate: {FilteredDate}", filteredDate);

            var httpSender = new HttpSender(_httpClientFactory);
            var conStr = _configuration.GetConnectionString("DefaultConnection")!;
            var database = _configuration["Database:Name"];
            var fieldCharLimit = _configuration.GetValue<int>("Database:FieldCharLimit", 500);

            _logger.LogInformation("Fetching data from external API");
            var payload = BuildPayload(request.Options, request.DestinationTableName, filteredDate, nameSchema);
            var result = await httpSender.SimpleAuthBodyRequestAsync(payload, HttpMethod.Post, request.ConnectionInfo);

            if (!result.IsOk)
            {
                _logger.LogError("Failed to fetch data: {Error}", result.Error.ExceptionMessage);
                response.Success = false;
                response.Message = $"Failed to fetch data: {result.Error.ExceptionMessage}";
                return response;
            }

            _logger.LogInformation("Converting JSON to DataTable");
            var tableResult = await _jsonConverter.ConvertToDataTableAsync(result.Value[_innerJsonName]!, fieldCharLimit);

            if (!tableResult.IsOk)
            {
                _logger.LogError("Failed to convert JSON: {Error}", tableResult.Error.ExceptionMessage);
                response.Success = false;
                response.Message = $"Failed to convert JSON: {tableResult.Error.ExceptionMessage}";
                return response;
            }

            _logger.LogInformation("Creating database table {SysName}.{TableName}", request.SysName, request.DestinationTableName);
            await using var sqlServer = await SqlServerCall.CreateAsync(conStr, _loggerFactory.CreateLogger<SqlServerCall>());
            var createResult = await sqlServer.CreateTable(tableResult.Value, request.DestinationTableName, request.SysName, database);

            if (!createResult.IsOk)
            {
                _logger.LogError("Failed to create table: {Error}", createResult.Error.ExceptionMessage);
                response.Success = false;
                response.Message = $"Failed to create table: {createResult.Error.ExceptionMessage}";
                return response;
            }

            _logger.LogInformation("Inserting {RecordCount} records", tableResult.Value.Rows.Count);
            var insertResult = await sqlServer.BulkInsert(tableResult.Value, request.DestinationTableName, request.SysName, database);

            if (!insertResult.IsOk)
            {
                _logger.LogError("Failed to insert data: {Error}", insertResult.Error.ExceptionMessage);
                response.Success = false;
                response.Message = $"Failed to insert data: {insertResult.Error.ExceptionMessage}";
                return response;
            }

            response.EndTime = DateTime.UtcNow;
            response.Success = true;
            response.RecordsProcessed = tableResult.Value.Rows.Count;
            response.Message = $"Successfully processed {response.RecordsProcessed} records";

            var duration = response.EndTime.Value - response.StartTime;
            _logger.LogInformation("=== Extraction completed for {TableName} ===", request.DestinationTableName);
            _logger.LogInformation("Results - Records: {Records}, Duration: {Duration:mm\\:ss}",
                response.RecordsProcessed, duration);

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

    private async Task<DataTable?> SamplePagesForSchemaAsync(
        HttpSender httpSender,
        ExtractRequest request,
        string filteredDate,
        string[] nameSchema,
        int totalPages,
        int sampleSize,
        int fieldCharLimit,
        CancellationToken cancellationToken)
    {
        var schemaOptions = new SchemaInferenceOptions
        {
            FieldCharLimit = fieldCharLimit,
            InferNumericTypes = false,  // All strings for safety
            InferDateTypes = false,
            InferBooleanTypes = false,
            SampleSize = 10000,
            TypeConfidenceThreshold = 1.0
        };

        // Calculate which pages to sample (evenly distributed)
        var pagesToSample = new List<int>();
        if (sampleSize >= totalPages)
        {
            // Sample all pages
            pagesToSample.AddRange(Enumerable.Range(1, totalPages));
        }
        else
        {
            // Sample evenly across the range
            var step = (double)totalPages / sampleSize;
            for (int i = 0; i < sampleSize; i++)
            {
                pagesToSample.Add((int)(i * step) + 1);
            }
        }

        _logger.LogInformation("Sampling pages: {Pages}", string.Join(", ", pagesToSample.Take(10)) + (pagesToSample.Count > 10 ? "..." : ""));

        DataTable? masterSchema = null;
        var semaphore = new SemaphoreSlim(10); // Limit concurrent sampling
        var tasks = new List<Task<DataTable?>>();

        foreach (var pageNumber in pagesToSample)
        {
            if (cancellationToken.IsCancellationRequested) break;

            var task = SamplePageSchemaAsync(semaphore, pageNumber, httpSender, request, filteredDate, nameSchema, schemaOptions, cancellationToken);
            tasks.Add(task);
        }

        var schemas = await Task.WhenAll(tasks);
        var validSchemas = schemas.Where(s => s != null).ToList();

        if (validSchemas.Count == 0)
        {
            _logger.LogError("No valid schemas could be sampled");
            return null;
        }

        // Merge all schemas
        masterSchema = new DataTable();
        foreach (var schema in validSchemas)
        {
            masterSchema.Merge(schema!, false, MissingSchemaAction.Add);
        }

        return masterSchema;
    }

    private async Task<DataTable?> SamplePageSchemaAsync(
        SemaphoreSlim semaphore,
        int pageNumber,
        HttpSender httpSender,
        ExtractRequest request,
        string filteredDate,
        string[] nameSchema,
        SchemaInferenceOptions options,
        CancellationToken cancellationToken)
    {
        await semaphore.WaitAsync(cancellationToken);

        try
        {
            var payload = BuildPayload(request.Options, request.DestinationTableName, filteredDate, nameSchema, pageNumber);
            var result = await httpSender.SimpleAuthBodyRequestAsync(payload, HttpMethod.Post, request.ConnectionInfo);

            if (!result.IsOk)
            {
                _logger.LogWarning("Failed to sample page {PageNumber}: {Error}", pageNumber, result.Error.ExceptionMessage);
                return null;
            }

            var tableResult = await _jsonConverter.ConvertToDataTableAsync(result.Value[_innerJsonName]!, options);

            if (!tableResult.IsOk)
            {
                _logger.LogWarning("Failed to convert sampled page {PageNumber}: {Error}", pageNumber, tableResult.Error.ExceptionMessage);
                return null;
            }

            return tableResult.Value;
        }
        finally
        {
            semaphore.Release();
        }
    }

    private static void PadDataTableToSchema(DataTable dataTable, DataTable masterSchema)
    {
        // First, remove any extra columns not in master schema
        var columnsToRemove = new List<DataColumn>();
        foreach (DataColumn col in dataTable.Columns)
        {
            if (!masterSchema.Columns.Contains(col.ColumnName))
            {
                columnsToRemove.Add(col);
            }
        }
        foreach (var col in columnsToRemove)
        {
            dataTable.Columns.Remove(col);
        }

        // Add any missing columns from master schema
        foreach (DataColumn masterColumn in masterSchema.Columns)
        {
            if (!dataTable.Columns.Contains(masterColumn.ColumnName))
            {
                var newColumn = new DataColumn(masterColumn.ColumnName, masterColumn.DataType)
                {
                    AllowDBNull = true,
                    DefaultValue = DBNull.Value
                };
                dataTable.Columns.Add(newColumn);

                // Fill existing rows with DBNull for new column
                foreach (DataRow row in dataTable.Rows)
                {
                    row[newColumn] = DBNull.Value;
                }
            }
        }

        // Reorder columns to match master schema exactly
        for (int i = 0; i < masterSchema.Columns.Count; i++)
        {
            var masterColumnName = masterSchema.Columns[i].ColumnName;
            if (dataTable.Columns.Contains(masterColumnName))
            {
                dataTable.Columns[masterColumnName]!.SetOrdinal(i);
            }
        }
    }

    private class PageProcessingResult
    {
        public bool Success { get; set; }
        public int PageNumber { get; set; }
        public int RecordsCount { get; set; }
    }
}