using System.Data;
using System.Text.Json.Nodes;
using DataConnect.Core.Models;
using DataConnect.Core.Common;

namespace DataConnect.Infrastructure.Json;

public interface IJsonDataTableConverter
{
    Task<Result<DataTable, Error>> ConvertToDataTableAsync(JsonNode jsonData, int fieldCharLimit = 500);
    Task<Result<DataTable, Error>> ConvertToDataTableAsync(JsonNode jsonData, SchemaInferenceOptions options);
}

public class SchemaInferenceOptions
{
    public int FieldCharLimit { get; init; } = 500;
    public bool InferNumericTypes { get; init; } = true;
    public bool InferDateTypes { get; init; } = false; // Disabled - too many false positives
    public bool InferBooleanTypes { get; init; } = true;
    public int SampleSize { get; init; } = 10000; // Larger sample for better accuracy
    public double TypeConfidenceThreshold { get; init; } = 0.95; // 95% of samples must match type
}