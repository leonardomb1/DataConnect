using System.Data;
using System.Text.Json.Nodes;
using System.Globalization;
using DataConnect.Core.Models;
using DataConnect.Core.Common;

namespace DataConnect.Infrastructure.Json;

public class JsonDataTableConverter : IJsonDataTableConverter
{
    public async Task<Result<DataTable, Error>> ConvertToDataTableAsync(JsonNode jsonData, int fieldCharLimit = 500)
    {
        var options = new SchemaInferenceOptions { FieldCharLimit = fieldCharLimit };
        return await ConvertToDataTableAsync(jsonData, options);
    }

    public async Task<Result<DataTable, Error>> ConvertToDataTableAsync(JsonNode jsonData, SchemaInferenceOptions options)
    {
        return await Task.Run(() => ConvertToDataTable(jsonData, options));
    }

    private Result<DataTable, Error> ConvertToDataTable(JsonNode jsonData, SchemaInferenceOptions options)
    {
        try
        {
            if (jsonData is not JsonArray jsonArray)
            {
                return new Error { ExceptionMessage = "JSON data must be an array for DataTable conversion" };
            }

            if (jsonArray.Count == 0)
            {
                return new Error { ExceptionMessage = "JSON array is empty" };
            }

            // Sample data for type inference
            var sampleSize = Math.Min(options.SampleSize, jsonArray.Count);
            var sampleObjects = jsonArray.Take(sampleSize).OfType<JsonObject>().ToList();

            if (sampleObjects.Count == 0)
            {
                return new Error { ExceptionMessage = "No valid JSON objects found in array" };
            }

            // Discover all possible columns
            var allColumns = new HashSet<string>();
            foreach (var obj in sampleObjects)
            {
                foreach (var property in obj)
                {
                    allColumns.Add(property.Key);
                }
            }

            // Infer column types
            var columnTypes = InferColumnTypes(allColumns, sampleObjects, options);

            // Create DataTable with inferred schema
            var dataTable = new DataTable();
            foreach (var column in allColumns)
            {
                var columnType = columnTypes[column];
                dataTable.Columns.Add(column, columnType);
            }

            // Populate data
            foreach (var jsonObject in jsonArray.OfType<JsonObject>())
            {
                var row = dataTable.NewRow();

                foreach (var column in dataTable.Columns.Cast<DataColumn>())
                {
                    if (jsonObject.TryGetPropertyValue(column.ColumnName, out var value))
                    {
                        row[column.ColumnName] = ConvertValue(value, column.DataType, options.FieldCharLimit);
                    }
                    else
                    {
                        row[column.ColumnName] = DBNull.Value;
                    }
                }

                dataTable.Rows.Add(row);
            }

            return dataTable;
        }
        catch (Exception ex)
        {
            return new Error { ExceptionMessage = $"Error converting JSON to DataTable: {ex.Message}" };
        }
    }

    private Dictionary<string, Type> InferColumnTypes(
        HashSet<string> columns,
        List<JsonObject> sampleObjects,
        SchemaInferenceOptions options)
    {
            var columnTypes = new Dictionary<string, Type>();

            foreach (var column in columns)
            {
                var values = sampleObjects
                    .Where(obj => obj.TryGetPropertyValue(column, out _))
                    .Select(obj => obj[column])
                    .Where(v => v != null)
                    .ToList();

                if (values.Count == 0)
                {
                    columnTypes[column] = typeof(string);
                    continue;
                }

                columnTypes[column] = InferBestType(values, options);
            }

        return columnTypes;
    }

    private Type InferBestType(List<JsonNode?> values, SchemaInferenceOptions options)
    {
        if (values.Count == 0) return typeof(string);

        var stringValues = values.Select(v => v?.ToString() ?? "").Where(v => !string.IsNullOrEmpty(v)).ToList();
        if (stringValues.Count == 0) return typeof(string);

        var totalCount = stringValues.Count;
        var confidenceThreshold = (int)(totalCount * options.TypeConfidenceThreshold);

        // Try numeric types (order matters: try larger types first to avoid overflow)
        if (options.InferNumericTypes)
        {
            // Try long first (covers both int and long)
            var longCount = stringValues.Count(v => long.TryParse(v, out _));
            if (longCount >= confidenceThreshold)
            {
                // Check if all values fit in int
                var allFitInInt = stringValues.All(v =>
                {
                    if (long.TryParse(v, out var longVal))
                        return longVal >= int.MinValue && longVal <= int.MaxValue;
                    return false;
                });

                return allFitInInt ? typeof(int) : typeof(long);
            }

            // Try decimal
            var decimalCount = stringValues.Count(v => decimal.TryParse(v, NumberStyles.Number, CultureInfo.InvariantCulture, out _));
            if (decimalCount >= confidenceThreshold)
                return typeof(decimal);
        }

        // Try boolean (must be exact match to avoid false positives)
        if (options.InferBooleanTypes)
        {
            var boolCount = stringValues.Count(v => bool.TryParse(v, out _));
            if (boolCount == totalCount) // 100% match required for boolean
                return typeof(bool);
        }

        // Skip DateTime inference - too many false positives with numeric strings
        // Always default to string for safety with mixed data

        return typeof(string);
    }

    private object ConvertValue(JsonNode? value, Type targetType, int fieldCharLimit)
    {
        if (value == null)
            return DBNull.Value;

        var stringValue = value.ToString();

        try
        {
            if (targetType == typeof(int))
            {
                if (int.TryParse(stringValue, out var intVal))
                    return intVal;
                return DBNull.Value; // Return NULL for non-parseable values
            }

            if (targetType == typeof(long))
            {
                if (long.TryParse(stringValue, out var longVal))
                    return longVal;
                return DBNull.Value;
            }

            if (targetType == typeof(decimal))
            {
                if (decimal.TryParse(stringValue, NumberStyles.Number, CultureInfo.InvariantCulture, out var decVal))
                    return decVal;
                return DBNull.Value;
            }

            if (targetType == typeof(bool))
            {
                if (bool.TryParse(stringValue, out var boolVal))
                    return boolVal;
                return DBNull.Value;
            }

            if (targetType == typeof(DateTime))
            {
                if (DateTime.TryParse(stringValue, out var dateVal))
                    return dateVal;
                return DBNull.Value;
            }

            // String type
            if (stringValue.Length > fieldCharLimit)
                stringValue = stringValue[..fieldCharLimit];

            return stringValue;
        }
        catch
        {
            // For typed columns, return NULL on error; for string columns, return truncated value
            if (targetType == typeof(string))
            {
                if (stringValue.Length > fieldCharLimit)
                    stringValue = stringValue[..fieldCharLimit];
                return stringValue;
            }

            return DBNull.Value;
        }
    }
}