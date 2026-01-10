using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Trino.Core.Utils;

/// <summary>
/// JSON converter that handles NaN strings for double values, as returned by Trino in some stats fields.
/// </summary>
public class NaNHandlingDoubleConverter : JsonConverter<double>
{
    public override double Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.String)
        {
            var stringValue = reader.GetString();
            if (string.Equals(stringValue, "NaN", StringComparison.OrdinalIgnoreCase))
            {
                return double.NaN;
            }

            return double.TryParse(stringValue, out var result)
                ? result
                : throw new JsonException($"Unable to convert \"{stringValue}\" to double.");
        }

        return reader.GetDouble();
    }

    public override void Write(Utf8JsonWriter writer, double value, JsonSerializerOptions options)
    {
        if (double.IsNaN(value))
        {
            writer.WriteStringValue("NaN");
        }
        else
        {
            writer.WriteNumberValue(value);
        }
    }
}

/// <summary>
/// JSON converter for object type that properly converts JsonElement values to their primitive types.
/// </summary>
public class ObjectToInferredTypeConverter : JsonConverter<object>
{
    public override object Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        switch (reader.TokenType)
        {
            case JsonTokenType.True:
                return true;
            case JsonTokenType.False:
                return false;
            case JsonTokenType.Number:
                if (reader.TryGetInt64(out var longValue))
                {
                    return longValue;
                }

                return reader.GetDouble();
            case JsonTokenType.String:
                return reader.GetString();
            case JsonTokenType.Null:
                return null;
            case JsonTokenType.StartArray:
                using (var doc = JsonDocument.ParseValue(ref reader))
                {
                    return doc.RootElement.Clone();
                }
            case JsonTokenType.StartObject:
                using (var doc = JsonDocument.ParseValue(ref reader))
                {
                    return doc.RootElement.Clone();
                }
            default:
                throw new JsonException($"Unexpected token type: {reader.TokenType}");
        }
    }

    public override void Write(Utf8JsonWriter writer, object value, JsonSerializerOptions options)
    {
        JsonSerializer.Serialize(writer, value, value?.GetType() ?? typeof(object), options);
    }
}

internal static class JsonSerializerConfig
{
    private static JsonSerializerOptions _options;

    public static JsonSerializerOptions Options
    {
        get
        {
            if (_options == null)
            {
                _options = new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                };
                _options.Converters.Add(new NaNHandlingDoubleConverter());
                _options.Converters.Add(new ObjectToInferredTypeConverter());
            }

            return _options;
        }
    }
}