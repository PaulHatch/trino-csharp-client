using System;
using System.Text.Json;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Trino.Core.Model.StatementV1;
using Trino.Core.Types;

namespace Trino.Core.Utils;

/// <summary>
/// Contains all type converters for Trino types.
/// </summary>
public class TrinoTypeConverters
{
    public const string TRINO_BIGINT = "bigint";
    public const string TRINO_INTEGER = "integer";
    public const string TRINO_SMALLINT = "smallint";
    public const string TRINO_TINYINT = "tinyint";
    public const string TRINO_BOOLEAN = "boolean";
    public const string TRINO_DOUBLE = "double";
    public const string TRINO_REAL = "real";
    public const string TRINO_DECIMAL = "decimal";
    public const string TRINO_TIMESTAMP = "timestamp";
    public const string TRINO_DATE = "date";
    public const string TRINO_TIME = "time";
    public const string TRINO_WITH_TIME_ZONE_SUFFIX = "with time zone";
    public const string TRINO_TIME_WITH_TIME_ZONE = "time " + TRINO_WITH_TIME_ZONE_SUFFIX;
    public const string TRINO_TIMESTAMP_WITH_TIME_ZONE = "timestamp " + TRINO_WITH_TIME_ZONE_SUFFIX;
    public const string TRINO_VARCHAR = "varchar";
    public const string TRINO_CHAR = "char";
    public const string TRINO_UUID = "uuid";
    public const string TRINO_VARBINARY = "varbinary";
    public const string TRINO_ARRAY = "array";
    public const string TRINO_MAP = "map";
    public const string TRINO_INTERVAL_YEAR_TO_MONTH = "interval year to month";
    public const string TRINO_INTERVAL_DAY_TO_SECOND = "interval day to second";
    public const string TRINO_ROW = "row";
    public const string TRINO_JSON = "json";
    public const string TRINO_IP = "ipaddress";

    private static readonly string _trinoDateFormat = "yyyy-MM-dd";
        
    // DateTime formatter is not used: "yyyy-MM-dd HH\\:mm\\:ss.ffffff";
    // Instead a regex is used to parse the timestamp to allow extraction of the timezone.
    // "K" is the offset only.
    private static readonly Regex _trinoTimestampWithTimezoneFormat = new Regex(@"([\d]{4})-([\d]{2})-([\d]{2}) ([\d]{2})\:([\d]{2})\:([\d]{2})(\.([\d]+))? ([+-]\d\d:\d\d|UTC)");
    private static readonly Regex _trinoOffset = new Regex(@"([+-]\d\d):(\d\d)");

    private static readonly string _trinoTimeFormat = "hh\\:mm\\:ss\\.fff";

    internal static object ConvertToTrinoTypeFromJson(object value, string trinoType, string validType = null)
    {
        if (value == null)
        {
            return null;
        }

        GetNestedTypes(trinoType, out var baseType, out var typeParameters);

        if (!string.IsNullOrEmpty(validType) && validType != baseType)
        {
            throw new ArgumentException($"Column is type {trinoType} but a value of type {validType} was expected");
        }

        return ConvertToTrinoTypeFromJson(value, trinoType, baseType, typeParameters);
    }

    internal static object ConvertToTrinoTypeFromJson(object value, string trinoType, string[] validTypes)
    {
        if (value == null)
        {
            return null;
        }

        GetNestedTypes(trinoType, out var baseType, out var typeParameters);

        if (!validTypes.Contains(baseType))
        {
            throw new ArgumentException($"Column is type {trinoType} but a value of type {string.Join(", ", validTypes)} was expected");
        }

        return ConvertToTrinoTypeFromJson(value, trinoType, baseType, typeParameters);
    }

    private static object ConvertToTrinoTypeFromJson(object value, string trinoType, string baseType, string typeParameters)
    {
        switch (baseType.ToLower())
        {
            case TRINO_BIGINT:
                // JSON parser automatically converts to long
                return value;
            case TRINO_INTEGER:
                return Convert.ToInt32(value);
            case TRINO_SMALLINT:
                return Convert.ToInt16(value);
            case TRINO_TINYINT:
                return Convert.ToSByte(value);
            case TRINO_BOOLEAN:
                // JSON parser automatically converts to bool
                return value;
            case TRINO_DOUBLE:
                // JSON parser automatically converts to double
                return value;
            case TRINO_REAL:
                return Convert.ToSingle(value);
            case TRINO_DATE:
                return DateTime.ParseExact(value.ToString(), _trinoDateFormat, null, System.Globalization.DateTimeStyles.None);
            case TRINO_DECIMAL:
                return new TrinoBigDecimal(value.ToString());
            case TRINO_CHAR:
                var str = value.ToString();
                if (str.Length > 0)
                {
                    return str.ToCharArray();
                }
                return null;
            case TRINO_VARCHAR:
                return value.ToString();
            case TRINO_TIME:
                return TimeSpan.ParseExact(value.ToString(), _trinoTimeFormat, null);
            case TRINO_TIME_WITH_TIME_ZONE:
                // No time with time zone in C#
                return value.ToString();
            case TRINO_TIMESTAMP:
                return DateTime.Parse(value.ToString());
            case TRINO_TIMESTAMP_WITH_TIME_ZONE:
                // custom parsing to handle fractional seconds
                var timestampParts = _trinoTimestampWithTimezoneFormat.Match(value.ToString());
                if (!timestampParts.Success)
                {
                    throw new TrinoException($"Could not parse timestamp with time zone: {value}");
                }

                var year = Convert.ToInt32(timestampParts.Groups[1].Value);
                var month = Convert.ToInt32(timestampParts.Groups[2].Value);
                var day = Convert.ToInt32(timestampParts.Groups[3].Value);
                var hour = Convert.ToInt32(timestampParts.Groups[4].Value);
                var minute = Convert.ToInt32(timestampParts.Groups[5].Value);
                var second = Convert.ToInt32(timestampParts.Groups[6].Value);
                var fraction = string.IsNullOrEmpty(timestampParts.Groups[8].Value) ? 0 : Convert.ToInt32(timestampParts.Groups[8].Value);
                // convert fraction to ticks, a tick is 100 nanoseconds
                // Which is 7 digits of precision
                if (timestampParts.Groups[8].Value.Length > 7)
                {
                    throw new TrinoException($"Timestamp with time zone has more than 7 digits of precision: {value}");
                }
                var fractionLength = timestampParts.Groups[8].Value.Length;
                var fractionTicks = fraction * (int)Math.Pow(10, 7 - fractionLength);
                var timezone = timestampParts.Groups[9].Value;
                var offset = TimeSpan.Zero;
                if (timezone != "UTC")
                {
                    var offsetMatch = _trinoOffset.Match(timezone);
                    // if not an offset match
                    if (!offsetMatch.Success)
                    {
                        throw new TrinoException($"Could not parse timezone in timestamp: {value}");
                    }
                    var offsetHours = Convert.ToInt32(offsetMatch.Groups[1].Value);
                    var offsetMinutes = Convert.ToInt32(offsetMatch.Groups[2].Value);
                    offset = new TimeSpan(offsetHours, offsetMinutes, 0);
                }

                var dateTimeParsed = new DateTimeOffset(year, month, day, hour, minute, second, offset).AddTicks(fractionTicks);
                return dateTimeParsed;
            case TRINO_INTERVAL_YEAR_TO_MONTH:
                var separator = value.ToString().IndexOf('-');
                var years = Convert.ToInt32(value.ToString().Substring(0, separator));
                var months = Convert.ToInt32(value.ToString().Substring(separator + 1));
                return new DateTime(years, months, 1);
            case TRINO_INTERVAL_DAY_TO_SECOND:
                var dayToTimeSeparator = value.ToString().IndexOf(' ');
                var days = Convert.ToInt32(value.ToString().Substring(0, dayToTimeSeparator));
                var time = value.ToString().Substring(dayToTimeSeparator + 1);
                return TimeSpan.ParseExact(time, _trinoTimeFormat, null).Add(TimeSpan.FromDays(days));
            case TRINO_VARBINARY:
                // convert base 64 string to byte array
                return Convert.FromBase64String(value.ToString());
            case TRINO_ARRAY:
                return HandleComplexType(baseType, typeParameters, value);
            case TRINO_MAP:
                return HandleComplexType(baseType, typeParameters, value);
            case TRINO_UUID:
                return Guid.Parse(value.ToString());
            default:
                return value.ToString();
        }
    }

    public static void GetNestedTypes(string trinoType, out string baseType, out string typeParameters)
    {
        var typeParametersIndex = trinoType.IndexOf("(");
        var typeParametersIndexEnd = trinoType.LastIndexOf(")");
        baseType = typeParametersIndex != -1 && typeParametersIndexEnd != -1
            ? trinoType.Substring(0, typeParametersIndex) + trinoType.Substring(typeParametersIndexEnd + 1, trinoType.Length - typeParametersIndexEnd - 1)
            : trinoType;
        typeParameters = typeParametersIndex != -1 ? trinoType.Substring(typeParametersIndex + 1, typeParametersIndexEnd - typeParametersIndex - 1) : null;
    }

    public static Type GetClrTypeFromTrinoType(TrinoColumn trinoType)
    {
        GetNestedTypes(trinoType.Type, out var baseType, out var typeParameters);

        switch (baseType.ToLower())
        {
            case TRINO_BIGINT:
                return typeof(long);
            case TRINO_INTEGER:
                return typeof(int);
            case TRINO_SMALLINT:
                return typeof(short);
            case TRINO_TINYINT:
                return typeof(byte);
            case TRINO_BOOLEAN:
                return typeof(bool);
            case TRINO_DOUBLE:
                return typeof(double);
            case TRINO_REAL:
                return typeof(float);
            case TRINO_DECIMAL:
                return typeof(decimal);
            case TRINO_VARCHAR:
            case TRINO_CHAR:
                return typeof(string);
            case TRINO_TIME_WITH_TIME_ZONE:
                // No time with time zone in C#
                return typeof(string);
            case TRINO_TIMESTAMP:
            case TRINO_TIMESTAMP_WITH_TIME_ZONE:
            case TRINO_DATE:
            case TRINO_INTERVAL_YEAR_TO_MONTH:
                return typeof(DateTime);
            case TRINO_TIME:
            case TRINO_INTERVAL_DAY_TO_SECOND:
                return typeof(TimeSpan);
            case TRINO_VARBINARY:
                // convert base 64 string to byte array
                return typeof(byte[]);
            case TRINO_ARRAY:
                return typeof(List<object>);
            case TRINO_MAP:
                return typeof(Dictionary<object, object>);
            case TRINO_UUID:
                return typeof(Guid);
            default:
                return typeof(string);
        }
    }

    public static Type GetTrinoTypeFromClrType(Type t)
    {
        switch (t.Name)
        {
            case TRINO_BIGINT:
                return typeof(long);
            case TRINO_INTEGER:
                return typeof(int);
            case TRINO_SMALLINT:
                return typeof(short);
            case TRINO_TINYINT:
                return typeof(byte);
            case TRINO_BOOLEAN:
                return typeof(bool);
            case TRINO_DOUBLE:
                return typeof(double);
            case TRINO_REAL:
                return typeof(float);
            case TRINO_DECIMAL:
                return typeof(decimal);
            case TRINO_VARCHAR:
            case TRINO_CHAR:
                return typeof(string);
            case TRINO_TIME_WITH_TIME_ZONE:
                // No time with time zone in C#
                return typeof(string);
            case TRINO_TIMESTAMP_WITH_TIME_ZONE:
                return typeof(DateTimeOffset);
            case TRINO_TIMESTAMP:
            case TRINO_DATE:
            case TRINO_INTERVAL_YEAR_TO_MONTH:
                return typeof(DateTime);
            case TRINO_TIME:
            case TRINO_INTERVAL_DAY_TO_SECOND:
                return typeof(TimeSpan);
            case TRINO_VARBINARY:
                // convert base 64 string to byte array
                return typeof(byte[]);
            case TRINO_ARRAY:
                return typeof(List<object>);
            case TRINO_MAP:
                return typeof(Dictionary<object, object>);
            case TRINO_UUID:
                return typeof(Guid);
            default:
                return typeof(string);
        }
    }

    private static object HandleComplexType(string baseType, string typeParameters, object value)
    {
        if (baseType == TRINO_ARRAY)
        {
            if (typeParameters == null)
            {
                throw new TrinoException("Array type must have type parameters");
            }

            JsonElement arrayElement;
            if (value is string stringValue)
            {
                try
                {
                    using (var doc = JsonDocument.Parse(stringValue))
                    {
                        arrayElement = doc.RootElement.Clone();
                    }
                }
                catch (Exception ex)
                {
                    throw new TrinoException("Failed to parse string as JSON array", ex);
                }
            }
            else if (value is JsonElement je && je.ValueKind == JsonValueKind.Array)
            {
                arrayElement = je;
            }
            else
            {
                throw new TrinoException("Array type expected to be JsonElement with Array kind");
            }

            var list = new List<object>();
            foreach (var item in arrayElement.EnumerateArray())
            {
                list.Add(ConvertToTrinoTypeFromJson(GetJsonElementValue(item), typeParameters));
            }

            return list;
        }
        else if (baseType == TRINO_MAP)
        {
            if (typeParameters == null)
            {
                throw new TrinoException("Map type must have type parameters");
            }

            JsonElement objectElement;
            if (value is string stringValue)
            {
                try
                {
                    using (var doc = JsonDocument.Parse(stringValue))
                    {
                        objectElement = doc.RootElement.Clone();
                    }
                }
                catch (Exception ex)
                {
                    throw new TrinoException("Failed to parse string as JSON object", ex);
                }
            }
            else if (value is JsonElement je && je.ValueKind == JsonValueKind.Object)
            {
                objectElement = je;
            }
            else
            {
                throw new TrinoException("Map type expected to be JsonElement with Object kind");
            }

            var map = new Dictionary<object, object>();
            var types = typeParameters.Split(',');
            var keyType = types[0].Trim();
            var valueType = types[1].Trim();
            foreach (var item in objectElement.EnumerateObject())
            {
                map.Add(ConvertToTrinoTypeFromJson(item.Name, keyType), ConvertToTrinoTypeFromJson(GetJsonElementValue(item.Value), valueType));
            }

            return map;
        }
        else
        {
            throw new Exception("Unknown complex type");
        }
    }

    private static object GetJsonElementValue(JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.String:
                return element.GetString();
            case JsonValueKind.Number:
                if (element.TryGetInt64(out var longVal))
                {
                    return longVal;
                }

                return element.GetDouble();
            case JsonValueKind.True:
                return true;
            case JsonValueKind.False:
                return false;
            case JsonValueKind.Null:
                return null;
            case JsonValueKind.Array:
            case JsonValueKind.Object:
                return element;
            default:
                return element.ToString();
        }
    }
}