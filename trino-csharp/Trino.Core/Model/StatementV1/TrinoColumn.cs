using System;
using Trino.Core.Types;
using Trino.Core.Utils;

namespace Trino.Core.Model.StatementV1;

/// <summary>
/// Represents a Presto column definition
/// </summary>
public class TrinoColumn
{
    /// <summary>
    /// The column name
    /// </summary>
    public string? Name { get; set; }

    /// <summary>
    /// The Trino column data type. Call GetColumnType() to get the .NET type
    /// </summary>
    public string? Type { get; set; }

    public Type GetColumnType()
    {
        TrinoTypeConverters.GetNestedTypes(Type, out var baseType, out _);

        return baseType.ToLowerInvariant() switch
        {
            TrinoTypeConverters.TRINO_BOOLEAN => typeof(bool),
            TrinoTypeConverters.TRINO_TINYINT => typeof(sbyte),
            TrinoTypeConverters.TRINO_SMALLINT => typeof(short),
            TrinoTypeConverters.TRINO_INTEGER => typeof(int),
            TrinoTypeConverters.TRINO_BIGINT => typeof(long),
            TrinoTypeConverters.TRINO_REAL => typeof(float),
            TrinoTypeConverters.TRINO_DOUBLE => typeof(double),
            TrinoTypeConverters.TRINO_DECIMAL => typeof(TrinoBigDecimal),
            TrinoTypeConverters.TRINO_DATE or TrinoTypeConverters.TRINO_TIMESTAMP => typeof(DateTime),
            TrinoTypeConverters.TRINO_TIMESTAMP_WITH_TIME_ZONE => typeof(DateTimeOffset),
            TrinoTypeConverters.TRINO_TIME or TrinoTypeConverters.TRINO_INTERVAL_DAY_TO_SECOND => typeof(TimeSpan),
            TrinoTypeConverters.TRINO_INTERVAL_YEAR_TO_MONTH => typeof(TrinoIntervalYearToMonth),
            TrinoTypeConverters.TRINO_UUID => typeof(Guid),
            TrinoTypeConverters.TRINO_VARBINARY => typeof(byte[]),
            // time with time zone has no C# equivalent and is surfaced as a string
            TrinoTypeConverters.TRINO_VARCHAR or TrinoTypeConverters.TRINO_CHAR
                or TrinoTypeConverters.TRINO_TIME_WITH_TIME_ZONE or TrinoTypeConverters.TRINO_JSON
                or TrinoTypeConverters.TRINO_ARRAY or TrinoTypeConverters.TRINO_MAP
                or TrinoTypeConverters.TRINO_ROW or TrinoTypeConverters.TRINO_IP => typeof(string),
            _ => typeof(string)
        };
    }
}
