using System;
using System.Collections.Generic;
using System.Linq;

namespace Trino.Core;

/// <summary>
/// A query parameter for a Trino query.
/// </summary>
public class QueryParameter
{
    public QueryParameter(object? value)
    {
        Value = value;
    }

    /// <summary>
    /// The value of the parameter.
    /// </summary>
    public object? Value { get; set; }

    /// <summary>
    /// Get the string representation of the parameter value that can be used in a SQL expression.
    /// </summary>
    internal string SqlExpressionValue
    {
        get
        {
            switch (Value)
            {
                case null:
                    return "NULL";
                case string s:
                    return $"'{s.Replace("'", "''")}'";
                case DateTime dateTime:
                    return $"timestamp '{dateTime:yyyy-MM-dd HH:mm:ss.fff}'";
                case DateTimeOffset offset:
                    return $"\"timestamp with time zone\" '{offset:yyyy-MM-dd HH:mm:ss.fff zzz}'";
                case TimeSpan span:
                    return $"'{span:c}'";
                case Guid guid:
                    return $"'{guid}'";
                case bool b:
                    return b ? "TRUE" : "FALSE";
                case byte[] binary:
                    return $"X'{BitConverter.ToString(binary).Replace("-", "")}'";
                case IEnumerable<object> enumerable:
                {
                    var items = enumerable
                        .Select(item => new QueryParameter(item).SqlExpressionValue);
                    return $"({string.Join(", ", items)})";
                }
                default:
                    return Value.ToString() ?? "NULL";
            }
        }
    }
}