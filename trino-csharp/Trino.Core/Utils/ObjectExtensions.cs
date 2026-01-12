using System;
using System.Collections.Generic;
using System.Net.Http.Headers;

namespace Trino.Core.Utils;

internal static class ObjectExtensions
{
    internal static T IsNullArgument<T>(this T value, string arg) => value ?? throw new ArgumentNullException(arg);

    internal static IEnumerable<string> GetValuesOrEmpty(this HttpResponseHeaders headers, string headerName)
        => headers.TryGetValues(headerName, out var values) ? values : new List<string>();
}