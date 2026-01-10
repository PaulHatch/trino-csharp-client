using System;

namespace Trino.Core;

/// <summary>
/// Contains the constants used by the Trino client.
/// </summary>
public static class Constants
{
    public const string TRINO_CLIENT_NAME = "Trino Microsoft .NET Client";

    // Increasing the max target result size to 5MB over the default increases read performance by 30%.
    internal const long MAX_TARGET_RESULT_SIZE_MB = 5;

    // The default buffer size for the query executor.
    // Set relative to optimal value of MaxTargetResultSizeMB
    public const long DEFAULT_BUFFER_SIZE_BYTES = (MAX_TARGET_RESULT_SIZE_MB * 10) * 1024 * 1024;

    /// <summary>
    /// HTTP connection timeout
    /// </summary>
    public static TimeSpan HttpConnectionTimeout => TimeSpan.FromSeconds(100);
}