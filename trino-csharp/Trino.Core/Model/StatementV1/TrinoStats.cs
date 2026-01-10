using System;

namespace Trino.Core.Model.StatementV1;

/// <summary>
/// Statistics about query execution
/// </summary>
public class TrinoStats
{
    /// <summary>
    /// State of the query
    /// </summary>
    public string State
    {
        get;
        set;
    }

    /// <summary>
    /// True, if the query is queued
    /// </summary>
    public bool Queued
    {
        get;
        set;
    }

    /// <summary>
    /// True, if the query was scheduled
    /// </summary>
    public bool Scheduled
    {
        get;
        set;
    }

    public long Nodes { get; set; }
    public long TotalSplits { get; set; }
    public long QueuedSplits { get; set; }
    public long RunningSplits { get; set; }
    public long CompletedSplits { get; set; }
    public long CpuTimeMillis { get; set; }
    public long WallTimeMillis { get; set; }
    public long QueuedTimeMillis { get; set; }
    public long ElapsedTimeMillis { get; set; }
    public long ProcessedRows { get; set; }
    public long ProcessedBytes { get; set; }
    public long PeakMemoryBytes { get; set; }
    public long SpilledBytes { get; set; }
    public double ProgressPercentage { get; set; }

    /// <summary>
    /// Get the query execution progress percentage as a ratio
    /// </summary>
    /// <returns>Progress executing the query</returns>
    public double GetProgressRatio()
    {
        if (TotalSplits == 0)
        {
            return 0;
        }
        else
        {
            return Math.Round(CompletedSplits / (double)TotalSplits, 2);
        }
    }
}