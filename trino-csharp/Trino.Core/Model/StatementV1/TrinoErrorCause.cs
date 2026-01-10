using System.Collections.Generic;

namespace Trino.Core.Model.StatementV1;

/// <summary>
/// Describes the cause of a Trino error.
/// </summary>
public class TrinoErrorCause
{
    /// <summary>
    /// Trino error type
    /// </summary>
    public string Type { get; set; }

    /// <summary>
    /// Trino error message
    /// </summary>
    public string Message { get; set; }

    /// <summary>
    /// Location of the error in the query
    /// </summary>
    public TrinoErrorLocation ErrorLocation { get; set; }

    /// <summary>
    /// Stack trace of the error
    /// </summary>
    public List<string> Stack { get; set; }

    /// <summary>
    /// Suppressed errors
    /// </summary>
    public List<TrinoErrorCause> Suppressed { get; set; }

    /// <summary>
    /// Cause of the error
    /// </summary>
    public TrinoErrorCause Cause { get; set; }
}