namespace Trino.Core.Model.StatementV1;

/// <summary>
/// Model class for Trino statement API response
/// </summary>
public class TrinoError
{
    /// <summary>
    /// Gets or sets the error type of the Trino error message.
    /// </summary>
    public string? Type { get; set; }

    /// <summary>
    /// Gets or sets the text of the Trino error message.
    /// </summary>
    public string? Message { get; set; }

    /// <summary>
    /// Gets of sets the Trino error code.
    /// </summary>
    public long ErrorCode { get; set; }

    /// <summary>
    /// Gets or sets the Trino error name
    /// </summary>
    public string? ErrorName { get; set; }

    /// <summary>
    /// Gets or sets the Trino error type
    /// </summary>
    public string? ErrorType { get; set; }

    /// <summary>
    /// Gets or sets the Trino query error line positions.
    /// </summary>
    public TrinoErrorLocation? ErrorLocation { get; set; }

    /// <summary>
    /// Gets or sets the Trino error failure details which includes the stack trace.
    /// </summary>
    public TrinoErrorCause? FailureInfo { get; set; }
}
