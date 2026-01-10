namespace Trino.Core.Model.StatementV1;

/// <summary>
/// Main model class representing a Trino statement API response
/// </summary>
public class Statement
{
    /// <summary>
    /// Gets or sets the query identifier
    /// </summary>
    public string ID
    {
        get;
        set;
    }

    /// <summary>
    /// Gets or sets the stats of the Trino query execution.
    /// </summary>
    public TrinoStats Stats
    {
        get;
        set;
    }

    /// <summary>
    /// Gets or sets the next URI in the paged Trino response (null indicates no more pages).
    /// </summary>
    public string NextUri
    {
        get;
        set;
    }

    /// <summary>
    /// Gets or sets the URI that points to the Trino query information UX
    /// </summary>
    public string InfoUri
    {
        get;
        set;
    }

    /// <summary>
    /// Gets or sets the error in the Trino response (if any).
    /// </summary>
    public TrinoError Error
    {
        get;
        set;
    }

    public bool IsLastPage => string.IsNullOrEmpty(NextUri);
}