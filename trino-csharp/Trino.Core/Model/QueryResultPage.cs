using System.Collections.Generic;
using System.Text;
using Trino.Core.Model.StatementV1;

namespace Trino.Core.Model;

/// <summary>
/// Main model class representing a Trino statement API response
/// </summary>
internal class QueryResultPage : Statement
{
    /// <summary>
    /// Gets or sets the data rows in the response.
    /// </summary>
    public List<List<object>> Data
    {
        get;
        set;
    } = [];

    /// <summary>
    /// Gets or sets the columns (schema definition) in the response.
    /// </summary>
    public IList<TrinoColumn>? Columns { get; set; }

    /// <summary>
    /// Indicates whether this message contains data
    /// </summary>
    public bool HasData => Data is {Count: > 0};

    /// <summary>
    /// Construct a string containing the data in this response
    /// </summary>
    /// <returns></returns>
    public override string ToString()
    {
        return ToTsv();
    }

    /// <summary>
    /// Construct a TSV from the data in the response
    /// </summary>
    /// <returns>TSV representation of the page.</returns>
    private string ToTsv(char separator = '\t', bool includeColumnNames = true)
    {
        var sv = new StringBuilder();
        var isNewRow = true;
        if (includeColumnNames && Columns != null)
        {
            foreach (var col in Columns)
            {
                if (isNewRow)
                {
                    isNewRow = false;
                }
                else
                {
                    sv.Append(separator);
                }
                sv.Append(col.Name);
            }
            sv.AppendLine();
        }

        isNewRow = true;
        foreach (var row in Data)
        {
            foreach (var value in row)
            {
                if (isNewRow)
                {
                    isNewRow = false;
                }
                else
                {
                    sv.Append(separator);
                }
                sv.Append(value);
            }
            sv.AppendLine();
            isNewRow = true;
        }

        return sv.ToString();
    }
}