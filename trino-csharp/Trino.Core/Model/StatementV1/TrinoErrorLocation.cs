namespace Trino.Core.Model.StatementV1;

public class TrinoErrorLocation
{
    public long LineNumber { get; set; }
    public long ColumnNumber { get; set; }
}