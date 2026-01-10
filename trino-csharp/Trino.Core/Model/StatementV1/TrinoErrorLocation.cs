namespace Trino.Core.Model.StatementV1
{
    public class TrinoErrorLocation
    {
        public long lineNumber { get; set; }
        public long columnNumber { get; set; }
    }
}
