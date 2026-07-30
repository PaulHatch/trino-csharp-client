using Trino.Core.Model.StatementV1;
using Trino.Core.Types;

namespace Trino.Core.Test;

[TestClass]
public class TrinoColumnTests
{
    [TestMethod]
    [DataRow("boolean", typeof(bool))]
    [DataRow("tinyint", typeof(sbyte))]
    [DataRow("smallint", typeof(short))]
    [DataRow("integer", typeof(int))]
    [DataRow("bigint", typeof(long))]
    [DataRow("real", typeof(float))]
    [DataRow("double", typeof(double))]
    [DataRow("decimal", typeof(TrinoBigDecimal))]
    [DataRow("decimal(38,2)", typeof(TrinoBigDecimal))]
    [DataRow("date", typeof(DateTime))]
    [DataRow("timestamp", typeof(DateTime))]
    [DataRow("timestamp(3)", typeof(DateTime))]
    [DataRow("timestamp(6)", typeof(DateTime))]
    [DataRow("timestamp with time zone", typeof(DateTimeOffset))]
    [DataRow("timestamp(3) with time zone", typeof(DateTimeOffset))]
    [DataRow("time", typeof(TimeSpan))]
    [DataRow("time(6)", typeof(TimeSpan))]
    [DataRow("time with time zone", typeof(string))]
    [DataRow("time(3) with time zone", typeof(string))]
    [DataRow("interval day to second", typeof(TimeSpan))]
    [DataRow("interval year to month", typeof(TrinoIntervalYearToMonth))]
    [DataRow("uuid", typeof(Guid))]
    [DataRow("varbinary", typeof(byte[]))]
    [DataRow("varchar", typeof(string))]
    [DataRow("varchar(50)", typeof(string))]
    [DataRow("char", typeof(string))]
    [DataRow("char(10)", typeof(string))]
    [DataRow("json", typeof(string))]
    [DataRow("array(integer)", typeof(string))]
    [DataRow("map(varchar, integer)", typeof(string))]
    [DataRow("row(a integer, b varchar)", typeof(string))]
    [DataRow("ipaddress", typeof(string))]
    [DataRow("unknown_future_type", typeof(string))]
    [DataRow(null, typeof(string))]
    public void GetColumnType_MapsTrinoTypeToClrType(string? trinoType, Type expected)
    {
        var column = new TrinoColumn { Name = "col", Type = trinoType };

        Assert.AreEqual(expected, column.GetColumnType());
    }
}