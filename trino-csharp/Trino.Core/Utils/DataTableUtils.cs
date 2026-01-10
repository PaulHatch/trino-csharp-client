using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Trino.Core.Model.StatementV1;

namespace Trino.Core.Utils;

public static class DataTableUtils
{
    /// <summary>
    /// Constructs a data table prepopulated with schema.
    /// </summary>
    /// <param name="columns"></param>
    /// <returns></returns>
    public static DataTable BuildDataTable(this IList<TrinoColumn> columns)
    {
        var dt = new DataTable();
        if (columns != null && dt.Columns.Count == 0)
        {
            foreach (var column in columns)
            {
                dt.Columns.Add(new DataColumn(column.Name, column.GetColumnType()));
            }
        }
        return dt;
    }

    /// <summary>
    /// Constructs a data table prepopulated with schema asynchronously.
    /// </summary>
    /// <exception cref="TrinoException"></exception>
    public async static Task<DataTable> BuildDataTableAsync(this RecordExecutor recordExecutor)
    {
        var records = recordExecutor.Records;
        await records.PopulateColumnsAsync().ConfigureAwait(false);
        var dt = records.Columns.BuildDataTable();
        var rowCount = 0;
        while (await records.MoveNextAsync().ConfigureAwait(false))
        {
            if (records.Current.Count != dt.Columns.Count)
            {
                throw new TrinoException($"Column count {records.Current.Count} does not match schema column count {dt.Columns.Count}.");
            }

            var dr = dt.NewRow();
            for (var colIndex = 0; colIndex < records.Current.Count; colIndex++)
            {
                if (records.Current[colIndex] != null)
                {
                    dr[colIndex] = Convert.ChangeType(records.Current[colIndex], dt.Columns[colIndex].DataType);
                }
            }
            dt.Rows.Add(dr);
            rowCount++;
        }
        return dt;
    }
}