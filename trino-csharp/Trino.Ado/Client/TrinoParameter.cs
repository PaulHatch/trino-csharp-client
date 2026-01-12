using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace Trino.Ado.Client
{
    /// <summary>
    /// Definition of a Trino parameter. Only the ParameterName is used by Trino.
    /// </summary>
    public class TrinoParameter : DbParameter
    {
        public override int Size { get; set; }
        public override DbType DbType { get; set; }
        public override ParameterDirection Direction { get; set; }
        public override bool IsNullable { get; set; }

        [AllowNull]
        public override string ParameterName
        {
            get => field ?? string.Empty;
            set => field = value;
        }

        [AllowNull]
        public override string SourceColumn
        {
            get => field ?? string.Empty;
            set => field = value;
        }

        public override DataRowVersion SourceVersion { get; set; }
        public override object? Value { get; set; }
        public override bool SourceColumnNullMapping { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

        public override void ResetDbType()
        {
            throw new NotSupportedException();
        }
    }
}
