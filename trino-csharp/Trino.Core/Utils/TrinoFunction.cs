using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Trino.Core.Utils;

public class TrinoFunction
{
    private readonly string catalog;
    private readonly string functionName;
    private readonly IList<object> parameters;

    public TrinoFunction(string catalog, string functionName, IList<object> parameters)
    {
        this.catalog = catalog;
        this.functionName = functionName;
        this.parameters = parameters;
    }

    public virtual Task<RecordExecutor> ExecuteAsync(ClientSession session)
    {
        var statement = BuildFunctionStatement();
        return RecordExecutor.Execute(session, statement);
    }

    protected virtual string BuildFunctionStatement()
    {
        var stringBuilder = new StringBuilder();
        if (!string.IsNullOrEmpty(catalog))
        {
            stringBuilder.Append(catalog);
            stringBuilder.Append(".");
        }
        stringBuilder.Append(functionName);
        stringBuilder.Append("(");

        for (var i = 0; i < parameters.Count; i++)
        {
            if (i > 0)
            {
                stringBuilder.Append(", ");
            }

            // if parameter is a digit, do not quote it
            if (parameters[i] is int || parameters[i] is long || parameters[i] is float || parameters[i] is double)
            {
                stringBuilder.Append(parameters[i]);
            }
            else
            {
                stringBuilder.Append("'");
                stringBuilder.Append(parameters[i]);
                stringBuilder.Append("'");
            }
        }
        stringBuilder.Append(")");

        return stringBuilder.ToString();
    }
}