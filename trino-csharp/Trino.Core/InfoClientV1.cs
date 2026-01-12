using System.Threading;
using Trino.Core.Logging;
using Trino.Core.Model.InfoV1;

namespace Trino.Core;

/// <summary>
/// Allows the v1/info endpoint to be queried.
/// </summary>
public class InfoClientV1 : AbstractClient<TrinoInfo>
{
    protected override string ResourcePath => "v1/info";

    public InfoClientV1(ClientSession session) : base(session, null, CancellationToken.None)
    {
    }

    public InfoClientV1(ClientSession session, ILoggerWrapper? logger, CancellationToken cancellationToken) : base(session, logger, cancellationToken)
    {
    }
}