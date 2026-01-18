using System.Collections.Generic;

namespace Trino.Core;

/// <summary>
/// Contains client session properties that are recieved from Trino.
/// </summary>
internal class ClientSessionOutput
{
    internal string? SetCatalog { get; set; }
    internal string? SetSchema { get; set; }
    internal string? SetPath { get; set; }
    internal string? SetAuthorizationUser { get; set; }
    internal bool ResetAuthorizationUser { get; set; }
    internal Dictionary<string, string> SetSessionProperties { get; set; } = new();
    internal Dictionary<string, string> ResponseAddedPrepare { get; set; } = new();
    internal Dictionary<string, string> ResponseDeallocatedPrepare { get; set; } = new();
}
