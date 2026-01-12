namespace Trino.Core.Model.InfoV1;

public class TrinoInfo
{
    public string? Uptime { get; set; }
    public TrinoNodeVersion? NodeVersion { get; set; }
    public string? Environment { get; set; }
    public bool Starting { get; set; }
}
