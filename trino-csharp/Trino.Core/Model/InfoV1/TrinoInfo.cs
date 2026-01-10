namespace Trino.Core.Model.InfoV1
{
    public class TrinoInfo
    {
        public string uptime { get; set; }
        public TrinoNodeVersion nodeVersion { get; set; }
        public string environment { get; set; }
        public bool starting { get; set; }
    }
}
