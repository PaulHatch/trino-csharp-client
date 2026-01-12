using System;
using System.Collections.Generic;

namespace Trino.Core;

/// <summary>
/// Represents the configuration settings for a Trino session. Contains all properties that can be set
/// for individual queries, including authentication, server settings, and session behavior options.
/// </summary>
public class ClientSessionProperties
{
    public const int DEFAULT_PORT = 443;

    // Server and Connection Settings
    public Uri? Server { get; set; }
    public string? Path { get; set; }
    public TimeSpan? ClientRequestTimeout { get; set; }
    public TimeSpan? Timeout { get; set; }
    public bool TestConnection { get; set; }

    // Authentication and Security
    public string? User { get; set; }
    public string? AuthorizationUser { get; set; }
    public string? Principal { get; set; }
    public Dictionary<string, string> ExtraCredentials { get; set; } = new();
    public Dictionary<string, ClientSelectedRole> Roles { get; set; } = new();

    // Database Settings
    public string? Catalog { get; set; }
    public string? Schema { get; set; }
    public string? TransactionId { get; set; }

    // Client Configuration
    public string Source { get; set; } = Constants.TRINO_CLIENT_NAME;
    public string? ClientInfo { get; set; }
    public HashSet<string> ClientTags { get; set; } = [];
    public string? TraceToken { get; set; }
    public string ServerType { get; set; } = "Trino";

    // Session Properties
    public Dictionary<string, string> Properties { get; set; } = new();
    public Dictionary<string, string> PreparedStatements { get; set; } = new();
    public Dictionary<string, string> ResourceEstimates { get; set; } = new();
    public Dictionary<string, string>? AdditionalHeaders { get; set; }

    // Locale and Time Settings
    public string? Locale { get; set; }
    public string? TimeZone { get; set; }  // Cannot be TimeZoneInfo because it's not compatible with Java's java.time.ZoneId

    // Connection Behavior
    public bool CompressionDisabled { get; set; }
    public bool AllowHostNameCnMismatch { get; set; }
    public bool AllowSelfSignedServerCert { get; set; }
    public bool UseSystemTrustStore { get; set; }

    // Certificate Settings
    public string? TrustedCertPath { get; set; }
    public string? TrustedCertificate { get; set; }

    /// <summary>
    /// Creates a server URI from component pieces for easier server configuration.
    /// </summary>
    /// <param name="host">The hostname of the server.</param>
    /// <param name="enableSsl">Whether to use HTTPS (true) or HTTP (false).</param>
    /// <param name="port">The server port number.</param>
    /// <param name="path">Optional path component for the URI.</param>
    /// <returns>A fully constructed server URI.</returns>
    public static Uri GetServerUri(string host, bool enableSsl = true, int port = 443, string? path = null)
    {
        var protocol = enableSsl ? "https" : "http";
        var serverUri = new Uri($"{protocol}://{host}:{port}");

        return string.IsNullOrEmpty(path)
            ? serverUri
            : new Uri(serverUri, path);
    }

    /// <summary>
    /// Combines the current session properties with updates received from Trino server.
    /// Creates a new instance with the merged properties while preserving unchangeable settings.
    /// </summary>
    /// <param name="updates">Session property updates from the server.</param>
    /// <returns>A new ClientSessionProperties instance with combined settings.</returns>
    internal ClientSessionProperties Combine(ClientSessionOutput updates)
    {
        return new ClientSessionProperties
        {
            AdditionalHeaders = AdditionalHeaders,
            AuthorizationUser = updates.ResetAuthorizationUser ? null : updates.SetAuthorizationUser ?? AuthorizationUser,
            Catalog = updates.SetCatalog ?? Catalog,
            ClientInfo = ClientInfo,
            ClientRequestTimeout = ClientRequestTimeout,
            ClientTags = ClientTags,
            CompressionDisabled = CompressionDisabled,
            ExtraCredentials = ExtraCredentials,
            Path = updates.SetPath ?? Path,
            PreparedStatements = MergeDictionary(PreparedStatements, updates.ResponseAddedPrepare, updates.ResponseDeallocatedPrepare),
            Principal = Principal,
            Properties = MergeDictionary(updates.SetSessionProperties, Properties),
            ResourceEstimates = ResourceEstimates,
            Roles = Roles,
            Schema = updates.SetSchema ?? Schema,
            Server = Server,
            ServerType = ServerType,
            Source = Source,
            TestConnection = TestConnection,
            Timeout = Timeout,
            TimeZone = TimeZone,
            TraceToken = TraceToken,
            TransactionId = TransactionId,
            User = User
        };
    }

    /// <summary>
    /// Merges multiple dictionaries, handling additions and removals of key-value pairs.
    /// </summary>
    /// <param name="baseDict">The base dictionary to merge into.</param>
    /// <param name="additions">Dictionary containing entries to add or update.</param>
    /// <param name="removals">Optional dictionary whose keys should be removed from the result.</param>
    /// <returns>A new dictionary containing the merged results.</returns>
    private static Dictionary<string, string> MergeDictionary(
        Dictionary<string, string> baseDict,
        Dictionary<string, string> additions,
        Dictionary<string, string>? removals = null)
    {
        var result = new Dictionary<string, string>(baseDict);

        foreach (var item in additions)
        {
            if (!result.ContainsKey(item.Key))
            {
                result.Add(item.Key, item.Value);
            }
        }

        if (removals != null)
        {
            foreach (var key in removals.Keys)
            {
                result.Remove(key);
            }
        }

        return result;
    }
}
