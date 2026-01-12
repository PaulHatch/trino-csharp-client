// Enabling TEST_OUTPUT writes the JSON response and headers to a file in the current directory to use for UTs.
//#define TEST_OUTPUT

using Trino.Core.Utils;
using System.Text.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Trino.Core.Logging;
using Trino.Core.Model;
using Trino.Core.Model.StatementV1;
using static Trino.Core.QueryState;

namespace Trino.Core;

/// <summary>
/// Handles direct interactions with Trino statement rest API /v1/statement/
/// </summary>
internal class StatementClientV1 : AbstractClient<Statement>
{
    // Initialize values for client response delay
    // Java client has 100ms initial delay, but 50ms provides noticeably better performance in testing.
    private double readDelay = _initialPageReadDelayMsec;
    private int readCount;
    private static readonly int _initialPageReadDelayMsec = (int) TimeSpan.FromMilliseconds(50).TotalMilliseconds;

    private static readonly int _maxReadDelayMsec = (int) TimeSpan.FromSeconds(5).TotalMilliseconds;

    // Java client does 100ms backoff but this affects query performance especially for metadata and cache operations.
    // This backoff produces fewer calls than the Java client.
    private static readonly double _backoffAmount = 1.2;

    private static readonly HashSet<HttpStatusCode> _ok = [HttpStatusCode.OK];
    private static readonly HashSet<HttpStatusCode> _oKorNoContent = [HttpStatusCode.OK, HttpStatusCode.NoContent];

    /// <summary>
    /// The default prefix for a parameterized query used when properties are provided.
    /// </summary>
    private string ParameterizedQueryPrefix => Session.Properties.ServerType.ToLower();

    /// <summary>
    /// Client capabilities is a comma separated list. Parametric datetime allows variable precision date times.
    /// </summary>
    private const string _clientCapabilities = "PARAMETRIC_DATETIME";

    // Timeout properties
    private readonly Stopwatch stopwatch = new();

    /// <summary>
    /// Last statement v1 response. Used to get stats and status from the server.
    /// </summary>
    private Statement Statement { get; set; } = null!;

    private readonly ClientSessionOutput sessionSet = new();

    /// <summary>
    /// The current executing state of the query.
    /// </summary>
    internal QueryState State { get; private set; }

    public bool IsTimeout =>
        Session.Properties.Timeout is {Ticks: > 0}
        && stopwatch.ElapsedTicks > Session.Properties.Timeout.Value.Ticks;

    protected override string ResourcePath => throw new NotImplementedException();

    /// <summary>
    /// Constructor
    /// </summary>
    internal StatementClientV1(
        ClientSession session,
        CancellationToken cancellationToken,
        ILoggerWrapper? logger = null) : base(session, logger, cancellationToken)
    {
        stopwatch.Start();
        State = new QueryState();

        var handler = Session.Properties.CompressionDisabled
            ? new HttpClientHandler()
            : new HttpClientHandler()
                {AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate};


        if (session.Properties.UseSystemTrustStore)
        {
            handler.ClientCertificateOptions = ClientCertificateOption.Automatic;
        }
        else
        {
            if (!string.IsNullOrEmpty(session.Properties.TrustedCertPath))
            {
                try
                {
                    var cert = new X509Certificate2(session.Properties.TrustedCertPath
                                                    ?? throw new InvalidOperationException(
                                                        "TrustedCertPath is null or empty"));
                    handler.ClientCertificates.Add(cert);
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException("Failed to load trusted certificate.", ex);
                }
            }
            else if (session.Properties.TrustedCertificate is {Length: > 0} trustedCert)
            {
                try
                {
                    var cert = ConvertPemToX509Certificate(trustedCert);
                    handler.ClientCertificates.Add(cert);
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException("Failed to load trusted certificate from PEM string.", ex);
                }
            }
        }

        handler.ServerCertificateCustomValidationCallback =
            (httpRequestMessage, x509Certificate2, x509Chain, sslPolicyErrors) =>
            {
                // Allow CN mismatch
                if (session.Properties.AllowHostNameCnMismatch
                    && sslPolicyErrors == SslPolicyErrors.RemoteCertificateNameMismatch)
                {
                    return true;
                }

                // Allow self-signed certificates
                if (session.Properties.AllowSelfSignedServerCert
                    && sslPolicyErrors == SslPolicyErrors.RemoteCertificateChainErrors
                    && x509Chain?.ChainStatus is {Length: 1} chainStatus
                    && chainStatus[0].Status == X509ChainStatusFlags.UntrustedRoot)
                {
                    return true;
                }

                // Default validation is not to allow any policy errors.
                return sslPolicyErrors == SslPolicyErrors.None;
            };

        HttpClient = new HttpClient(handler);
        HttpClient.Timeout = Constants.HttpConnectionTimeout;

        if (!Session.Properties.CompressionDisabled)
        {
            HttpClient.DefaultRequestHeaders.AcceptEncoding.Add(new StringWithQualityHeaderValue("gzip"));
        }
    }

    /// <summary>
    /// Converts an embedded PEM-formatted certificate string into an X509Certificate2 object.
    /// </summary>
    /// <param name="pemString">The PEM-formatted certificate string, including "-----BEGIN CERTIFICATE-----" and "-----END CERTIFICATE-----" markers.</param>
    /// <returns>An X509Certificate2 object representing the certificate.</returns>
    internal static X509Certificate2 ConvertPemToX509Certificate(string pemString)
    {
        // Remove the PEM header and footer, extracting only the Base64 encoded portion
        var base64String = pemString
            .Replace("-----BEGIN CERTIFICATE-----", string.Empty)
            .Replace("-----END CERTIFICATE-----", string.Empty)
            .Replace("\r", string.Empty)
            .Replace("\n", string.Empty)
            .Trim();

        // Decode the Base64 string into a byte array
        var certBytes = Convert.FromBase64String(base64String);

        // Create and return an X509Certificate2 object from the byte array
        return new X509Certificate2(certBytes);
    }

    /// <summary>
    /// Get response from Trino server.
    /// </summary>
    internal async Task<TrinoStats?> GetInitialResponse(string statement, IEnumerable<QueryParameter>? parameters,
        CancellationToken cancellationToken)
    {
        string? responseContent = null;
        try
        {
            using var queryRequest = BuildInitialQueryRequest(statement, parameters);
            Logger?.LogDebug("Trino: sending request at {1} msec: {0}", queryRequest.RequestUri?.ToString(),
                stopwatch.ElapsedMilliseconds);
            responseContent = await GetResourceAsync(
                HttpClient,
                RetryableResponses,
                Session,
                queryRequest,
                _ok,
                cancellationToken).ConfigureAwait(false);

            Logger?.LogDebug("Trino: got response content: {0}", responseContent);
            Statement = JsonSerializer.Deserialize<Statement>(responseContent, JsonSerializerConfig.Options)
                        ?? throw new TrinoException("Failed to deserialize initial response from Trino server");
            Logger?.LogInformation("Query created queryId at {1} msec: {0}", Statement.ID,
                stopwatch.ElapsedMilliseconds);
            return Statement.Stats;
        }
        catch (Exception e)
        {
            if (responseContent != null)
            {
                throw new TrinoException("Error starting query. Got response: " + responseContent, e);
            }

            throw;
        }
    }

    /// <summary>
    /// Build POST request to start query.
    /// </summary>
    private HttpRequestMessage BuildInitialQueryRequest(string query, IEnumerable<QueryParameter>? parameters)
    {
        if (Session.Properties.Server == null)
        {
            throw new TrinoException("Invalid server URL: " + Session.Properties.Server);
        }

        var request = new HttpRequestMessage(HttpMethod.Post, $"{Session.Properties.Server}v1/statement");

        // Handle parameterized queries on the server side by converting any parameterized query into a prepared statement.
        var additionalPreparedStatements = new Dictionary<string, string>();
        if (parameters != null && parameters.Any())
        {
            var parameterizedQueryId = ParameterizedQueryPrefix + Guid.NewGuid().ToString().Replace("-", "");
            additionalPreparedStatements.Add(parameterizedQueryId, query);
            Logger?.LogDebug("Trino: Converting parameterized query to prepared statement: {0}", query);
            query =
                $"EXECUTE {parameterizedQueryId} USING {string.Join(", ", parameters.Select(p => p.SqlExpressionValue))}";
            Logger?.LogDebug("Trino: Converted parameterized query to prepared statement: {0}", query);
        }

        AddHeadersToRequest(request, additionalPreparedStatements);
        request.Content = new StringContent(query);
        return request;
    }

    /// <summary>
    /// Delete query on server.
    /// </summary>
    internal async Task<bool> Cancel()
    {
        return await Cancel(QueryCancellationReason.USER_CANCEL).ConfigureAwait(false);
    }

    /// <summary>
    /// Delete query on server.
    /// </summary>
    private async Task<bool> Cancel(QueryCancellationReason reason)
    {
        Logger?.LogInformation("Cancelling due to {0} queryId:{1}", reason.ToString(), Statement.ID);
        // Sets client aborted state and terminates query.
        if (State.StateTransition(TrinoQueryStates.CLIENT_ABORTED, TrinoQueryStates.RUNNING))
        {
            if (Statement.NextUri != null)
            {
                Logger?.LogInformation("Trino: Sending cancellation request queryId:{0}", Statement.ID);
                using var request = new HttpRequestMessage(HttpMethod.Delete, Statement.NextUri);
                // do not use cancellation token here as the query is already canceled
                await GetResourceAsync(
                    HttpClient,
                    RetryableResponses,
                    Session,
                    request,
                    _oKorNoContent,
                    CancellationToken.None).ConfigureAwait(false);
            }

            Logger?.LogInformation("Trino: Cancelled queryId:{0}", Statement.ID);
        }
        else
        {
            Logger?.LogInformation("Trino: Could not cancel query, already cancelled queryId:{0}, state:{1}",
                Statement.ID, State.ToString());
        }

        return State.IsClientAborted;
    }

    /// <summary>
    /// Fetches the next Trino page with data. Similar to Java client class with same name.
    /// </summary>
    internal async Task<ResponseQueueStatement> Advance()
    {
        try
        {
            var nextUri = Statement.NextUri
                          ?? throw new InvalidOperationException("Cannot advance: no next URI available");

            if (nextUri.Contains("/executing"))
            {
                nextUri += nextUri.Contains("?")
                    ? $"&targetResultSize={Constants.MAX_TARGET_RESULT_SIZE_MB}MB"
                    : $"?targetResultSize={Constants.MAX_TARGET_RESULT_SIZE_MB}MB";
            }

            Logger?.LogDebug("Trino: request: {0}", nextUri);

            var responseStr = await GetAsync(new Uri(nextUri), _ok).ConfigureAwait(false);
            Logger?.LogDebug("Trino: response: {1}", responseStr);
            var response = JsonSerializer.Deserialize<QueryResultPage>(responseStr, JsonSerializerConfig.Options)
                           ?? throw new TrinoException("Failed to deserialize query result from Trino server");
            Logger?.LogDebug("Trino: response at {0} msec with state {1}", stopwatch.ElapsedMilliseconds,
                response.Stats?.State);

            // Note, the size is estimated based on the response string size which is not the actual deserialized size.
            var responseQueueItem = new ResponseQueueStatement(response, responseStr.Length);
            if (responseQueueItem.Response.Error != null)
            {
                State.StateTransition(TrinoQueryStates.CLIENT_ERROR, TrinoQueryStates.RUNNING);
                throw new TrinoException(responseQueueItem.Response.Error.Message ?? "Unknown error from Trino server",
                    responseQueueItem.Response.Error);
            }

            // Make status available
            Statement = responseQueueItem.Response;

            // If no next URI, the query is completed.
            if (Statement.IsLastPage)
            {
                Finish();
            }
            else if (IsTimeout)
            {
                var timeout = Session.Properties.Timeout.GetValueOrDefault();
                Logger?.LogInformation("Trino: Query timed out queryId:{0}, run time: {1} s, timeout {2} s.",
                    Statement.ID, stopwatch.Elapsed.TotalSeconds, timeout.TotalSeconds);
                await Cancel(QueryCancellationReason.TIMEOUT).ConfigureAwait(false);
                throw new TimeoutException(
                    $"Trino query ran for {stopwatch.Elapsed.TotalSeconds} s, exceeding the timeout of {timeout.TotalSeconds} s.");
            }

            // Do not wait if the query had data - the next page may be ready immediately.
            if (!responseQueueItem.Response.HasData && !State.IsFinished && readCount > 4)
            {
                Logger?.LogDebug("Trino: No data yet, backoff wait queryId:{0}, delay {1} msec", Statement.ID,
                    readDelay);
                await Task.Delay((int) readDelay).ConfigureAwait(false);
                if (readDelay < _maxReadDelayMsec)
                {
                    readDelay *= _backoffAmount;
                }
            }

            readCount++;
            return responseQueueItem;
        }
        catch (Exception)
        {
            if (CancellationToken.IsCancellationRequested)
            {
                await Cancel(QueryCancellationReason.USER_CANCEL).ConfigureAwait(false);
                throw new OperationCanceledException("Cancellation requested");
            }

            throw;
        }
    }

    /// <summary>
    /// Set states to indicate the query has finished.
    /// </summary>
    private void Finish()
    {
        stopwatch.Stop();
        Session.Update(sessionSet);
        State.StateTransition(TrinoQueryStates.FINISHED, TrinoQueryStates.RUNNING);
        Logger?.LogInformation("Trino: Query finished queryId:{0}", Statement.ID);
    }

    /// <summary>
    /// Capture all response headers and set session properties.
    /// </summary>
    protected override void ProcessResponseHeaders(HttpResponseHeaders headers)
    {
        var setCatalog = headers.GetValuesOrEmpty(ProtocolHeaders.ResponseSetCatalog).FirstOrDefault();
        if (setCatalog != null)
        {
            sessionSet.SetCatalog = setCatalog;
        }

        var setSchema = headers.GetValuesOrEmpty(ProtocolHeaders.ResponseSetSchema).FirstOrDefault();
        if (setSchema != null)
        {
            sessionSet.SetSchema = setSchema;
        }

        sessionSet.SetPath = headers.GetValuesOrEmpty(ProtocolHeaders.ResponseSetPath).FirstOrDefault();

        var setAuthorizationUser =
            headers.GetValuesOrEmpty(ProtocolHeaders.ResponseSetAuthorizationUser).FirstOrDefault();
        if (setAuthorizationUser != null)
        {
            sessionSet.SetAuthorizationUser = setAuthorizationUser;
        }

        var resetAuthorizationUser =
            headers.GetValuesOrEmpty(ProtocolHeaders.ResponseSetAuthorizationUser).FirstOrDefault();
        if (setAuthorizationUser != null)
        {
            if (bool.TryParse(resetAuthorizationUser, out var resetAuthorizationUserBool))
            {
                sessionSet.ResetAuthorizationUser = resetAuthorizationUserBool;
            }
        }

        foreach (var session in headers.GetValuesOrEmpty(ProtocolHeaders.ResponseSetSession))
        {
            var keyValue = session.Split('=');
            if (keyValue.Length != 2)
            {
                continue;
            }

            sessionSet.SetSessionProperties.Add(keyValue[0], HttpUtility.UrlDecode(keyValue[1]));
        }

        foreach (var preparedStatement in headers.GetValuesOrEmpty(ProtocolHeaders.ResponseAddedPrepare))
        {
            var keyValue = preparedStatement.Split('=');
            if (keyValue.Length != 2)
            {
                throw new TrinoException("Invalid response header. Expecting key=value: " +
                                         ProtocolHeaders.ResponseAddedPrepare + ": " + preparedStatement);
            }

            var value = HttpUtility.UrlDecode(keyValue[1]);
            sessionSet.ResponseAddedPrepare.Add(keyValue[0], value);
        }

        foreach (var deallocateStatement in headers.GetValuesOrEmpty(ProtocolHeaders.ResponseDeallocatedPrepare))
        {
            var keyValue = deallocateStatement.Split('=');
            if (keyValue.Length != 2)
            {
                throw new TrinoException("Invalid response header. Expecting key=value: " +
                                         ProtocolHeaders.ResponseDeallocatedPrepare + ": " + deallocateStatement);
            }

            var value = HttpUtility.UrlDecode(keyValue[1]);
            sessionSet.ResponseDeallocatedPrepare.Add(keyValue[0], value);
        }
    }

    private void AddHeadersToRequest(HttpRequestMessage request,
        Dictionary<string, string> additionalPreparedStatements)
    {
        request.Headers.Add(ProtocolHeaders.RequestClientCapabilities, _clientCapabilities);

        if (Session.Properties.AdditionalHeaders != null)
        {
            foreach (var header in Session.Properties.AdditionalHeaders)
            {
                request.Headers.Add(header.Key, header.Value);
            }
        }

        if (!string.IsNullOrEmpty(Session.Properties.Source))
        {
            request.Headers.Add(ProtocolHeaders.RequestSource, Session.Properties.Source);
        }

        if (!string.IsNullOrEmpty(Session.Properties.TraceToken))
        {
            request.Headers.Add(ProtocolHeaders.RequestTraceToken, Session.Properties.TraceToken);
        }

        if (Session.Properties.ClientTags.Count > 0)
        {
            request.Headers.Add(ProtocolHeaders.RequestClientTags, string.Join(",", Session.Properties.ClientTags));
        }

        if (!string.IsNullOrEmpty(Session.Properties.ClientInfo))
        {
            request.Headers.Add(ProtocolHeaders.RequestClientInfo, Session.Properties.ClientInfo);
        }

        if (!string.IsNullOrEmpty(Session.Properties.Catalog))
        {
            request.Headers.Add(ProtocolHeaders.RequestCatalog, Session.Properties.Catalog);
        }

        if (!string.IsNullOrEmpty(Session.Properties.Schema))
        {
            request.Headers.Add(ProtocolHeaders.RequestSchema, Session.Properties.Schema);
        }

        if (!string.IsNullOrEmpty(Session.Properties.Path))
        {
            request.Headers.Add(ProtocolHeaders.RequestPath, Session.Properties.Path);
        }

        if (Session.Properties.TimeZone != null)
        {
            request.Headers.Add(ProtocolHeaders.RequestTimeZone, Session.Properties.TimeZone);
        }

        if (Session.Properties.Locale != null)
        {
            request.Headers.Add(ProtocolHeaders.RequestLanguage, Session.Properties.Locale);
        }

        var property = Session.Properties.Properties;
        foreach (var pair in property)
        {
            request.Headers.Add(ProtocolHeaders.RequestSession, $"{pair.Key}={HttpUtility.UrlEncode(pair.Value)}");
        }

        var resourceEstimates = Session.Properties.ResourceEstimates;
        foreach (var pair in resourceEstimates)
        {
            request.Headers.Add(ProtocolHeaders.RequestResourceEstimate,
                $"{pair.Key}={HttpUtility.UrlEncode(pair.Value)}");
        }

        var roles = Session.Properties.Roles;
        foreach (var pair in roles)
        {
            request.Headers.Add(ProtocolHeaders.RequestRole,
                $"{pair.Key}={HttpUtility.UrlEncode(pair.Value.ToString())}");
        }

        var extraCredentials = Session.Properties.ExtraCredentials;
        foreach (var pair in extraCredentials)
        {
            request.Headers.Add(ProtocolHeaders.RequestExtraCredential,
                $"{pair.Key}={HttpUtility.UrlEncode(pair.Value)}");
        }

        foreach (var pair in Session.Properties.PreparedStatements)
        {
            request.Headers.Add(ProtocolHeaders.RequestPreparedStatement,
                $"{pair.Key}={HttpUtility.UrlEncode(pair.Value)}");
        }

        foreach (var pair in additionalPreparedStatements)
        {
            request.Headers.Add(ProtocolHeaders.RequestPreparedStatement,
                $"{pair.Key}={HttpUtility.UrlEncode(pair.Value)}");
        }

        if (!string.IsNullOrEmpty(Session.Properties.TransactionId))
        {
            request.Headers.Add(ProtocolHeaders.RequestTransactionId, Session.Properties.TransactionId);
        }
    }

    private enum QueryCancellationReason
    {
        TIMEOUT,
        USER_CANCEL
    }
}