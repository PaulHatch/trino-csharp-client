using System.Net;
using Trino.Ado.Server;

namespace Trino.Core.Test;

internal class TrinoTestServer : IDisposable
{
    public int Port { get; private set; }
    private readonly HttpListener listener = new();
    private readonly ManualResetEventSlim listenerReady = new(false);
    private Task? serverTask;
    private bool cancelled = false;

    private TrinoTestServer()
    {
        // Pick a random port to listen on
        Port = new Random().Next(10000) + 10000;
    }

    public static TrinoTestServer Create(string testFile)
    {
        return Create(testFile, TimeSpan.Zero);
    }

    /// <summary>
    /// Create a server to respond with pre-recorded Trino responses.
    /// </summary>
    /// <param name="waitBetweenResponses">Allows for the simulation of a slow server.</param>
    public static TrinoTestServer Create(string testFile, TimeSpan waitBetweenResponses)
    {
        TrinoTestServer server = new();
        server.StartServer(testFile, waitBetweenResponses);
        return server;
    }

    private void StartServer(string testFile, TimeSpan waitBetweenResponses)
    {
        // Check testFile exists before starting
        if (!File.Exists(testFile))
        {
            throw new FileNotFoundException(testFile);
        }

        serverTask = new Task(() =>
        {
            ConfigureTest(testFile, waitBetweenResponses);
        });
        serverTask.Start();
        listenerReady.Wait();
    }

    /// <summary>
    /// Represents a Trino HTTP response: headers and payload
    /// </summary>
    internal class TestStep
    {
        public Dictionary<string, List<string>> Headers;
        public string Payload { get; set; }

        internal TestStep()
        {
            Headers = [];
            Payload = string.Empty;
        }
    }

    internal void ConfigureTest(string testFile, TimeSpan waitBetweenResponses)
    {
        var isHeader = true;
        TestStep current = new();
        List<TestStep> testSteps = [];
        try
        {
            foreach (var line in File.ReadAllLines(testFile))
            {
                if (isHeader)
                {
                    isHeader = PrepareHeaders(current, line);
                }
                else
                {
                    // replace port in response
                    var portUpdatedResponse = line.Replace("localhost", "localhost:" + Port);
                    current.Payload = portUpdatedResponse;
                    testSteps.Add(current);
                    current = new TestStep();
                    isHeader = true;
                }
            }
        }
        catch (Exception e)
        {
            Assert.Fail(e.Message);
        }

        QueueUpResponses(testSteps, waitBetweenResponses);
    }

    private static bool PrepareHeaders(TestStep current, string line)
    {
        bool isHeader;
        var headerValues = line.Split('|')
            .Where(l => !string.IsNullOrEmpty(l))
            .Select(l => new KeyValuePair<string, string>(l[..l.IndexOf('=')], l[(l.IndexOf('=') + 1)..]));
        foreach (var header in headerValues)
        {
            if (!current.Headers.TryGetValue(header.Key, out var value))
            {
                value = [];
                current.Headers.Add(header.Key, value);
            }

            value.Add(header.Value);
        }
        isHeader = false;
        return isHeader;
    }

    /// <summary>
    /// Runs local webserver to respond with Trino HTTP responses.
    /// </summary>
    /// <param name="responses"></param>
    private void QueueUpResponses(List<TestStep> responses, TimeSpan waitBetweenResponses)
    {
        Console.WriteLine("Starting test server on port " + Port);
        // Add the prefixes.
        listener.Prefixes.Add($"http://localhost:{Port}/v1/");
        listener.Start();
        listenerReady.Set();
        Console.WriteLine("Listening...");
        // Note: The GetContext method blocks while waiting for a request.
        foreach (var response in responses)
        {
            // Listener.GetContext() blocks while waiting for a request.
            Console.WriteLine("Waiting for requests...");
            var contextTask = listener.GetContextAsync();
            while (!contextTask.IsCompleted)
            {
                if (cancelled)
                {
                    return;
                }
                contextTask.Wait(1000);
            }

            var request = contextTask.Result.Request;
            var contentLength = request.ContentLength64;
            if (contentLength > 0)
            {
                var buffer = new byte[contentLength];
                request.InputStream.Read(buffer, 0, (int)contentLength);
                var requestContent = System.Text.Encoding.UTF8.GetString(buffer);
                Console.WriteLine($"Request recieved with body: {requestContent}");
            }
            else
            {
                Console.WriteLine("Request recieved.");
            }
            // Obtain a response object.
            using (var httpListenerResponse = contextTask.Result.Response)
            {
                // Construct a response.
                var buffer = System.Text.Encoding.UTF8.GetBytes(response.Payload);

                // Disable keep-alive to ensure clean connection handling when using waitBetweenResponses
                httpListenerResponse.KeepAlive = false;

                // add headers
                foreach (var header in response.Headers)
                {
                    foreach (var value in header.Value)
                    {
                        httpListenerResponse.Headers.Add(header.Key, value);
                    }
                }

                Console.WriteLine("Starting response.");
                // Get a response stream and write the response to it.
                httpListenerResponse.ContentLength64 = buffer.Length;
                using (var output = httpListenerResponse.OutputStream)
                {
                    output.Write(buffer, 0, buffer.Length);
                }
                Console.WriteLine("Written response.");
                if (waitBetweenResponses.Ticks > 0)
                {
                    Thread.Sleep(waitBetweenResponses);
                }
            }
        }
    }

    internal TrinoConnectionProperties GetConnectionProperties()
    {
        TrinoConnectionProperties properties = new()
        {
            Catalog = "tpch",
            Host = "localhost",
            Port = Port,
            EnableSsl = false
        };
        return properties;
    }

    public void Dispose()
    {
        if (serverTask != null)
        {
            cancelled = true;
            serverTask.Wait();
        }
    }
}