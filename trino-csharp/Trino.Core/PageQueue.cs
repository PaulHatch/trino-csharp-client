using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Trino.Core.Logging;
using Trino.Core.Model.StatementV1;

namespace Trino.Core;

/// <summary>
/// Queue to hold the pages of data returned from Trino.
/// </summary>
internal class PageQueue
{
    private readonly StatementClientV1 client;
    // BlockingCollection offers no advantage over ConcurrentQueue for this use case.
    private readonly ConcurrentQueue<ResponseQueueStatement> responseQueue = new();
    private readonly ConcurrentBag<Exception> errors = [];

    // The actual buffer size
    private readonly long bufferSize;
    private readonly CancellationToken cancellationToken;
    private readonly ILoggerWrapper? logger;
    private readonly IList<Action<TrinoStats?, TrinoError?>>? queryStatusNotifications;
    private readonly SemaphoreSlim signalUpdatedQueue = new(0, int.MaxValue);
    private readonly SemaphoreSlim signalFoundResult = new(0, 1);
    private readonly SemaphoreSlim signalColumnsRead = new(0, 1);

    private readonly object readAheadLock = new();

    // backoff for checking the queue for new pages, values tuned 2024
    private const int _maxWaitForQueueTimeoutMsec = 10000;
    private const int _queueCheckBackoff = 100;
    private int waitForQueueTimeoutMsec = 50;
    private Task? readAhead;

    internal PageQueue(ILoggerWrapper? logger, IList<Action<TrinoStats?, TrinoError?>>? queryStatusNotifications, StatementClientV1 client, long bufferSize, bool isQuery, CancellationToken cancellationToken = default)
    {
        if (bufferSize == 0)
        {
            throw new ArgumentException("Buffer size of zero is not allowed as no rows can be read.");
        }

        this.logger = logger;
        this.queryStatusNotifications = queryStatusNotifications;
        this.client = client;
        this.bufferSize = bufferSize;
        this.cancellationToken = cancellationToken;
        IsQuery = isQuery;
    }

    /// <summary>
    /// The connection to Trino will consume query results.
    /// </summary>
    public bool IsQuery { get; private set; }

    /// <summary>
    /// The schema columns.
    /// </summary>
    internal IList<TrinoColumn>? Columns { get; private set; }
    internal bool IsEmpty => responseQueue.Count == 0;

    /// <summary>
    /// The last response from the statement endpoint.
    /// </summary>
    internal Statement? LastStatement { get; private set; }

    /// <summary>
    /// True if the last page has been reached.
    /// </summary>
    internal bool IsLastPage => LastStatement is {IsLastPage: true};

    /// <summary>
    /// The client state.
    /// </summary>
    internal QueryState State => client.State;

    /// <summary>
    /// True if results have been found in Trino responses.
    /// </summary>
    internal bool HasResults { get; private set; }

    /// <summary>
    /// Starts a thread to asynchronously read ahead to fill the queue with the result set.
    /// </summary>
    internal void StartReadAhead()
    {
        logger?.LogDebug("Attempt to start read ahead: queryId: {0}", LastStatement?.ID);
        // Try to read.
        // Starts a task to read. If a read task has already started, ignore.
        if (ShouldReadAheadToNextPage())
        {
            lock (readAheadLock) {
                if (readAhead == null || readAhead.IsCompleted)
                {
                    // Thread to read ahead.
                    readAhead = Task.Run(async () =>
                    {
                        logger?.LogDebug("Starting read ahead: queryId: {0}", LastStatement?.ID);
                        await ReadAhead().ConfigureAwait(false);
                    });
                }
            }
        }
    }

    /// <summary>
    /// Reads ahead until query is stopped or buffer is full.
    /// </summary>
    private async Task ReadAhead()
    {
        try
        {
            // Will keep reading from Trino until the query is complete or the buffer is over full.
            while (ShouldReadAheadToNextPage() && !ShouldStopReading())
            {
                var statementResponse = await client.Advance().ConfigureAwait(false);

                // if schema is discovered, make it available
                if (Columns == null && statementResponse.Response.Columns != null)
                {
                    Columns = statementResponse.Response.Columns;
                    signalColumnsRead.Release();
                }

                // If results are needed, queue the result pages, otherwise they can be ignored.
                if (IsQuery && statementResponse.Response.HasData)
                {
                    responseQueue.Enqueue(statementResponse);
                    HasResults = true;
                    LastStatement = statementResponse.Response;
                    signalUpdatedQueue.Release();
                }
                else
                {
                    LastStatement = statementResponse.Response;
                }
            }

            if (client.State.IsFinished)
            {
                logger?.LogDebug("Trino Query Executor: Set finished state, queryId:{0}", LastStatement?.ID);
                PublishStatus(LastStatement?.Stats, LastStatement?.Error);
            }
        }
        catch (Exception ex)
        {
            logger?.LogError("Trino Query Executor: {0}", ex.ToString());
            errors.Add(ex);
            if (ex is TrinoException exception)
            {
                PublishStatus(LastStatement?.Stats, exception.Error);
            }
        }
    }

    /// <summary>
    /// Determines if there is a reason to stop reading.
    /// </summary>
    private bool ShouldStopReading()
    {
        if (cancellationToken.IsCancellationRequested)
        {
            logger?.LogDebug("Trino Query Executor: query cancelled.");
            errors.Add(new OperationCanceledException("Query cancelled"));
            return false;
        }

        if (client.IsTimeout)
        {
            logger?.LogDebug("Trino Query Executor: terminating due to timeout.");
            errors.Add(new TimeoutException("Query timed out"));
            return true;
        }

        if (errors.Count > 0)
        {
            logger?.LogDebug("Trino Query Executor: terminating due to exceptions: {0} ", string.Join(",", errors.Select(e => e.ToString())));
            return true;
        }

        return false;
    }

    /// <summary>
    /// Continue to read pages until the queue is full or the executor is finished.
    /// Or if the query does not return results.
    /// </summary>
    private bool ShouldReadAheadToNextPage()
    {
        if (!IsLastPage && client.State is {IsClientAborted: false, IsClientError: false})
        {
            if (IsQuery)
            {
                // if this is a query, but not finished, only read ahead the size of the buffer.
                // Note, buffer is a soft approximate limit based on the string size of the pages.
                long queueSize = 0;
                foreach (var page in responseQueue)
                {
                    queueSize += page.SizeBytes;
                }
                return queueSize < bufferSize;
            }
            else
            {
                // if non-query, results are going to be thrown away, always read ahead to the end of the query.
                return true;
            }
        }
        return false;
    }

    /// <summary>
    /// Fetches list of columns for the result set.
    /// </summary>
    internal async Task<IList<TrinoColumn>?> GetColumns()
    {
        if (Columns == null)
        {
            StartReadAhead();
            while (Columns == null && !IsLastPage && !ShouldStopReading())
            {
                await signalColumnsRead.WaitAsync(_queueCheckBackoff).ConfigureAwait(false);
                ThrowIfErrors();
            }
        }
        return Columns;
    }

    /// <summary>
    /// Throw an exception if there are any errors during the read.
    /// </summary>
    internal void ThrowIfErrors()
    {
        if (errors.Any())
        {
            throw new TrinoAggregateException(errors);
        }
    }

    /// <summary>
    /// Attempt to dequeue the next available page. Poses an exponential backoff if result is not found.
    /// </summary>
    /// <returns>The next page, or null, if not available</returns>
    internal async Task<ResponseQueueStatement?> DequeueOrNull()
    {
        if (!responseQueue.TryDequeue(out var response))
        {
            // wait for signal of next dequeue
            if (!(await signalUpdatedQueue.WaitAsync(waitForQueueTimeoutMsec).ConfigureAwait(false)))
            {
                // ensure readahead is running if there is nothing to dequeue
                // backoff wait time because aggressive checks only benefit short running queries, and the signal covers most cases
                waitForQueueTimeoutMsec = Math.Min(waitForQueueTimeoutMsec + _queueCheckBackoff, _maxWaitForQueueTimeoutMsec);
            }
        }
        return response;
    }

    internal async Task<bool> Cancel()
    {
        return await client.Cancel().ConfigureAwait(false);
    }

    /// <summary>
    /// Checks to see if any results have been returned. If not, will wait for results.
    /// </summary>
    internal async Task<bool> HasData()
    {
        if (!IsQuery)
        {
            return false;
        }
        else if (HasResults)
        {
            return true;
        }
        else
        {
            StartReadAhead();
            while (!HasResults && !IsLastPage && !ShouldStopReading())
            {
                await signalFoundResult.WaitAsync(_queueCheckBackoff).ConfigureAwait(false);
                ThrowIfErrors();
            }
        }
        return HasResults;
    }

    /// <summary>
    /// Publishes a notification if the query fails and when the query finishes.
    /// </summary>
    private void PublishStatus(TrinoStats? stats, TrinoError? error)
    {
        if (queryStatusNotifications != null)
        {
            foreach (var notifier in queryStatusNotifications)
            {
                notifier.Invoke(stats, error);
            }
        }
    }

}