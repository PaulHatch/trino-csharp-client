using System;
using Trino.Core.Model.StatementV1;

namespace Trino.Core;

/// <summary>
/// Wraps an exception originating within Trino
/// </summary>
public class TrinoException : Exception
{
    public TrinoError? Error { get; private set;}

    /// <summary>
    /// Create a new Trino exception.
    /// </summary>
    /// <param name="message">Exception message</param>
    public TrinoException(string message) : this(message, (TrinoError?)null)
    {
    }

    /// <summary>
    /// Create a new Trino exception.
    /// </summary>
    /// <param name="message">Exception message</param>
    /// <param name="trinoError">Trino error</param>
    public TrinoException(string message, TrinoError? trinoError) : base(message)
    {
        Error = trinoError;
    }

    /// <summary>
    /// Create a new Trino exception.
    /// </summary>
    /// <param name="message">Exception message</param>
    /// <param name="inner">Inner exception</param>
    public TrinoException(string message, Exception inner) : this(message, null, inner)
    {
    }

    /// <summary>
    /// Create a new Trino exception.
    /// </summary>
    /// <param name="message">Exception message</param>
    /// <param name="trinoError">Trino error</param>
    /// <param name="inner">Inner exception</param>
    public TrinoException(string message, TrinoError? trinoError, Exception inner) : base(message, inner)
    {
        Error = trinoError;
    }
}
