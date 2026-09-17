using Microsoft.Extensions.Logging;

namespace SharedKernel.Tests;

internal sealed class TestLogger : ILogger
{
    public static TestLogger Instance { get; } = new();

    public IDisposable BeginScope<TState>(TState state)
        where TState : notnull
        => NullScope.Instance;

    public bool IsEnabled(LogLevel logLevel) => false;

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
    }

    private sealed class NullScope : IDisposable
    {
        public static NullScope Instance { get; } = new();

        public void Dispose()
        {
        }
    }
}

internal sealed class TestLogger<T> : ILogger<T>
{
    public static TestLogger<T> Instance { get; } = new();

    public IDisposable BeginScope<TState>(TState state)
        where TState : notnull
        => TestLogger.Instance.BeginScope(state);

    public bool IsEnabled(LogLevel logLevel) => false;

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
    }
}
