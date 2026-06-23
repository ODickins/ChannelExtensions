using System.Collections.Concurrent;
using System.Text.Json;
using System.Threading.Channels;
using ChannelExtensions.Durability.FileSystem.FileBackedChannel;
using Microsoft.Extensions.Logging;

namespace ChannelExtensions.Durability.FileSystem.Tests;

/// <summary>A throwaway temp directory that deletes itself on dispose.</summary>
internal sealed class TempDir : IDisposable
{
    public string Path { get; }

    public TempDir()
    {
        Path = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(), "fbc-tests-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Path);
    }

    public void Dispose()
    {
        try
        {
            Directory.Delete(Path, recursive: true);
        }
        catch
        {
            // Best effort — background drain tasks may still hold a handle briefly.
        }
    }
}

internal readonly record struct LogEntry(LogLevel Level, EventId EventId, string Message, Exception? Exception);

/// <summary>Captures every log call so tests can assert on EventIds.</summary>
internal sealed class TestLogger : ILogger
{
    public ConcurrentQueue<LogEntry> Entries { get; } = new();

    public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(
        LogLevel logLevel, EventId eventId, TState state, Exception? exception,
        Func<TState, Exception?, string> formatter)
        => Entries.Enqueue(new LogEntry(logLevel, eventId, formatter(state, exception), exception));

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();
        public void Dispose() { }
    }
}

internal static class TestHelpers
{
    // Short commit interval so tests don't wait on the production 15s default.
    public static FileBackedChannelOptions Options(
        string path, int capacity, ILogger? logger = null, int maxBlockSize = 16)
        => new(capacity, path)
        {
            CommitInterval = TimeSpan.FromMilliseconds(100),
            MaxBlockSize = maxBlockSize,
            Logger = logger,
        };

    /// <summary>Reads exactly <paramref name="count"/> items or throws on timeout.</summary>
    public static async Task<List<int>> ReadManyAsync(Channel<int> channel, int count, TimeSpan timeout)
    {
        var result = new List<int>(count);
        using var cts = new CancellationTokenSource(timeout);
        for (var i = 0; i < count; i++)
        {
            result.Add(await channel.Reader.ReadAsync(cts.Token));
        }

        return result;
    }

    /// <summary>Asserts nothing more arrives within the window (no replay / no extra items).</summary>
    public static async Task AssertNoMoreAsync(Channel<int> channel, TimeSpan within)
    {
        using var cts = new CancellationTokenSource(within);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await channel.Reader.ReadAsync(cts.Token));
    }

    /// <summary>Polls a condition until true or the timeout elapses.</summary>
    public static async Task WaitForAsync(Func<bool> condition, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        while (!condition())
        {
            if (cts.IsCancellationRequested)
            {
                return;
            }

            await Task.Delay(20);
        }
    }

    /// <summary>Serializes ints to the on-disk NDJSON format (trailing newline included).</summary>
    public static string Ndjson(IEnumerable<int> items)
        => string.Concat(items.Select(i => JsonSerializer.Serialize(i, JsonSerializerOptions.Web) + "\n"));

    public static void WriteCommittedBlock(string dir, string name, int count, string content)
        => File.WriteAllText(System.IO.Path.Combine(dir, $"{name}.{count}.ndjson"), content);
}
