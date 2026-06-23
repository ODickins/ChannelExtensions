using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Amazon.S3;
using Amazon.S3.Model;
using ChannelExtensions.Durability.S3.S3BackedChannel;
using Microsoft.Extensions.Logging;

namespace ChannelExtensions.Durability.S3.Tests;

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

    public bool HasEvent(string name) => Entries.Any(e => e.EventId.Name == name);

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();
        public void Dispose() { }
    }
}

internal static class TestHelpers
{
    // Short commit interval so tests don't wait on the production 15s default.
    public static S3BackedChannelOptions Options(
        string bucket, string prefix, IAmazonS3 client, int capacity, ILogger? logger = null, int maxChunkSize = 16)
        => new(capacity, bucket, prefix)
        {
            Client = client,
            CommitInterval = TimeSpan.FromMilliseconds(100),
            MaxChunkSize = maxChunkSize,
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

            await Task.Delay(25);
        }
    }

    /// <summary>Polls an async condition until true or the timeout elapses.</summary>
    public static async Task WaitForAsync(Func<Task<bool>> condition, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        while (!await condition())
        {
            if (cts.IsCancellationRequested)
            {
                return;
            }

            await Task.Delay(50);
        }
    }

    /// <summary>
    /// Uploads a committed NDJSON chunk object directly, exactly as the channel would, to simulate a
    /// pre-existing backlog in the bucket. Keyed by a time-ordered v7 GUID so listing sorts chronologically.
    /// </summary>
    public static async Task UploadCommittedChunkAsync(
        IAmazonS3 s3, string bucket, string prefix, IEnumerable<int> items)
    {
        var materialized = items.ToList();
        var body = string.Concat(
            materialized.Select(i => JsonSerializer.Serialize(i, JsonSerializerOptions.Web) + "\n"));

        var fileName = $"{Guid.CreateVersion7():N}.{materialized.Count}.ndjson";
        var key = string.IsNullOrEmpty(prefix) ? fileName : $"{prefix}/{fileName}";

        await s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucket,
            Key = key,
            InputStream = new MemoryStream(Encoding.UTF8.GetBytes(body)),
            ContentType = "application/x-ndjson"
        });
    }

    /// <summary>Lists the committed chunk object keys (".ndjson") currently under the prefix.</summary>
    public static async Task<List<string>> ListChunkKeysAsync(IAmazonS3 s3, string bucket, string prefix)
    {
        var listPrefix = string.IsNullOrEmpty(prefix) ? null : prefix + "/";
        var keys = new List<string>();
        string? token = null;

        do
        {
            var response = await s3.ListObjectsV2Async(new ListObjectsV2Request
            {
                BucketName = bucket,
                Prefix = listPrefix,
                ContinuationToken = token
            });

            if (response.S3Objects is not null)
                keys.AddRange(response.S3Objects
                    .Select(o => o.Key)
                    .Where(k => k.EndsWith(".ndjson", StringComparison.Ordinal)));

            token = response.IsTruncated == true ? response.NextContinuationToken : null;
        }
        while (token is not null);

        return keys;
    }
}
