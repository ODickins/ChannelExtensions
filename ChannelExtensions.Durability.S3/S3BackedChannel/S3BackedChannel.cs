using System.Threading.Channels;
using Amazon.S3;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChannelExtensions.Durability.S3.S3BackedChannel;

public sealed partial class S3BackedChannel<T> : Channel<T>, IDisposable
{
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly Task _drainTask;

    private readonly S3BackedChannelOptions _options;
    private readonly ILogger _logger;
    private readonly IAmazonS3 _s3;

    private readonly Channel<T> _publisher;

    // Overflow items waiting to be batched into a chunk and uploaded. Held entirely in memory -
    // nothing is staged to local disk.
    private readonly Channel<T> _uploadBuffer;

    // The pending object keys awaiting replay, in chronological order. Seeded once from a single
    // S3 listing at startup, then maintained purely in memory: the write loop appends a key after
    // each upload and the read loop consumes them. The reader blocks on this channel, so S3 is
    // never polled for "is there more work?".
    private readonly Channel<string> _pendingKeys;

    private readonly Lock _gate = new();
    private bool _spilling;
    private long _pendingCount;

    /// <summary>
    /// Creates a durable, S3-backed channel that buffers in-memory overflow, uploads committed
    /// NDJSON chunks to S3, and replays them in order across restarts.
    /// </summary>
    /// <param name="options">The configuration for the channel's capacity, bucket/prefix, client, and behavior.</param>
    public S3BackedChannel(S3BackedChannelOptions options)
    {
        // Set basic options, used throughout the channel's lifecycle.
        _options = options;
        _logger = options.Logger ?? NullLogger.Instance;
        _s3 = options.Client ?? throw new ArgumentException("An S3 client must be provided.", nameof(options));

        var publisherOptions = new BoundedChannelOptions(options.Capacity)
        {
            SingleWriter = false,
            SingleReader = options.SingleReader,
            AllowSynchronousContinuations = options.AllowSynchronousContinuations,
            FullMode = BoundedChannelFullMode.Wait
        };

        // Create the publisher channel, bounded to the specified capacity.
        _publisher = Channel.CreateBounded<T>(publisherOptions);

        // Create the upload buffer, unbounded to allow for exponential growth - batched into chunks
        // and uploaded when pressure on the publisher channel is detected.
        _uploadBuffer = Channel.CreateUnbounded<T>();

        // The in-memory queue of pending S3 keys. Single reader (the replay loop); the write loop is the writer.
        _pendingKeys = Channel.CreateUnbounded<string>(new UnboundedChannelOptions { SingleReader = true });

        // Expose the reader from the publisher channel, allowing for reading from the channel.
        Reader = _publisher.Reader;

        // Expose the writer, wrapped in a spill writer to handle buffering overflow for upload.
        Writer = new SpillWriter(this);

        // One-time S3 read on first start: list the existing committed chunk objects under the
        // prefix and seed the pending-key queue (oldest first). This is the only time we list S3.
        SeedPendingKeysFromS3();

        // Start in spill mode if there is a backlog in S3, so new writes queue behind it and ordering
        // is preserved across the restart.
        _spilling = _pendingCount > 0;

        if (_spilling)
        {
            _logger.LogInformation(
                S3BackedChannelEventIds.BacklogRecovered,
                "Recovered {Count} pending record(s) from s3://{Bucket}/{Prefix}; starting in spill mode.",
                _pendingCount, _options.Bucket, _options.Prefix);
        }

        // Create a cancellation token source to manage the lifecycle of the durable channel - on disposal, this is canceled.
        _cancellationTokenSource = new CancellationTokenSource();

        // Start the read/write tasks to handle uploading chunks and replaying them from S3.
        _drainTask = Task.WhenAll(
            RunResilientAsync(WriteBufferAsync, nameof(WriteBufferAsync), _cancellationTokenSource.Token),
            RunResilientAsync(ReadBufferAsync, nameof(ReadBufferAsync), _cancellationTokenSource.Token)
        );
    }

    /// <summary>
    /// Executes a given asynchronous loop resiliently: cancellation on shutdown is swallowed, and
    /// any other exception is logged (not rethrown) so a fault halts durability quietly rather than
    /// taking down the host.
    /// </summary>
    private async Task RunResilientAsync(
        Func<CancellationToken, Task> innerTask, string name, CancellationToken cancellationToken)
    {
        try
        {
            await innerTask(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Normal shutdown via Dispose.
        }
        catch (Exception ex)
        {
            _logger.LogCritical(
                S3BackedChannelEventIds.DrainLoopFaulted, ex,
                "Durable channel {Loop} terminated unexpectedly; durability halted.", name);
        }
    }

    public void Dispose()
    {
        _cancellationTokenSource.Cancel();

        try
        {
            _drainTask.Wait(_options.CommitInterval);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                S3BackedChannelEventIds.DrainStopUnclean, ex, "Durable channel drain did not stop cleanly.");
        }

        _cancellationTokenSource.Dispose();
    }
}
