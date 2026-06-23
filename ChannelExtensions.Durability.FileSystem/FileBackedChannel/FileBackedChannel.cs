using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChannelExtensions.Durability.FileSystem.FileBackedChannel;

public sealed partial class FileBackedChannel<T> : Channel<T>, IDisposable
{
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly Task _drainTask;

    private readonly FileBackedChannelOptions _options;
    private readonly ILogger _logger;
    private readonly Channel<T> _publisher;
    private readonly Channel<T> _diskBuffer;

    private readonly Lock _gate = new();
    private bool _spilling;
    private long _pendingDiskCount;

    /// <summary>
    /// Creates a durable, file-backed channel that persists in-memory overflow to disk,
    /// allowing data recovery and resiliency across restarts.
    /// </summary>
    /// <param name="options">The configuration for the channel's capacity, paths, and behavior.</param>
    public FileBackedChannel(FileBackedChannelOptions options)
    {
        // Set basic options, used throughout the channel's lifecycle.
        _options = options;
        _logger = options.Logger ?? NullLogger.Instance;

        var publisherOptions = new BoundedChannelOptions(options.Capacity)
        {
            SingleWriter = false,
            SingleReader = options.SingleReader,
            AllowSynchronousContinuations = options.AllowSynchronousContinuations,
            FullMode = BoundedChannelFullMode.Wait
        };

        // Create the publisher channel, bounded to the specified capacity.
        _publisher = Channel.CreateBounded<T>(publisherOptions);
        
        // Create a disk buffer channel, unbounded to allow for exponential growth - flushed to disk when pressure on the publisher channel is detected.
        _diskBuffer = Channel.CreateUnbounded<T>();

        // Expose the reader from the publisher channel, allowing for reading from the channel.
        Reader = _publisher.Reader;
        
        // Expose the writer, wrapped in a spill writer to handle spilling to disk.
        Writer = new SpillWriter(this);

        // Before we start, recover any temporary blocks from disk (blocks which were not closed gracefully from an application crash)
        RecoverTempBlocks();

        // Count the number of committed records in the disk buffer, used to determine if we need to start in spill mode.
        _pendingDiskCount = CountCommittedRecords();
        
        // Start in spill mode if there are any committed records in the disk buffer, ensures we read back in order.
        _spilling = _pendingDiskCount > 0;

        // If we are spilling, log that we are starting in spill mode.
        if (_spilling)
        {
            _logger.LogInformation(
                FileBackedChannelEventIds.BacklogRecovered,
                "Recovered {Count} buffered record(s) from {Path}; starting in spill mode.",
                _pendingDiskCount, _options.Path);
        }

        // Create a cancellation token source to manage the lifecycle of the durable channel - on disposal, this is canceled.
        _cancellationTokenSource = new CancellationTokenSource();

        // Start the read/write tasks to handle spilling to disk and reading from disk.
        _drainTask = Task.WhenAll(
            RunResilientAsync(WriteBufferAsync, nameof(WriteBufferAsync), _cancellationTokenSource.Token),
            RunResilientAsync(ReadBufferAsync, nameof(ReadBufferAsync), _cancellationTokenSource.Token)
        );
    }

    /// <summary>
    /// Executes a given asynchronous task within a resilient loop, ensuring that exceptions are handled gracefully.
    /// Logs critical errors and prevents the task from terminating unexpectedly.
    /// </summary>
    /// <param name="innerTask">
    /// The asynchronous task to be executed. This task represents the core logic to be run within the resilient loop.
    /// </param>
    /// <param name="name">
    /// A user-defined identifier for the loop, primarily used for logging purposes to distinguish between different loops.
    /// </param>
    /// <param name="cancellationToken">
    /// A token to observe while waiting for the task to complete. If cancellation is requested, the task will stop executing normally.
    /// </param>
    /// <returns>
    /// A task representing the lifecycle of the resilient loop, monitoring the execution of the provided task and handling
    /// any unhandled exceptions that occur.
    /// </returns>
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
                FileBackedChannelEventIds.DrainLoopFaulted, ex,
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
                FileBackedChannelEventIds.DrainStopUnclean, ex, "Durable channel drain did not stop cleanly.");
        }

        _cancellationTokenSource.Dispose();
    }
}