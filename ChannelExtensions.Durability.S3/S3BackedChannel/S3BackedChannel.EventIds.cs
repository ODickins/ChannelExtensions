using Microsoft.Extensions.Logging;

namespace ChannelExtensions.Durability.S3.S3BackedChannel;

internal static class S3BackedChannelEventIds
{
    // Spill lifecycle.
    public static readonly EventId SpillCompleted = new(1, nameof(SpillCompleted));
    public static readonly EventId SpillStarted = new(2, nameof(SpillStarted));

    // Startup.
    public static readonly EventId BacklogRecovered = new(3, nameof(BacklogRecovered));

    // Write / upload path.
    public static readonly EventId ChunkUploadFailed = new(4, nameof(ChunkUploadFailed));

    // Read / replay path.
    public static readonly EventId RecordDeserializeFailed = new(5, nameof(RecordDeserializeFailed));
    public static readonly EventId ObjectReplayFailed = new(6, nameof(ObjectReplayFailed));

    // Object operations.
    public static readonly EventId QuarantineFailed = new(7, nameof(QuarantineFailed));
    public static readonly EventId ObjectDeleteFailed = new(8, nameof(ObjectDeleteFailed));

    // Drain loop lifecycle.
    public static readonly EventId DrainLoopFaulted = new(9, nameof(DrainLoopFaulted));
    public static readonly EventId DrainStopUnclean = new(10, nameof(DrainStopUnclean));
}
