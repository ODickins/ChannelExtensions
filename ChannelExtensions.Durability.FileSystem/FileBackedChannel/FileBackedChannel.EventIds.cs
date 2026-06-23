using Microsoft.Extensions.Logging;

namespace ChannelExtensions.Durability.FileSystem.FileBackedChannel;

internal static class FileBackedChannelEventIds
{
    // Spill lifecycle.
    public static readonly EventId SpillCompleted = new(1, nameof(SpillCompleted));
    public static readonly EventId SpillStarted = new(2, nameof(SpillStarted));

    // Startup / recovery.
    public static readonly EventId BacklogRecovered = new(3, nameof(BacklogRecovered));
    public static readonly EventId OrphanedBlockRecoveryFailed = new(4, nameof(OrphanedBlockRecoveryFailed));

    // Write path.
    public static readonly EventId BlockCommitFailed = new(5, nameof(BlockCommitFailed));

    // Read path.
    public static readonly EventId RecordDeserializeFailed = new(6, nameof(RecordDeserializeFailed));
    public static readonly EventId BlockReplayFailed = new(7, nameof(BlockReplayFailed));

    // File operations.
    public static readonly EventId QuarantineFailed = new(8, nameof(QuarantineFailed));
    public static readonly EventId DeleteFailed = new(9, nameof(DeleteFailed));

    // Drain loop lifecycle.
    public static readonly EventId DrainLoopFaulted = new(10, nameof(DrainLoopFaulted));
    public static readonly EventId DrainStopUnclean = new(11, nameof(DrainStopUnclean));
}
