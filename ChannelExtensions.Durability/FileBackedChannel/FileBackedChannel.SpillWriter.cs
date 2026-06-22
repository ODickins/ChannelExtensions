using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace ChannelExtensions.Durability.FileBackedChannel;

public sealed partial class FileBackedChannel<T>
{
    private sealed class SpillWriter(FileBackedChannel<T> owner) : ChannelWriter<T>
    {
        public override bool TryWrite(T item)
        {
            // Async lock
            lock (owner._gate)
            {
                // If we are not spilling, try and go direct to the publisher.
                if (!owner._spilling)
                {
                    if (owner._publisher.Writer.TryWrite(item))
                    {
                        // If that worked, report it worked.
                        return true;
                    }

                    // If it failed, the channel is full - we need to start spilling to disk.
                    owner._spilling = true;

                    // Log a warning indicating that the channel is full and spilling to disk has started.
                    owner._logger.LogWarning(
                        FileBackedChannelEventIds.SpillStarted, "Channel is full, starting spill to disk.");
                }

                // Increment the pending count.
                owner._pendingDiskCount++;

                // Write to the disk buffer.
                return owner._diskBuffer.Writer.TryWrite(item);
            }
        }

        // Disk buffer is unbounded, so a slot is always available while open;
        // the base WriteAsync loop relies on this never reporting "full".
        public override ValueTask<bool> WaitToWriteAsync(CancellationToken cancellationToken = default)
            => owner._diskBuffer.Writer.WaitToWriteAsync(cancellationToken);

        public override bool TryComplete(Exception? error = null)
            => owner._diskBuffer.Writer.TryComplete(error);
    }
}