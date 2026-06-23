using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace ChannelExtensions.Durability.S3.S3BackedChannel;

public sealed partial class S3BackedChannel<T>
{
    private sealed class SpillWriter(S3BackedChannel<T> owner) : ChannelWriter<T>
    {
        public override bool TryWrite(T item)
        {
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

                    // If it failed, the channel is full - we need to start buffering for upload.
                    owner._spilling = true;

                    owner._logger.LogWarning(
                        S3BackedChannelEventIds.SpillStarted, "Channel is full, buffering overflow for upload to S3.");
                }

                // Increment the pending count.
                owner._pendingCount++;

                // Write to the upload buffer; the write loop batches it into a chunk and uploads it.
                return owner._uploadBuffer.Writer.TryWrite(item);
            }
        }

        // Upload buffer is unbounded, so a slot is always available while open;
        // the base WriteAsync loop relies on this never reporting "full".
        public override ValueTask<bool> WaitToWriteAsync(CancellationToken cancellationToken = default)
            => owner._uploadBuffer.Writer.WaitToWriteAsync(cancellationToken);

        public override bool TryComplete(Exception? error = null)
            => owner._uploadBuffer.Writer.TryComplete(error);
    }
}
