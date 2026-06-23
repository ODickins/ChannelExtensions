using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace ChannelExtensions.Durability.S3.S3BackedChannel;

public sealed partial class S3BackedChannel<T>
{
    private async Task ReadBufferAsync(CancellationToken cancellationToken = default)
    {
        // Get the writer for the publisher, we will be writing the replayed values into it.
        var writer = _publisher.Writer;

        // Do work until we're told to stop.
        while (!cancellationToken.IsCancellationRequested)
        {
            // Wait for the next pending key. This blocks on the in-memory queue rather than polling
            // S3 - keys are produced by the write loop and by the one-time startup listing.
            string key;
            try
            {
                if (!await _pendingKeys.Reader.WaitToReadAsync(cancellationToken))
                    break;

                if (!_pendingKeys.Reader.TryRead(out key!))
                    continue;
            }
            catch (OperationCanceledException)
            {
                break;
            }

            // The total record count is encoded in the key; used to reconcile the pending count on failure.
            var fileTotal = ParseRecordCount(key);
            var processed = 0L;

            try
            {
                // Stream the object straight from S3 and replay it line by line.
                using var response = await _s3.GetObjectAsync(_options.Bucket, key, cancellationToken);
                await using var body = response.ResponseStream;
                using var reader = new StreamReader(body);

                while (await reader.ReadLineAsync(cancellationToken) is { } line)
                {
                    // If that line is empty, skip it.
                    if (string.IsNullOrEmpty(line))
                        continue;

                    T? item;
                    try
                    {
                        item = JsonSerializer.Deserialize<T>(line, _options.JsonSerializerOptions);
                    }
                    catch (Exception ex)
                    {
                        // If it could not deserialize, discard it - but log the error.
                        _logger.LogError(
                            S3BackedChannelEventIds.RecordDeserializeFailed, ex,
                            "Failed to deserialize record in {Key}; discarding.", key);
                        processed++;
                        ReleasePending(1);
                        continue;
                    }

                    if (item is null)
                    {
                        processed++;
                        ReleasePending(1);
                        continue;
                    }

                    // Write into the publisher channel, WriteAsync blocks until there is space.
                    await writer.WriteAsync(item, cancellationToken);

                    processed++;
                    ReleasePending(1);
                }

                // Fully replayed: delete the object from S3.
                await DeleteObjectAsync(key, CancellationToken.None);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Shutdown: stop the loop. The object remains in S3, so a restart rediscovers it via
                // the startup listing and replays it again (at-least-once at the replay boundary).
                throw;
            }
            catch (Exception ex)
            {
                // Corrupt/unreadable object. Quarantine it so we don't spin on it, and release the
                // records we never replayed to keep the pending count sane.
                _logger.LogError(
                    S3BackedChannelEventIds.ObjectReplayFailed, ex,
                    "Failed to replay object {Key}; quarantining.", key);

                ReleasePending(fileTotal - processed);

                await QuarantineObjectAsync(key, cancellationToken);
            }
        }
    }
}
