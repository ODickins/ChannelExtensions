using System.Text;
using System.Text.Json;
using Amazon.S3.Model;
using Microsoft.Extensions.Logging;

namespace ChannelExtensions.Durability.S3.S3BackedChannel;

public sealed partial class S3BackedChannel<T>
{
    private async Task WriteBufferAsync(CancellationToken cancellationToken = default)
    {
        // Register a cancellation callback to ensure the buffer is drained on shutdown.
        await using var registration = cancellationToken.Register(() => _uploadBuffer.Writer.TryComplete());

        // Drain the buffer until the writer is completed (after the last item); keep draining even after application cancellation.
        while (await _uploadBuffer.Reader.WaitToReadAsync(CancellationToken.None))
        {
            // Accumulate a chunk entirely in memory - nothing touches local disk. The guid v7 name
            // is time-ordered and reused as the S3 key, so listing the bucket sorts chunks chronologically.
            var guid = Guid.CreateVersion7().ToString("N");
            var payload = new MemoryStream();
            var count = 0;

            // NDJSON: one JSON record per line, UTF-8 with no BOM.
            await using (var writer = new StreamWriter(payload, new UTF8Encoding(false), bufferSize: 1024, leaveOpen: true))
            {
                writer.NewLine = "\n";

                // Commit by a timer: if back-pressure ends and the chunk isn't full, it is still
                // uploaded once the window elapses.
                using var window = new CancellationTokenSource(_options.CommitInterval);
                try
                {
                    await foreach (var item in _uploadBuffer.Reader.ReadAllAsync(window.Token))
                    {
                        await writer.WriteLineAsync(JsonSerializer.Serialize(item, _options.JsonSerializerOptions));

                        // Upload as soon as the chunk is full.
                        if (++count >= _options.MaxChunkSize)
                            break;
                    }
                }
                catch (OperationCanceledException)
                {
                    // Commit window has elapsed, upload this batch below.
                }

                await writer.FlushAsync(CancellationToken.None);
            }

            // The window can elapse with nothing buffered; don't upload an empty chunk.
            if (count == 0)
            {
                await payload.DisposeAsync();
                continue;
            }

            var key = BuildKey(guid, count);
            try
            {
                // Single PutObject straight from memory. CancellationToken.None: the chunk is already
                // serialized and represents durable data, so finish the upload even if the host is
                // shutting down (Dispose waits up to CommitInterval for this).
                payload.Position = 0;
                await _s3.PutObjectAsync(
                    new PutObjectRequest
                    {
                        BucketName = _options.Bucket,
                        Key = key,
                        InputStream = payload,
                        AutoCloseStream = false,
                        ContentType = "application/x-ndjson"
                    },
                    CancellationToken.None);

                // Hand the key to the replay loop. The queue is unbounded, so this always succeeds.
                _pendingKeys.Writer.TryWrite(key);
            }
            catch (Exception ex)
            {
                // Upload failed; this chunk could not be made durable. Log and release the records so
                // the spill state stays consistent. The buffered items are lost (they were never on disk).
                _logger.LogError(
                    S3BackedChannelEventIds.ChunkUploadFailed, ex,
                    "Failed to upload chunk to s3://{Bucket}/{Prefix}; {Count} item(s) lost.",
                    _options.Bucket, _options.Prefix, count);

                ReleasePending(count);
            }
            finally
            {
                await payload.DisposeAsync();
            }
        }
    }
}
