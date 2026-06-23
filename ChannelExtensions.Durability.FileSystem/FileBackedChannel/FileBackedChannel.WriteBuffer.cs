using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace ChannelExtensions.Durability.FileSystem.FileBackedChannel;

public sealed partial class FileBackedChannel<T>
{
    private async Task WriteBufferAsync(CancellationToken cancellationToken = default)
    {
        // Register a cancellation callback to ensure the buffer is drained.
        await using var registration = cancellationToken.Register(() => _diskBuffer.Writer.TryComplete());

        // Drain the buffer until the reader is closed (after the last item is written), we keep draining even after application cancellation.
        while (await _diskBuffer.Reader.WaitToReadAsync(CancellationToken.None))
        {
            // Create a temporary file to write to. Using this name, on app start, we can scan for any half-written files to recover them.
            var tmpPath = Path.Combine(_options.Path, $"{Guid.CreateVersion7():N}.tmp");
            var count = 0;

            try
            {
                await using (var stream = new FileStream(
                                 tmpPath, FileMode.CreateNew, FileAccess.Write, FileShare.None,
                                 // Write through flag tells the OS to flush the buffer to disk immediately
                                 bufferSize: 4096, FileOptions.WriteThrough | FileOptions.Asynchronous))

                await using (var writer = new StreamWriter(stream))
                {
                    // Use newline-delimited JSON for the disk format.
                    writer.NewLine = "\n";

                    // Auto-flush the stream to disk every commit interval.
                    writer.AutoFlush = true;

                    // Commit by a timer, effectively - if the back pressure ends and nothing is written, the pending items will be committed even in a file with only a single record.
                    using var window = new CancellationTokenSource(_options.CommitInterval);
                    try
                    {
                        await foreach (var item in _diskBuffer.Reader.ReadAllAsync(window.Token))
                        {
                            await writer.WriteLineAsync(JsonSerializer.Serialize(item, _options.JsonSerializerOptions));

                            // Commit early once the block is full.
                            if (++count >= _options.MaxBlockSize)
                                break;
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // Commit window has elapsed, commit this batch below.
                    }
                }

                // The commit path is the same name, with the count of items in the file as "ndjson".
                var committedPath = Path.Combine(_options.Path, $"{Path.GetFileNameWithoutExtension(tmpPath)}.{count}.ndjson");

                // Move the file, this is an atomic 99% of the time depending on the OS and filesystem mind.
                File.Move(tmpPath, committedPath);
            }
            catch (Exception ex)
            {
                // Something went wrong committing the file, log the error and release the pending items.
                _logger.LogError(
                    FileBackedChannelEventIds.BlockCommitFailed, ex,
                    "Failed to commit disk block; {Count} item(s) lost.", count);

                // Get rid of the temp file if it exists.
                TryDelete(tmpPath);

                // Release the pending items.
                ReleasePending(count);
            }
        }
    }
}