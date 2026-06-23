using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace ChannelExtensions.Durability.FileSystem.FileBackedChannel;

public sealed partial class FileBackedChannel<T>
{
    /// <summary>
    /// Reads the checkpoint value from the specified file path.
    /// If the checkpoint file exists and contains a valid positive long value,
    /// it returns that value. Otherwise, returns 0.
    /// </summary>
    /// <param name="path">The file path of the checkpoint file to read.</param>
    /// <returns>
    /// The checkpoint value if the file exists, contains a valid value, and the value is positive;
    /// otherwise, returns 0.
    /// </returns>
    private static long ReadCheckpoint(string path)
    {
        try
        {
            return File.Exists(path) && long.TryParse(File.ReadAllText(path).Trim(), out var n) && n > 0
                ? n
                : 0;
        }
        catch
        {
            // Unreadable checkpoint -> treat as no progress (may replay this block).
            return 0;
        }
    }

    /// <summary>
    /// Writes the checkpoint value to the specified file stream asynchronously.
    /// The method updates the checkpoint file with the new emitted value, ensuring
    /// that progress can be resumed accurately in case of a failure or restart.
    /// </summary>
    /// <param name="checkpoint">
    /// The file stream representing the checkpoint file to be updated.
    /// </param>
    /// <param name="emitted">
    /// The checkpoint value representing the number of processed items to be written to the file.
    /// </param>
    /// <param name="cancellationToken">
    /// A cancellation token that can be used to cancel the asynchronous operation.
    /// </param>
    /// <returns>
    /// A task that represents the asynchronous write operation.
    /// </returns>
    private static async Task WriteCheckpointAsync(
        FileStream checkpoint, long emitted, CancellationToken cancellationToken)
    {
        var bytes = System.Text.Encoding.ASCII.GetBytes(emitted.ToString("D20"));
        checkpoint.Seek(0, SeekOrigin.Begin);
        await checkpoint.WriteAsync(bytes, cancellationToken);
        await checkpoint.FlushAsync(cancellationToken);
    }

    private async Task ReadBufferAsync(CancellationToken cancellationToken = default)
    {
        // Get the writer for the publisher, we will be writing the spilled values into it.
        var writer = _publisher.Writer;

        // Do work until we're told to stop.
        while (!cancellationToken.IsCancellationRequested)
        {
            // Wait for the next pending block. This blocks on the in-memory queue rather than polling
            // the directory - paths are produced by the write loop and by the one-time startup scan.
            string next;
            try
            {
                if (!await _pendingPaths.Reader.WaitToReadAsync(cancellationToken))
                    break;

                if (!_pendingPaths.Reader.TryRead(out next!))
                    continue;
            }
            catch (OperationCanceledException)
            {
                break;
            }

            // We have a file to process, get the total number of records in it from the file name.
            var fileTotal = ParseRecordCount(next);

            // Create a checkpoint file to track how many records we've processed (we don't want to replay on a crash).
            var checkpointPath = next + ".ckpt";

            // Read the checkpoint if we have one (how many records we've processed).
            var emitted = ReadCheckpoint(checkpointPath);
            var seen = 0L;
            var processed = 0L;

            try
            {
                // Open the file for reading and start processing.
                await using (var checkpoint = new FileStream(
                                 checkpointPath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None,
                                 bufferSize: 64, FileOptions.WriteThrough | FileOptions.Asynchronous))
                {
                    // Read the file line by line.
                    await foreach (var line in File.ReadLinesAsync(next, cancellationToken))
                    {
                        // If that line is empty, skip it.
                        if (string.IsNullOrEmpty(line))
                            continue;

                        // If this line has already been processed, skip it.
                        if (seen < emitted)
                        {
                            seen++;
                            processed++;
                            ReleasePending(1);
                            continue;
                        }

                        // Deserialize the line into the target type.
                        T? item;
                        try
                        {
                            item = JsonSerializer.Deserialize<T>(line, _options.JsonSerializerOptions);
                        }
                        catch (Exception ex)
                        {
                            // If it could not deserialize, discard it - but log the error.
                            _logger.LogError(
                                FileBackedChannelEventIds.RecordDeserializeFailed, ex,
                                "Failed to deserialize record in {File}; discarding.", next);
                            seen++;
                            processed++;
                            ReleasePending(1);
                            continue;
                        }

                        // If the item is null, skip it.
                        if (item is null)
                        {
                            seen++;
                            processed++;
                            ReleasePending(1);
                            continue;
                        }

                        // Write to the checkpoint file our new position; this ensures that we do not replay on an application crash.
                        await WriteCheckpointAsync(checkpoint, seen + 1, cancellationToken);

                        // Write into the publisher channel, WriteAsync will block until we have space to add it.
                        await writer.WriteAsync(item, cancellationToken);

                        seen++;
                        processed++;

                        // Release a pending item, decrementing the count.
                        ReleasePending(1);
                    }
                }

                File.Delete(next);
                TryDelete(checkpointPath);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Shutdown: stop the loop, leave the file (and checkpoint) for restart.
                throw;
            }
            catch (Exception ex)
            {
                // Corrupt/unreadable block. Quarantine it so we don't spin on it,
                // and release the records we never replayed to keep the count sane.
                _logger.LogError(
                    FileBackedChannelEventIds.BlockReplayFailed, ex,
                    "Failed to replay block {File}; quarantining.", next);

                // On an exception, decrement the pending count for the entire file.
                ReleasePending(fileTotal - processed);

                // Quarantine the file, so an administrator can inspect it.
                Quarantine(next);

                // Delete the checkpoint file, we do not need it anymore.
                TryDelete(checkpointPath);
            }
        }
    }
}