using Microsoft.Extensions.Logging;

namespace ChannelExtensions.Durability.FileSystem.FileBackedChannel;

public sealed partial class FileBackedChannel<T>
{
    /// <summary>
    /// Decrements the count of pending disk writes and resets the spillover state if no pending writes remain.
    /// </summary>
    /// <param name="records">The number of records to release from the pending disk write count.</param>
    private void ReleasePending(long records)
    {
        lock (_gate)
        {
            _pendingDiskCount -= records;

            if (_pendingDiskCount <= 0)
            {
                _pendingDiskCount = 0;

                if (_spilling)
                    _logger.LogInformation(
                        FileBackedChannelEventIds.SpillCompleted, "Spill completed; reverting to direct mode.");

                _spilling = false;
            }
        }
    }

    /// <summary>
    /// Recovers temporary block files left in the channel's designated path due to
    /// improper shutdown or crashes. Any orphaned temporary files are either renamed
    /// to a committed format based on their content or deleted if empty. If an error occurs
    /// during processing, the problematic file is quarantined to prevent it from
    /// interfering with the channel's operation.
    /// </summary>
    private void RecoverTempBlocks()
    {
        foreach (var tmpPath in Directory.EnumerateFiles(_options.Path, "*.tmp"))
        {
            try
            {
                TruncateToLastNewline(tmpPath);

                // Orphaned .tmp has no count in its name; count its lines (rare, few
                // files) so it joins the committed naming scheme. Drop if empty.
                var count = CountLines(tmpPath);
                if (count == 0)
                {
                    File.Delete(tmpPath);
                    continue;
                }

                var committedPath = Path.Combine(
                    _options.Path, $"{Path.GetFileNameWithoutExtension(tmpPath)}.{count}.ndjson");
                File.Move(tmpPath, committedPath, overwrite: true);
            }
            catch (Exception ex)
            {
                // Don't let one unreadable leftover block channel construction.
                _logger.LogError(
                    FileBackedChannelEventIds.OrphanedBlockRecoveryFailed, ex,
                    "Failed to recover orphaned block {File}; quarantining.", tmpPath);
                Quarantine(tmpPath);
            }
        }
    }

    /// <summary>
    /// Counts the number of non-empty lines in the specified file.
    /// </summary>
    /// <param name="path">The path to the file whose lines should be counted.</param>
    /// <returns>The total number of non-empty lines in the file.</returns>
    private static long CountLines(string path)
    {
        long count = 0;
        foreach (var line in File.ReadLines(path))
        {
            if (!string.IsNullOrEmpty(line))
            {
                count++;
            }
        }

        return count;
    }

    /// <summary>
    /// Calculates the total number of committed records from all files in the specified directory matching the "*.ndjson" pattern.
    /// </summary>
    /// <returns>The cumulative count of committed records found in the disk buffer directory.</returns>
    private long CountCommittedRecords()
    {
        long total = 0;
        foreach (var file in Directory.EnumerateFiles(_options.Path, "*.ndjson"))
        {
            total += ParseRecordCount(file);
        }

        return total;
    }

    /// <summary>
    /// Parses and returns the record count from the specified file path.
    /// Assumes the file name format to be {guid}.{count}.
    /// </summary>
    /// <param name="path">The full file path to parse for the record count.</param>
    /// <returns>The extracted record count from the file name, or 0 if the format is invalid.</returns>
    private static long ParseRecordCount(string path)
    {
        var name = Path.GetFileNameWithoutExtension(path); // {guid}.{count}
        var dot = name.LastIndexOf('.');
        return dot >= 0 && long.TryParse(name.AsSpan(dot + 1), out var count) ? count : 0;
    }

    /// <summary>
    /// Truncates the specified file to remove any partial record content after the last newline character.
    /// </summary>
    /// <param name="path">The path to the file that needs to be truncated.</param>
    private static void TruncateToLastNewline(string path)
    {
        using var stream = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        var length = stream.Length;
        if (length == 0)
        {
            return;
        }

        // Scan backwards for the last '\n' (records are newline-terminated).
        var buffer = new byte[8192];
        var position = length;
        long lastNewline = -1;

        while (position > 0 && lastNewline < 0)
        {
            var chunk = (int)Math.Min(buffer.Length, position);
            position -= chunk;
            stream.Seek(position, SeekOrigin.Begin);
            stream.ReadExactly(buffer, 0, chunk);

            for (var i = chunk - 1; i >= 0; i--)
            {
                if (buffer[i] == (byte)'\n')
                {
                    lastNewline = position + i;
                    break;
                }
            }
        }

        // No complete record -> empty file; otherwise drop the partial tail.
        stream.SetLength(lastNewline < 0 ? 0 : lastNewline + 1);
    }

    /// <summary>
    /// Sets aside a corrupt or unrecoverable block. By default it is renamed with a ".corrupt"
    /// suffix (excluding it from the "*.ndjson" scan) so it can be inspected. When
    /// <see cref="FileBackedChannelOptions.QuarantineCorruptBlocks"/> is disabled, the block is
    /// deleted instead.
    /// </summary>
    /// <param name="path">The full path of the file to set aside.</param>
    private void Quarantine(string path)
    {
        // Quarantining disabled -> just delete the file, keeping the directory self-cleaning.
        if (!_options.QuarantineCorruptBlocks)
        {
            TryDelete(path);
            return;
        }

        try
        {
            // .corrupt no longer matches the *.ndjson scan, so it won't be retried.
            File.Move(path, path + ".corrupt", overwrite: true);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                FileBackedChannelEventIds.QuarantineFailed, ex, "Failed to quarantine {Path}.", path);
        }
    }

    /// <summary>
    /// Attempts to delete the specified file path. Logs a warning if the deletion fails.
    /// </summary>
    /// <param name="path">The file path to delete.</param>
    private void TryDelete(string path)
    {
        try
        {
            File.Delete(path);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                FileBackedChannelEventIds.DeleteFailed, ex, "Failed to delete {Path}.", path);
        }
    }
}