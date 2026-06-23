using Amazon.S3.Model;
using Microsoft.Extensions.Logging;

namespace ChannelExtensions.Durability.S3.S3BackedChannel;

public sealed partial class S3BackedChannel<T>
{
    /// <summary>
    /// Decrements the count of pending records and reverts to direct mode once none remain.
    /// </summary>
    /// <param name="records">The number of records to release from the pending count.</param>
    private void ReleasePending(long records)
    {
        lock (_gate)
        {
            _pendingCount -= records;

            if (_pendingCount <= 0)
            {
                _pendingCount = 0;

                if (_spilling)
                    _logger.LogInformation(
                        S3BackedChannelEventIds.SpillCompleted, "Spill completed; reverting to direct mode.");

                _spilling = false;
            }
        }
    }

    /// <summary>
    /// Performs the single startup listing of the bucket under the prefix, seeding the pending-key
    /// queue (oldest first) and the pending record count. This is the only time S3 is listed; after
    /// this, pending keys are tracked entirely in memory.
    /// </summary>
    private void SeedPendingKeysFromS3()
    {
        // Keys we create are "{Prefix}/{NodeId}.{guid}.{count}.ndjson"; list under
        // "{Prefix}/{NodeId}." (or just "{NodeId}." when there is no prefix). Scoping the listing to
        // our node id means we never see, list, or replay another node's chunks in a shared bucket.
        var listPrefix = string.IsNullOrEmpty(_options.Prefix)
            ? _options.NodeId + "."
            : $"{_options.Prefix}/{_options.NodeId}.";
        var keys = new List<string>();
        string? continuationToken = null;

        do
        {
            var response = _s3.ListObjectsV2Async(
                new ListObjectsV2Request
                {
                    BucketName = _options.Bucket,
                    Prefix = listPrefix,
                    ContinuationToken = continuationToken
                }).GetAwaiter().GetResult();

            if (response.S3Objects is not null)
            {
                foreach (var s3Object in response.S3Objects)
                {
                    // Only committed chunks; ".corrupt" quarantined objects are intentionally excluded.
                    if (s3Object.Key.EndsWith(".ndjson", StringComparison.Ordinal))
                        keys.Add(s3Object.Key);
                }
            }

            continuationToken = response.IsTruncated == true ? response.NextContinuationToken : null;
        }
        while (continuationToken is not null);

        // Ordinal sort on the keys orders them chronologically via the time-ordered v7 guid prefix.
        keys.Sort(StringComparer.Ordinal);

        foreach (var key in keys)
        {
            _pendingKeys.Writer.TryWrite(key);
            _pendingCount += ParseRecordCount(key);
        }
    }

    /// <summary>Builds the S3 object key for a chunk: <c>{Prefix}/{NodeId}.{guid}.{count}.ndjson</c>.</summary>
    private string BuildKey(string guid, long count)
    {
        var fileName = $"{_options.NodeId}.{guid}.{count}.ndjson";
        return string.IsNullOrEmpty(_options.Prefix) ? fileName : $"{_options.Prefix}/{fileName}";
    }

    /// <summary>
    /// Parses the record count from an object key of the form <c>{prefix}/{guid}.{count}.ndjson</c>.
    /// </summary>
    private static long ParseRecordCount(string key)
    {
        var fileName = key[(key.LastIndexOf('/') + 1)..]; // {guid}.{count}.ndjson
        var name = Path.GetFileNameWithoutExtension(fileName); // {guid}.{count}
        var dot = name.LastIndexOf('.');
        return dot >= 0 && long.TryParse(name.AsSpan(dot + 1), out var count) ? count : 0;
    }

    /// <summary>
    /// Sets aside a corrupt or unrecoverable S3 object. By default it is copied to a sibling
    /// ".corrupt" key (which the startup listing ignores) and the original deleted, so it can be
    /// inspected. When <see cref="S3BackedChannelOptions.QuarantineCorruptObjects"/> is disabled,
    /// the object is deleted instead.
    /// </summary>
    private async Task QuarantineObjectAsync(string key, CancellationToken cancellationToken)
    {
        if (!_options.QuarantineCorruptObjects)
        {
            await DeleteObjectAsync(key, cancellationToken);
            return;
        }

        try
        {
            // ".corrupt" no longer ends with ".ndjson", so the next startup listing won't retry it.
            await _s3.CopyObjectAsync(
                new CopyObjectRequest
                {
                    SourceBucket = _options.Bucket,
                    SourceKey = key,
                    DestinationBucket = _options.Bucket,
                    DestinationKey = key + ".corrupt"
                }, cancellationToken);

            await _s3.DeleteObjectAsync(_options.Bucket, key, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                S3BackedChannelEventIds.QuarantineFailed, ex, "Failed to quarantine object {Key}.", key);
        }
    }

    /// <summary>Deletes an S3 object, logging (not throwing) on failure.</summary>
    private async Task DeleteObjectAsync(string key, CancellationToken cancellationToken)
    {
        try
        {
            await _s3.DeleteObjectAsync(_options.Bucket, key, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                S3BackedChannelEventIds.ObjectDeleteFailed, ex, "Failed to delete object {Key}.", key);
        }
    }
}
