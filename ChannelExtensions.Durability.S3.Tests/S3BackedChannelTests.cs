using System.Threading.Channels;
using ChannelExtensions.Durability.S3;
using static ChannelExtensions.Durability.S3.Tests.TestHelpers;

namespace ChannelExtensions.Durability.S3.Tests;

public sealed class S3BackedChannelTests(MinioFixture minio) : IClassFixture<MinioFixture>
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task DirectMode_DeliversAllItemsInOrder()
    {
        var bucket = await minio.CreateBucketAsync();
        using var s3 = minio.CreateClient();

        // Capacity far exceeds the item count: everything stays in memory, nothing is uploaded.
        var channel = Channel.CreateS3BackedChannel<int>(Options(bucket, "events", s3, capacity: 1024));
        try
        {
            const int n = 500;
            for (var i = 0; i < n; i++)
                await channel.Writer.WriteAsync(i);

            var received = await ReadManyAsync(channel, n, Timeout);

            Assert.Equal(Enumerable.Range(0, n), received);
        }
        finally
        {
            (channel as IDisposable)?.Dispose();
        }
    }

    [Fact]
    public async Task Spill_DeliversAllItemsInOrderAcrossUploadBoundary()
    {
        var bucket = await minio.CreateBucketAsync();
        using var s3 = minio.CreateClient();

        // Small capacity forces a spill; items beyond capacity are buffered, uploaded, and replayed.
        var channel = Channel.CreateS3BackedChannel<int>(
            Options(bucket, "events", s3, capacity: 8, maxChunkSize: 4));
        try
        {
            const int n = 200;
            for (var i = 0; i < n; i++)
                await channel.Writer.WriteAsync(i);

            var received = await ReadManyAsync(channel, n, Timeout);

            // Ordering is preserved across the spill boundary: direct items first, then S3-replayed.
            Assert.Equal(Enumerable.Range(0, n), received);
        }
        finally
        {
            (channel as IDisposable)?.Dispose();
        }
    }

    [Fact]
    public async Task NoItemsDropped_WhenWritingFarBeyondCapacity()
    {
        var bucket = await minio.CreateBucketAsync();
        using var s3 = minio.CreateClient();

        var channel = Channel.CreateS3BackedChannel<int>(
            Options(bucket, "events", s3, capacity: 4, maxChunkSize: 8));
        try
        {
            const int n = 1000;
            for (var i = 0; i < n; i++)
                await channel.Writer.WriteAsync(i);

            var received = await ReadManyAsync(channel, n, Timeout);

            Assert.Equal(n, received.Count);
            Assert.Equal(Enumerable.Range(0, n).ToHashSet(), received.ToHashSet());
        }
        finally
        {
            (channel as IDisposable)?.Dispose();
        }
    }

    [Fact]
    public async Task ExistingObjectsInBucket_AreReplayedInOrderOnStartup()
    {
        var bucket = await minio.CreateBucketAsync();
        using var s3 = minio.CreateClient();

        // Seed the bucket with three committed chunks before the channel exists, oldest first.
        await UploadCommittedChunkAsync(s3, bucket, "events", new[] { 0, 1, 2, 3, 4 });
        await UploadCommittedChunkAsync(s3, bucket, "events", new[] { 5, 6, 7, 8, 9 });
        await UploadCommittedChunkAsync(s3, bucket, "events", new[] { 10, 11, 12, 13, 14 });

        var logger = new TestLogger();
        var channel = Channel.CreateS3BackedChannel<int>(
            Options(bucket, "events", s3, capacity: 8, logger: logger));
        try
        {
            // The single startup listing discovers the backlog and starts in spill mode.
            Assert.True(logger.HasEvent("BacklogRecovered"));

            var received = await ReadManyAsync(channel, 15, Timeout);

            Assert.Equal(Enumerable.Range(0, 15), received);
        }
        finally
        {
            (channel as IDisposable)?.Dispose();
        }
    }

    [Fact]
    public async Task AfterBacklogDrained_RevertsToDirectMode()
    {
        var bucket = await minio.CreateBucketAsync();
        using var s3 = minio.CreateClient();

        // Pre-existing backlog: the channel starts spilling and reverts once it is drained.
        await UploadCommittedChunkAsync(s3, bucket, "events", new[] { 0, 1, 2, 3, 4 });

        var logger = new TestLogger();
        var channel = Channel.CreateS3BackedChannel<int>(
            Options(bucket, "events", s3, capacity: 64, logger: logger));
        try
        {
            var backlog = await ReadManyAsync(channel, 5, Timeout);
            Assert.Equal(new[] { 0, 1, 2, 3, 4 }, backlog);

            // Once the backlog drains, the channel logs completion and reverts to direct mode.
            await WaitForAsync(() => logger.HasEvent("SpillCompleted"), Timeout);
            Assert.True(logger.HasEvent("SpillCompleted"));

            // A subsequent write now goes direct through the in-memory channel.
            await channel.Writer.WriteAsync(99);
            var after = await ReadManyAsync(channel, 1, Timeout);
            Assert.Equal(new[] { 99 }, after);
        }
        finally
        {
            (channel as IDisposable)?.Dispose();
        }
    }

    [Fact]
    public async Task ReplayedObjects_AreDeletedFromBucket()
    {
        var bucket = await minio.CreateBucketAsync();
        using var s3 = minio.CreateClient();

        var channel = Channel.CreateS3BackedChannel<int>(
            Options(bucket, "events", s3, capacity: 4, maxChunkSize: 8));
        try
        {
            const int n = 200;
            for (var i = 0; i < n; i++)
                await channel.Writer.WriteAsync(i);

            await ReadManyAsync(channel, n, Timeout);

            // After full replay the chunk objects are deleted, leaving the prefix empty.
            await WaitForAsync(async () => (await ListChunkKeysAsync(s3, bucket, "events")).Count == 0, Timeout);
            Assert.Empty(await ListChunkKeysAsync(s3, bucket, "events"));
        }
        finally
        {
            (channel as IDisposable)?.Dispose();
        }
    }

    [Fact]
    public async Task UploadedObjects_AreStoredUnderTheConfiguredPrefix()
    {
        var bucket = await minio.CreateBucketAsync();
        using var s3 = minio.CreateClient();
        const string prefix = "tenant-a/durable/events";

        var channel = Channel.CreateS3BackedChannel<int>(
            Options(bucket, prefix, s3, capacity: 2, maxChunkSize: 4));
        try
        {
            for (var i = 0; i < 100; i++)
                await channel.Writer.WriteAsync(i);

            // At least one chunk object should appear under the configured prefix.
            await WaitForAsync(async () => (await ListChunkKeysAsync(s3, bucket, prefix)).Count > 0, Timeout);

            var keys = await ListChunkKeysAsync(s3, bucket, prefix);
            Assert.NotEmpty(keys);
            Assert.All(keys, k => Assert.StartsWith(prefix + "/", k));

            // Drain so the background loops have nothing outstanding at dispose.
            await ReadManyAsync(channel, 100, Timeout);
        }
        finally
        {
            (channel as IDisposable)?.Dispose();
        }
    }

    [Fact]
    public async Task Logging_EmitsSpillStarted_WhenChannelFills()
    {
        var bucket = await minio.CreateBucketAsync();
        using var s3 = minio.CreateClient();

        var logger = new TestLogger();
        var channel = Channel.CreateS3BackedChannel<int>(
            Options(bucket, "events", s3, capacity: 2, logger: logger, maxChunkSize: 4));
        try
        {
            for (var i = 0; i < 50; i++)
                await channel.Writer.WriteAsync(i);

            Assert.True(logger.HasEvent("SpillStarted"));

            await ReadManyAsync(channel, 50, Timeout);
        }
        finally
        {
            (channel as IDisposable)?.Dispose();
        }
    }

    [Fact]
    public async Task EmptyPrefix_StoresChunksAtBucketRoot()
    {
        var bucket = await minio.CreateBucketAsync();
        using var s3 = minio.CreateClient();

        var channel = Channel.CreateS3BackedChannel<int>(
            Options(bucket, prefix: "", s3, capacity: 2, maxChunkSize: 4));
        try
        {
            const int n = 100;
            for (var i = 0; i < n; i++)
                await channel.Writer.WriteAsync(i);

            var received = await ReadManyAsync(channel, n, Timeout);
            Assert.Equal(Enumerable.Range(0, n), received);
        }
        finally
        {
            (channel as IDisposable)?.Dispose();
        }
    }
}
