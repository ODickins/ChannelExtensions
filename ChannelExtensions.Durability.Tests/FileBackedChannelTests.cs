using System.Threading.Channels;
using ChannelExtensions.Durability.FileSystem;
using ChannelExtensions.Durability.FileSystem.FileBackedChannel;
using static ChannelExtensions.Durability.Tests.TestHelpers;

namespace ChannelExtensions.Durability.Tests;

public class FileBackedChannelTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task DirectMode_DeliversAllItemsInOrder()
    {
        using var tmp = new TempDir();
        var channel = Channel.CreateFileBackedChannel<int>(Options(tmp.Path, capacity: 1024));

        try
        {
            const int n = 500;
            for (var i = 0; i < n; i++)
            {
                await channel.Writer.WriteAsync(i);
            }

            var received = await ReadManyAsync(channel, n, Timeout);

            Assert.Equal(Enumerable.Range(0, n), received);
        }
        finally
        {
            ((IDisposable)channel).Dispose();
        }
    }

    [Fact]
    public async Task SpillMidStream_PreservesOrderAndDropsNothing()
    {
        using var tmp = new TempDir();
        var logger = new TestLogger();

        // Tiny capacity + burst write before consuming guarantees the publisher
        // fills and the channel spills the tail to disk.
        var channel = Channel.CreateFileBackedChannel<int>(Options(tmp.Path, capacity: 4, logger));

        try
        {
            const int n = 1000;
            for (var i = 0; i < n; i++)
            {
                await channel.Writer.WriteAsync(i);
            }

            var received = await ReadManyAsync(channel, n, Timeout);

            // Nothing dropped, nothing reordered across the spill boundary.
            Assert.Equal(Enumerable.Range(0, n), received);

            // Prove the spill path was actually exercised.
            Assert.Contains(logger.Entries, e => e.EventId == FileBackedChannelEventIds.SpillStarted);
        }
        finally
        {
            ((IDisposable)channel).Dispose();
        }
    }

    [Fact]
    public async Task SpillThenDrain_RevertsToDirectMode()
    {
        using var tmp = new TempDir();
        var logger = new TestLogger();
        var channel = Channel.CreateFileBackedChannel<int>(Options(tmp.Path, capacity: 4, logger));

        try
        {
            const int n = 300;
            for (var i = 0; i < n; i++)
            {
                await channel.Writer.WriteAsync(i);
            }

            var received = await ReadManyAsync(channel, n, Timeout);
            Assert.Equal(Enumerable.Range(0, n), received);

            // Once the backlog drains, the channel must flip back to direct writes.
            await WaitForAsync(
                () => logger.Entries.Any(e => e.EventId == FileBackedChannelEventIds.SpillCompleted),
                TimeSpan.FromSeconds(5));

            Assert.Contains(logger.Entries, e => e.EventId == FileBackedChannelEventIds.SpillCompleted);
        }
        finally
        {
            ((IDisposable)channel).Dispose();
        }
    }

    [Fact]
    public async Task CommittedBlocks_RecoveredOnStartupInOrder()
    {
        using var tmp = new TempDir();

        // Two pre-existing committed blocks; ordinal filename order is the replay order.
        WriteCommittedBlock(tmp.Path, "0001", count: 3, Ndjson([0, 1, 2]));
        WriteCommittedBlock(tmp.Path, "0002", count: 3, Ndjson([3, 4, 5]));

        var channel = Channel.CreateFileBackedChannel<int>(Options(tmp.Path, capacity: 1024));

        try
        {
            var received = await ReadManyAsync(channel, 6, Timeout);

            Assert.Equal(new[] { 0, 1, 2, 3, 4, 5 }, received);
        }
        finally
        {
            ((IDisposable)channel).Dispose();
        }
    }

    [Fact]
    public async Task OrphanedTmp_RecoversCompleteLinesAndDropsPartialTail()
    {
        using var tmp = new TempDir();

        // Simulates a crash mid-write: three whole records, then a partial fourth
        // with no terminating newline.
        File.WriteAllText(Path.Combine(tmp.Path, "0001.tmp"), "0\n1\n2\n99");

        var channel = Channel.CreateFileBackedChannel<int>(Options(tmp.Path, capacity: 1024));

        try
        {
            var received = await ReadManyAsync(channel, 3, Timeout);

            Assert.Equal(new[] { 0, 1, 2 }, received);

            // The partial record must not be replayed.
            await AssertNoMoreAsync(channel, TimeSpan.FromMilliseconds(500));

            // No .tmp should survive recovery.
            Assert.Empty(Directory.EnumerateFiles(tmp.Path, "*.tmp"));
        }
        finally
        {
            ((IDisposable)channel).Dispose();
        }
    }

    [Fact]
    public async Task CorruptRecord_IsDiscardedAndOthersDelivered()
    {
        using var tmp = new TempDir();
        var logger = new TestLogger();

        // Five non-empty lines, the middle one invalid JSON.
        WriteCommittedBlock(tmp.Path, "0001", count: 5, "0\n1\n{\n3\n4\n");

        var channel = Channel.CreateFileBackedChannel<int>(Options(tmp.Path, capacity: 1024, logger));

        try
        {
            var received = await ReadManyAsync(channel, 4, Timeout);

            Assert.Equal(new[] { 0, 1, 3, 4 }, received);
            await AssertNoMoreAsync(channel, TimeSpan.FromMilliseconds(500));
            Assert.Contains(logger.Entries, e => e.EventId == FileBackedChannelEventIds.RecordDeserializeFailed);
        }
        finally
        {
            ((IDisposable)channel).Dispose();
        }
    }

    [Fact]
    public async Task Checkpoint_SkipsAlreadyEmittedRecords()
    {
        using var tmp = new TempDir();

        WriteCommittedBlock(tmp.Path, "0001", count: 5, Ndjson([0, 1, 2, 3, 4]));

        // Pretend two records were already handed off before a crash.
        File.WriteAllText(Path.Combine(tmp.Path, "0001.5.ndjson.ckpt"), 2L.ToString("D20"));

        var channel = Channel.CreateFileBackedChannel<int>(Options(tmp.Path, capacity: 1024));

        try
        {
            var received = await ReadManyAsync(channel, 3, Timeout);

            // 0 and 1 already emitted -> resume at 2, no replay.
            Assert.Equal(new[] { 2, 3, 4 }, received);
            await AssertNoMoreAsync(channel, TimeSpan.FromMilliseconds(500));
        }
        finally
        {
            ((IDisposable)channel).Dispose();
        }
    }

    [Fact]
    public async Task AllLogs_CarryEventIds()
    {
        using var tmp = new TempDir();
        var logger = new TestLogger();
        var channel = Channel.CreateFileBackedChannel<int>(Options(tmp.Path, capacity: 4, logger));

        try
        {
            const int n = 200;
            for (var i = 0; i < n; i++)
            {
                await channel.Writer.WriteAsync(i);
            }

            var received = await ReadManyAsync(channel, n, Timeout);
            Assert.Equal(Enumerable.Range(0, n), received);

            await WaitForAsync(() => !logger.Entries.IsEmpty, TimeSpan.FromSeconds(5));
        }
        finally
        {
            ((IDisposable)channel).Dispose();
        }

        // Every emitted log must have a non-default EventId.
        Assert.NotEmpty(logger.Entries);
        Assert.All(logger.Entries, e => Assert.NotEqual(0, e.EventId.Id));
    }
}
