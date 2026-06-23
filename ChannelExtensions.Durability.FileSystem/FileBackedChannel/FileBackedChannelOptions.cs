using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace ChannelExtensions.Durability.FileSystem.FileBackedChannel;

/// <summary>
/// Represents the configuration options for a file-backed channel. This class provides
/// customizable settings for channels that utilize file-based storage, enabling durability
/// and persistent messaging across operations. It extends the <see cref="ChannelOptions"/> class.
/// </summary>
public class FileBackedChannelOptions : ChannelOptions
{
    /// <summary>
    /// Represents the configuration options for a file-backed channel. This class provides
    /// customizable settings for channels that use file-based storage to enable durability
    /// and message persistence across operations.
    /// </summary>
    public FileBackedChannelOptions(int capacity, string path)
    {
        if (capacity < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity));
        }

        Capacity = capacity;

        // Ensure the directory exists, create it if it doesn't (could be a subdirectory).
        if (!Directory.Exists(path))
            Directory.CreateDirectory(path);

        // Create a probe file to verify that we can read, write, and delete files in the specified path.
        var probePath = System.IO.Path.Combine(path, $".durable-probe-{Guid.NewGuid():N}");
        try
        {
            // Write a probe file to the path and verify that we can read it back.
            const string probeContent = "probe";
            File.WriteAllText(probePath, probeContent);
            
            // Read the file back and verify that it matches the content we wrote.
            if (File.ReadAllText(probePath) != probeContent)
                throw new IOException($"Read-back verification failed for path '{path}'.");

            // Ensure we can delete the probe file.
            File.Delete(probePath);
        }
        catch (Exception ex)
        {
            // If any of that failed, we have an issue - throw to the caller which should halt the application in most cases or surface a problem.
            throw new ArgumentException(
                $"Path '{path}' is not readable, writable, and deletable.", nameof(path), ex);
        }

        Path = path;
    }


    /// <summary>
    /// The maximum number of items that the in-memory bounded channel can hold at any given time, before it starts spilling to disk.
    /// </summary>
    public int Capacity { get; init; }

    /// <summary>
    /// Specifies the directory path used by the file-backed channel for storing
    /// intermediate data on disk. This path is used for creating temporary files,
    /// buffered records, and checkpoint files, ensuring data durability and recovery
    /// during read and write operations.
    /// </summary>
    public string Path { get; init; }

    /// <summary>
    /// The interval at which pending items in the buffer are automatically committed to disk.
    /// This property determines the duration between consecutive flush operations, ensuring
    /// that data is persisted at regular intervals to minimize the risk of loss during unexpected
    /// application shutdowns or failures.
    /// </summary>
    public TimeSpan CommitInterval { get; init; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Specifies the maximum number of items to be written to a single block on disk before committing the block.
    /// This property defines the size of each batch written to the file system during the persistence process.
    /// </summary>
    public int MaxBlockSize { get; init; } = 1000;

    /// <summary>
    /// Provides a mechanism to log informational, warning, and error messages
    /// within the context of a file-backed channel's operations.
    /// </summary>
    /// <remarks>
    /// This property is utilized to output logs for critical events such as recovery operations,
    /// buffer spills, disk writes, and potential system errors encountered during the
    /// lifecycle of the file-backed channel.
    /// </remarks>
    public ILogger? Logger { get; init; }

    /// <summary>
    /// Defines the JSON serialization and deserialization behavior for file-backed channel operations.
    /// This property is used to configure the options for the <see cref="System.Text.Json.JsonSerializer"/>
    /// when serializing and deserializing objects to and from files for channel persistence.
    /// </summary>
    public JsonSerializerOptions JsonSerializerOptions { get; set; } = JsonSerializerOptions.Web;

    /// <summary>
    /// Controls how corrupt or unrecoverable blocks are handled. When <c>true</c> (the default),
    /// such blocks are renamed with a ".corrupt" suffix and left on disk for inspection. When
    /// <c>false</c>, they are deleted instead, trading post-mortem diagnostics for a self-cleaning
    /// directory.
    /// </summary>
    public bool QuarantineCorruptBlocks { get; init; } = true;
}