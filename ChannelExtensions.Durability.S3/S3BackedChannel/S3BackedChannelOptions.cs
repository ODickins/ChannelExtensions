using System.Text.Json;
using System.Threading.Channels;
using Amazon.S3;
using Microsoft.Extensions.Logging;

namespace ChannelExtensions.Durability.S3.S3BackedChannel;

/// <summary>
/// Configuration for an <see cref="S3BackedChannel{T}"/>. Overflow items are buffered in memory and,
/// once a chunk is full (or the commit window elapses), uploaded as a single NDJSON object to
/// <see cref="Bucket"/> under <see cref="Prefix"/>. Nothing is written to the local filesystem.
/// Extends <see cref="ChannelOptions"/>.
/// </summary>
public class S3BackedChannelOptions : ChannelOptions
{
    /// <summary>
    /// Creates the options. The two required values — the in-memory <paramref name="capacity"/>, the
    /// target <paramref name="bucket"/>, and the <paramref name="client"/> — are constructor
    /// arguments; everything else (including <see cref="Prefix"/>) is an optional init property.
    /// </summary>
    /// <param name="capacity">In-memory bound. The channel spills once this many unread items are buffered.</param>
    /// <param name="bucket">The S3 bucket that committed chunks are uploaded to.</param>
    /// <param name="client">The S3 client used for all bucket operations. Inject your configured (region, credentials) <see cref="IAmazonS3"/>.</param>
    public S3BackedChannelOptions(int capacity, string bucket, IAmazonS3 client)
    {
        if (capacity < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity));
        }

        if (string.IsNullOrWhiteSpace(bucket))
        {
            throw new ArgumentException("A bucket name must be provided.", nameof(bucket));
        }

        Capacity = capacity;
        Bucket = bucket;
        Client = client ?? throw new ArgumentNullException(nameof(client));
    }

    /// <summary>
    /// The maximum number of items the in-memory bounded channel holds before it starts buffering
    /// overflow items for upload.
    /// </summary>
    public int Capacity { get; init; }

    /// <summary>The S3 bucket that committed chunk objects are uploaded to.</summary>
    public string Bucket { get; init; }

    /// <summary>The S3 client used for all bucket operations.</summary>
    public IAmazonS3 Client { get; }

    /// <summary>
    /// The key prefix (sub-key) under which chunk objects are stored, with surrounding slashes
    /// trimmed. Optional — defaults to empty, meaning objects are written at the root of the bucket.
    /// Object keys take the form <c>{Prefix}/{guidv7}.{count}.ndjson</c>.
    /// </summary>
    public string Prefix
    {
        get => _prefix;
        init => _prefix = (value ?? string.Empty).Trim('/');
    }

    private readonly string _prefix = string.Empty;

    /// <summary>
    /// Identifies this node/instance. It is embedded as the first segment of every object key
    /// (<c>{Prefix}/{NodeId}.{guidv7}.{count}.ndjson</c>) and used to scope the one-time startup
    /// listing, so a node only ever lists and replays its own chunks - even when many nodes share a
    /// bucket and prefix.
    /// <para>
    /// Defaults to the sanitized machine name (<see cref="Environment.MachineName"/>), which is stable
    /// across restarts on the same host: a restarted process - or a Kubernetes StatefulSet pod, whose
    /// host name is preserved - recovers and replays its own pending chunks. Override it for scenarios
    /// where the machine name is not stable or not unique. The value is always sanitized to
    /// <c>[A-Za-z0-9_-]</c> so it is safe inside a key; falls back to the all-zero guid
    /// (<c>00000000000000000000000000000000</c>) only if the machine name is empty - which is itself
    /// stable, so recovery still works on that host.
    /// </para>
    /// </summary>
    public string NodeId
    {
        get => _nodeId;
        init => _nodeId = SanitizeNodeId(value);
    }

    private readonly string _nodeId = SanitizeNodeId(null);

    /// <summary>
    /// Sanitizes a node id to <c>[A-Za-z0-9_-]</c> so it is safe as the leading segment of an object
    /// key. A <c>null</c>/blank value falls back to the machine name; if that is also empty, the
    /// all-zero guid is used (stable, so the node still recovers its own backlog).
    /// </summary>
    private static string SanitizeNodeId(string? value)
    {
        var source = string.IsNullOrWhiteSpace(value) ? Environment.MachineName : value;

        // Replace anything outside the safe set (path/key separators, dots, whitespace, unicode) with
        // '-'. Dots are excluded so they can't be confused with the dot-delimited key segments.
        var sanitized = string.IsNullOrEmpty(source)
            ? string.Empty
            : new string(source.Select(c =>
                (c is >= 'a' and <= 'z') || (c is >= 'A' and <= 'Z') || (c is >= '0' and <= '9') || c is '-' or '_'
                    ? c : '-').ToArray());

        // All-zero guid (not a random one) so an empty machine name still yields a stable id.
        return string.IsNullOrEmpty(sanitized) ? Guid.Empty.ToString("N") : sanitized;
    }

    /// <summary>
    /// The maximum time an in-flight chunk is held in memory before it is uploaded, even if it has
    /// not reached <see cref="MaxChunkSize"/>.
    /// </summary>
    public TimeSpan CommitInterval { get; init; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// The maximum number of records per chunk object. The chunk is uploaded as soon as this many
    /// items have accumulated.
    /// </summary>
    public int MaxChunkSize { get; init; } = 1000;

    /// <summary>Serialization used for records.</summary>
    public JsonSerializerOptions JsonSerializerOptions { get; set; } = JsonSerializerOptions.Web;

    /// <summary>
    /// Controls how corrupt or unrecoverable chunk objects are handled. When <c>true</c> (the
    /// default), such objects are copied to a sibling <c>.corrupt</c> key (excluded from the startup
    /// listing) and the original deleted, so they can be inspected. When <c>false</c>, they are
    /// deleted outright.
    /// </summary>
    public bool QuarantineCorruptObjects { get; init; } = true;

    /// <summary>An optional logger for spill/upload/replay and error events.</summary>
    public ILogger? Logger { get; init; }
}
