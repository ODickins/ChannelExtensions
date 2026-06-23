# ChannelExtensions.Durability.S3

A durable, drop-in `System.Threading.Channels.Channel<T>` backed by Amazon S3. You
write through `Writer` and read through `Reader` exactly as with an in-memory
channel; overflow is buffered in memory, uploaded to S3 as ordered chunks, and
replayed back in order. Nothing is written to the local filesystem.

Part of the `ChannelExtensions.Durability.*` family. See also
[`ChannelExtensions.Durability.FileSystem`](https://www.nuget.org/packages/ChannelExtensions.Durability.FileSystem)
for the local-disk variant.

## How it works

- Writes normally go straight to an in-memory bounded channel (**direct mode**).
- When the channel fills, it switches to **spill mode**: *every* write is buffered in
  memory instead. This preserves order — new items can never jump ahead of items already
  queued.
- A background writer batches buffered items into a newline-delimited JSON (`.ndjson`)
  chunk **in memory**. When the chunk reaches `MaxChunkSize` items **or** `CommitInterval`
  elapses, it is uploaded to S3 in a single `PutObject`.
- A background reader replays committed chunks oldest-first back into the channel,
  waiting for space so the consumer is never overwhelmed, then deletes each object once
  fully replayed.
- On startup the bucket is listed **once** under the prefix to discover pending chunk
  objects. From then on the set of pending object keys is tracked **in memory** (the
  writer appends a key after each upload; the reader consumes them). S3 is never polled
  to ask "is there more?" — the reader blocks on the in-memory key queue.
- Once the entire backlog is drained, the channel reverts to direct mode.

## Guarantees

- **Ordering** — preserved across the spill boundary and across restarts. Chunk objects
  are keyed by a time-ordered v7 GUID, so an ordinal sort of the keys is chronological.
- **Durability begins at the upload.** Because there is no local staging, items that have
  been buffered but not yet uploaded live only in memory and are lost on a hard process
  kill. The upload to S3 is the durability boundary — once a chunk is `PutObject`-ed it
  survives a restart and is replayed.
- **At-least-once replay.** A chunk object is deleted only after it has been fully replayed.
  A crash mid-replay leaves the object in S3, so on restart it is rediscovered by the
  startup listing and replayed again — consumers may therefore see a chunk's records more
  than once across a crash. Make your consumer idempotent if that matters.

## Usage

```csharp
using System.Threading.Channels;
using Amazon.S3;
using ChannelExtensions.Durability.S3;
using ChannelExtensions.Durability.S3.S3BackedChannel;

IAmazonS3 s3 = new AmazonS3Client(); // your configured region/credentials

Channel<MyEvent> channel = Channel.CreateS3BackedChannel<MyEvent>(
    new S3BackedChannelOptions(
        capacity: 10_000,
        bucket: "my-bucket",
        prefix: "events/durable-channel") // sub-key the chunks are stored under
    {
        Client = s3,
        MaxChunkSize = 1_000, // upload once this many overflow items have accumulated
    });

// Producer and consumer are identical to any Channel<T>.
await channel.Writer.WriteAsync(new MyEvent(...));
await foreach (var item in channel.Reader.ReadAllAsync())
    Handle(item);
```

> Like the other durable channels, this is **not** an `IHostedService` — its background
> loops start in the constructor. The constructor also performs the one-time S3 listing
> synchronously, so it makes a blocking S3 call; construct it off the hot path. Logging is
> configured purely by the `Logger` option.

## Options

`S3BackedChannelOptions` extends `ChannelOptions`.

| Option | Default | Description |
| --- | --- | --- |
| `Capacity` (ctor) | — | In-memory bound. The channel spills once this many unread items are buffered. |
| `Bucket` (ctor) | — | The S3 bucket chunk objects are uploaded to. |
| `Prefix` (ctor) | — | Key prefix (sub-key) for chunk objects; surrounding slashes are trimmed. May be empty. |
| `Client` | — (required) | The `IAmazonS3` used for all bucket operations. The constructor throws if not supplied. |
| `CommitInterval` | `15s` | Max time an in-flight chunk is held in memory before it is uploaded. |
| `MaxChunkSize` | `1000` | Max records per chunk object; uploads as soon as this many have accumulated. |
| `JsonSerializerOptions` | `JsonSerializerOptions.Web` | Serialization for records. |
| `QuarantineCorruptObjects` | `true` | When `true`, corrupt objects are copied to a sibling `.corrupt` key and the original deleted. When `false`, they are deleted outright. |
| `Logger` | `null` (no-op) | `ILogger` for spill/upload/replay events. |

## On-S3 layout

| Item | Meaning |
| --- | --- |
| `{prefix}/{guidv7}.{count}.ndjson` | A committed chunk of `count` records. Time-ordered by the v7 GUID prefix. |
| `{key}.corrupt` | A chunk quarantined after an unrecoverable read error (when quarantining is enabled). |
