# ChannelExtensions.Durability.FileSystem

A durable, drop-in `System.Threading.Channels.Channel<T>` backed by the local filesystem. You
write through `Writer` and read through `Reader` exactly as with an in-memory channel; overflow
**spills to disk** when the channel fills and is replayed back in order once the consumer catches
up. The durability is transparent to producers and consumers.

Part of the `ChannelExtensions.Durability.*` family. See also
[`ChannelExtensions.Durability.S3`](https://www.nuget.org/packages/ChannelExtensions.Durability.S3)
for the S3-backed variant.

## Install

```bash
dotnet add package ChannelExtensions.Durability.FileSystem
```

## How it works

- Writes normally go straight to an in-memory bounded channel (**direct mode**).
- When the channel fills, it switches to **spill mode**: *every* write is appended to disk instead.
  This preserves order — new items can never jump ahead of items already queued on disk.
- A background writer batches spilled items into newline-delimited JSON (`.ndjson`) blocks. A block
  is committed when it reaches `MaxBlockSize` items **or** `CommitInterval` elapses, whichever comes
  first.
- A background reader replays committed blocks oldest-first back into the channel, waiting for space
  so the consumer is never overwhelmed.
- Once the entire disk backlog is drained, the channel reverts to direct mode.

## Guarantees

- **No drops** — items that don't fit in memory are persisted, not discarded.
- **Ordering** — preserved across the spill boundary and across restarts. Blocks are keyed by a
  time-ordered v7 GUID, so an ordinal sort of the filenames is chronological.
- **Crash recovery** — on startup, half-written blocks left by a crash are finalized (the truncated
  trailing record is dropped) and replayed. If a backlog exists at startup, the channel begins in
  spill mode so new writes queue behind it.
- **Idempotent replay** — each block tracks how many records have been handed to the consumer via a
  checkpoint written *before* each emit, so a crash mid-replay does not re-deliver an already-emitted
  record (at-most-once at the boundary; no duplicates).
- **Durable writes** — blocks and checkpoints are written with `FileOptions.WriteThrough`, bypassing
  the OS write cache.

## Usage

```csharp
using System.Threading.Channels;
using ChannelExtensions.Durability.FileSystem;
using ChannelExtensions.Durability.FileSystem.FileBackedChannel;

// Create the channel. The path is created (and verified read/write/delete-able)
// in the constructor; an unusable path throws.
Channel<MyEvent> channel = Channel.CreateFileBackedChannel<MyEvent>(
    new FileBackedChannelOptions(capacity: 10_000, path: @"C:\data\my-channel"));

// Producer — identical to any Channel<T>.
await channel.Writer.WriteAsync(new MyEvent(...));

// Consumer — identical to any Channel<T>.
await foreach (var item in channel.Reader.ReadAllAsync())
{
    Handle(item);
}
```

## Dependency injection

The factory runs eagerly, so register it with a factory delegate when you want the configured
`ILogger` (or other services) injected:

```csharp
builder.Services.AddSingleton<Channel<MyEvent>>(sp =>
    Channel.CreateFileBackedChannel<MyEvent>(
        new FileBackedChannelOptions(10_000, @"C:\data\my-channel")
        {
            Logger = sp.GetRequiredService<ILoggerFactory>().CreateLogger("FileBackedChannel"),
        }));
```

> Note: a durable channel is **not** an `IHostedService`. Its background drain loops start in the
> constructor. Logging is configured purely by the `Logger` option — when omitted it defaults to a
> no-op logger.

## Options

`FileBackedChannelOptions` extends `ChannelOptions`.

| Option | Default | Description |
| --- | --- | --- |
| `Capacity` (ctor) | — | In-memory bound. The channel spills to disk once this many unread items are buffered. |
| `Path` (ctor) | — | Directory for block/checkpoint files. Created and verified at construction. |
| `CommitInterval` | `15s` | Max time a spill block stays open before being committed. |
| `MaxBlockSize` | `1000` | Max records per block; commits early when reached. |
| `NodeId` | sanitized machine name | Scopes block file names (`{NodeId}.…`) and the startup scan to this node, so nodes sharing a directory never read each other's blocks. Defaults to `Environment.MachineName` (sanitized to `[A-Za-z0-9_-]`), stable across restarts on the same host - so a restarted process, or a StatefulSet pod, recovers its own backlog. Override for custom scenarios; falls back to the all-zero guid (still stable) if the machine name is empty. |
| `JsonSerializerOptions` | `JsonSerializerOptions.Web` | Serialization for on-disk records. |
| `QuarantineCorruptBlocks` | `true` | When `true`, corrupt/unrecoverable blocks are renamed `.corrupt` and kept for inspection. When `false`, they are deleted instead (self-cleaning directory). |
| `Logger` | `null` (no-op) | `ILogger` for spill/recovery/error events. |

## On-disk layout

All files live under `Path`:

| File | Meaning |
| --- | --- |
| `{nodeid}.{guidv7}.tmp` | A block currently being written. Recovered on startup. |
| `{nodeid}.{guidv7}.{count}.ndjson` | A committed block of `count` records. Time-ordered by the v7 GUID; the node id scopes it to one node. |
| `{block}.ndjson.ckpt` | Replay checkpoint: records already delivered from that block. |
| `{name}.corrupt` | A block quarantined after an unrecoverable read error. Only present when `QuarantineCorruptBlocks` is enabled (the default); otherwise such blocks are deleted. |

## Logging

All log events carry an `EventId` (see `FileBackedChannelEventIds`): spill started/completed,
backlog recovered, block commit/replay/recovery failures, quarantine/delete failures, and drain-loop
faults. Failures are logged and handled rather than thrown out of the background loops.

## Disposal

Disposing the channel cancels the background loops and gives in-flight commits a bounded chance to
finish. Items still buffered in memory (not yet spilled) are lost on a hard process kill, which is
the window the spill + recovery machinery exists to minimize.
