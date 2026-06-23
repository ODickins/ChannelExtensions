# ChannelExtensions.Durability

Durable, drop-in variants of `System.Threading.Channels.Channel<T>` that survive process restarts
and back-pressure **without dropping or reordering items**.

Each durable channel is a drop-in `Channel<T>`: you write through `Writer` and read through `Reader`
exactly as with an in-memory channel. When the channel fills, overflow is persisted to a backing
store and replayed back in order once the consumer catches up. The durability is transparent to
producers and consumers — no API to learn beyond the standard `Channel<T>`.

This repository is a family of packages, one per backing store:

| Package | Backing store | Use when | Docs |
| --- | --- | --- | --- |
| [`ChannelExtensions.Durability.FileSystem`](https://www.nuget.org/packages/ChannelExtensions.Durability.FileSystem) | Local filesystem (NDJSON blocks) | You need overflow + crash durability on a single node with a local/attached disk. | [README](ChannelExtensions.Durability.FileSystem/README.md) |
| [`ChannelExtensions.Durability.S3`](https://www.nuget.org/packages/ChannelExtensions.Durability.S3) | Amazon S3 (NDJSON chunk objects; no local disk) | You need overflow durability backed by S3, buffered in memory and uploaded in chunks. | [README](ChannelExtensions.Durability.S3/README.md) |

## Install

```bash
# Local filesystem backing store
dotnet add package ChannelExtensions.Durability.FileSystem

# Amazon S3 backing store
dotnet add package ChannelExtensions.Durability.S3
```

## Quick start

Both channels are created from a factory extension on `Channel` and then used like any other
`Channel<T>`. See each package's README for the full options and behavior.

**FileSystem** — spills to a local directory:

```csharp
using System.Threading.Channels;
using ChannelExtensions.Durability.FileSystem;
using ChannelExtensions.Durability.FileSystem.FileBackedChannel;

Channel<MyEvent> channel = Channel.CreateFileBackedChannel<MyEvent>(
    new FileBackedChannelOptions(capacity: 10_000, path: @"C:\data\my-channel"));

await channel.Writer.WriteAsync(new MyEvent(...));
await foreach (var item in channel.Reader.ReadAllAsync())
    Handle(item);
```

**S3** — buffers in memory and uploads chunks to a bucket/prefix:

```csharp
using System.Threading.Channels;
using Amazon.S3;
using ChannelExtensions.Durability.S3;
using ChannelExtensions.Durability.S3.S3BackedChannel;

IAmazonS3 s3 = new AmazonS3Client(); // your configured region/credentials

Channel<MyEvent> channel = Channel.CreateS3BackedChannel<MyEvent>(
    new S3BackedChannelOptions(capacity: 10_000, bucket: "my-bucket", client: s3)
    {
        Prefix = "events/durable-channel", // optional
    });

await channel.Writer.WriteAsync(new MyEvent(...));
await foreach (var item in channel.Reader.ReadAllAsync())
    Handle(item);
```

## Shared design

All channels in the family follow the same model:

- **Drop-in `Channel<T>`.** The exposed `Reader` is an in-memory bounded channel; the exposed
  `Writer` decides per-write whether to go direct or to spill to the backing store.
- **Spill on pressure.** While there's room, writes go straight through (direct mode). Once full,
  *every* write spills to the backing store until the backlog drains — this is what preserves global
  ordering across the boundary.
- **Ordered replay.** Spilled records are batched into time-ordered (v7 GUID) NDJSON blocks/objects
  and replayed oldest-first, waiting for space so the consumer is never overwhelmed.
- **Eager, not hosted.** Background drain loops start in the constructor — a channel is *not* an
  `IHostedService`. There is no async init step to await.
- **Resilient loops.** Backing-store failures are logged (with `EventId`s) and handled rather than
  thrown out of the background loops, so a transient fault never tears down the host.

The packages differ in their durability boundary and recovery story — for example, FileSystem gives
at-most-once delivery at the replay boundary via local checkpoints, while S3 keeps nothing on disk
and is at-least-once at that boundary. See each README for the exact guarantees.

## Choosing a backing store

- **Single node with a local/attached disk** → `FileSystem`. Strongest crash story (durable writes,
  checkpointed idempotent replay), no external dependency.
- **Durability that outlives the node, or no usable local disk** → `S3`. Nothing touches local disk;
  the bucket is listed once on startup and pending object keys are tracked in memory thereafter.

## Repository layout

| Project | Purpose |
| --- | --- |
| `ChannelExtensions.Durability.FileSystem` | The file-backed durable channel. |
| `ChannelExtensions.Durability.S3` | The S3-backed durable channel (in-memory buffering, no local disk). |
| `ChannelExtensions.Durability.FileSystem.Tests` | xUnit tests for the file-backed channel (no-drop, ordering across spill, crash recovery, idempotent replay, logging). |
| `ChannelExtensions.Durability.S3.Tests` | xUnit tests for the S3-backed channel, run against a real MinIO server via Testcontainers (requires Docker). |

## Building and testing

```bash
dotnet build

# All test projects.
dotnet test

# A single project.
dotnet test ChannelExtensions.Durability.FileSystem.Tests
```

> The S3 tests (`ChannelExtensions.Durability.S3.Tests`) run against a real MinIO server via
> Testcontainers and therefore require Docker to be running.

## Publishing

Publishing is automated. Publishing a GitHub Release tagged `vX.Y.Z` triggers
`.github/workflows/publish-to-nuget.yml`, which builds, tests, packs **every** packable project, and
pushes the packages to nuget.org via OIDC trusted publishing (no stored API key). The tag's `v`
prefix is stripped to form the package version.

## License

MIT — see [LICENSE](LICENSE).
