using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Testcontainers.Minio;

namespace ChannelExtensions.Durability.S3.Tests;

/// <summary>
/// Starts a single MinIO container (an S3-compatible server) for the lifetime of a test class and
/// hands out path-style <see cref="IAmazonS3"/> clients pointed at it. Each test creates its own
/// bucket via <see cref="CreateBucketAsync"/> for isolation.
/// </summary>
public sealed class MinioFixture : IAsyncLifetime
{
    private const string User = "minioadmin";
    private const string Password = "minioadmin";

    private readonly MinioContainer _container = new MinioBuilder("minio/minio:latest")
        .WithUsername(User)
        .WithPassword(Password)
        .Build();

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();

    /// <summary>Creates an S3 client for the MinIO endpoint. Path-style addressing is required for MinIO.</summary>
    public IAmazonS3 CreateClient()
    {
        var endpoint = _container.GetConnectionString();
        if (!endpoint.StartsWith("http", StringComparison.OrdinalIgnoreCase))
            endpoint = "http://" + endpoint;

        var config = new AmazonS3Config
        {
            ServiceURL = endpoint,
            ForcePathStyle = true,
            AuthenticationRegion = "us-east-1",

            // AWSSDK v4 enables request/response integrity checksums by default ("WhenSupported"),
            // which sends a streaming-trailer body that MinIO rejects with an x-amz-content-sha256
            // mismatch. Only compute checksums when the operation actually requires them.
            RequestChecksumCalculation = RequestChecksumCalculation.WHEN_REQUIRED,
            ResponseChecksumValidation = ResponseChecksumValidation.WHEN_REQUIRED
        };

        return new AmazonS3Client(new BasicAWSCredentials(User, Password), config);
    }

    /// <summary>Creates a fresh, uniquely named bucket and returns its name.</summary>
    public async Task<string> CreateBucketAsync()
    {
        var bucket = "test-" + Guid.NewGuid().ToString("N");
        using var s3 = CreateClient();
        await s3.PutBucketAsync(new PutBucketRequest { BucketName = bucket });
        return bucket;
    }
}
