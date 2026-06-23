using System.Threading.Channels;
using ChannelExtensions.Durability.S3.S3BackedChannel;

namespace ChannelExtensions.Durability.S3;

public static class DurableChannel
{
    extension(Channel)
    {
        public static Channel<T> CreateS3BackedChannel<T>(S3BackedChannelOptions options)
            => new S3BackedChannel<T>(options);
    }
}
