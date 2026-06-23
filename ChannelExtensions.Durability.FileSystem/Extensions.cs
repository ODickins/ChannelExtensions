using System.Threading.Channels;
using ChannelExtensions.Durability.FileSystem.FileBackedChannel;

namespace ChannelExtensions.Durability.FileSystem;

public static class DurableChannel
{
    extension(Channel)
    {
        public static Channel<T> CreateFileBackedChannel<T>(FileBackedChannelOptions options)
            => new FileBackedChannel<T>(options);
    }
}