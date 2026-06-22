using System.Threading.Channels;
using ChannelExtensions.Durability.FileBackedChannel;

namespace ChannelExtensions.Durability;

public static class DurableChannel
{
    extension(Channel)
    {
        public static Channel<T> CreateFileBackedChannel<T>(FileBackedChannelOptions options)
            => new FileBackedChannel<T>(options);
    }
}