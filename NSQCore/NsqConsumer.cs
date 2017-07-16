using System;
using System.Linq;
using System.Threading.Tasks;

namespace NSQCore
{
    public interface INsqConsumer : IDisposable
    {
        void Connect(MessageHandler handler);
        Task ConnectAndWaitAsync(MessageHandler handler);
        Task PublishAsync(Topic topic, MessageBody message);
        Task SetMaxInFlightAsync(int maxInFlight);
    }

    public delegate Task MessageHandler(Message message);

    public static class NsqConsumer
    {
        public static INsqConsumer Create(string connectionString)
        {
            return Create(ConsumerOptions.Parse(connectionString));
        }

        public static INsqConsumer Create(ConsumerOptions options)
        {
            if (options.LookupEndPoints.Any())
            {
                return new NsqLookupConsumer(options);
            }
            return new NsqTcpConnection(options.NsqEndPoint, options);
        }
    }
}
