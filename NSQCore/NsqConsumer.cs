using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

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
        
        public static INsqConsumer Create(string connectionString, ILogger logger)
        {
            return Create(ConsumerOptions.Parse(connectionString), logger);
        }
        
        public static INsqConsumer Create(ConsumerOptions options)
        {
            return Create(options, null);
        }

        public static INsqConsumer Create(ConsumerOptions options, ILogger logger)
        {
            if (options.LookupEndPoints.Any())
            {
                return new NsqLookupConsumer(options, logger);
            }
            return new NsqTcpConnection(options.NsqEndPoint, options);
        }
    }
}
