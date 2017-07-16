using System.Linq;
using System.Text;

namespace NSQCore.Commands
{
    internal class Subscribe : ICommand
    {
        private readonly Topic _topic;
        private readonly Channel _channel;

        public Subscribe(Topic topic, Channel channel)
        {
            _topic = topic;
            _channel = channel;
        }

        private static readonly byte[] SubSpace = Encoding.ASCII.GetBytes("SUB ");

        public byte[] ToByteArray()
        {
            return SubSpace
                .Concat(_topic.ToUtf8())
                .Concat(ByteArrays.Space)
                .Concat(_channel.ToUtf8())
                .Concat(ByteArrays.Lf)
                .ToArray();
        }
    }
}
