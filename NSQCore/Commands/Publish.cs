using System;
using System.Linq;
using System.Text;

namespace NSQCore.Commands
{
    internal class Publish : ICommand
    {
        private static readonly byte[] Prefix = Encoding.ASCII.GetBytes("PUB ");

        private readonly Topic _topic;
        private readonly MessageBody _message;


        public Publish(Topic topic, MessageBody message)
        {
            _message = message;
            _topic = topic;
        }

        public byte[] ToByteArray()
        {
            byte[] messageBody = _message;
            byte[] size = BitConverter.GetBytes(messageBody.Length);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(size);

            return Prefix
                .Concat(_topic.ToUtf8())
                .Concat(ByteArrays.Lf)
                .Concat(size)
                .Concat(messageBody)
                .ToArray();
        }
    }
}
