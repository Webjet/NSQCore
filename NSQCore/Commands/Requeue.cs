using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NSQCore.Commands
{
    internal class Requeue : ICommand
    {
        private static readonly byte[] Prefix = Encoding.ASCII.GetBytes("REQ ");

        private readonly string _messageId;
        private readonly int _maxTimeout;

        public Requeue(string id, int maxTimeout = 0)
        {
            _messageId = string.Format("{0} ", id);
            _maxTimeout = maxTimeout;
        }

        public byte[] ToByteArray()
        {
            return Prefix
                .Concat(Encoding.ASCII.GetBytes(_messageId))
                .Concat(Encoding.ASCII.GetBytes(_maxTimeout.ToString()))
                .Concat(ByteArrays.Lf)
                .ToArray();
        }
    }
}
