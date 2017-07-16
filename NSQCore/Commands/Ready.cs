using System.Linq;
using System.Text;

namespace NSQCore.Commands
{
    internal class Ready : ICommand
    {
        private readonly int _count;

        public Ready(int count)
        {
            _count = count;
        }

        private static readonly byte[] RdySpace = Encoding.ASCII.GetBytes("RDY ");

        public byte[] ToByteArray()
        {
            return RdySpace
                .Concat(Encoding.UTF8.GetBytes(_count.ToString()))
                .Concat(ByteArrays.Lf)
                .ToArray();
        }
    }
}
