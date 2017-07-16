using System.Linq;
using System.Text;

namespace NSQCore.Commands
{
    internal class Finish : ICommand
    {
        private static readonly byte[] FinSpace = Encoding.ASCII.GetBytes("FIN ");

        private readonly Message _message;

        public Finish(Message message)
        {
            _message = message;
        }

        public byte[] ToByteArray()
        {
            return FinSpace
                .Concat(Encoding.UTF8.GetBytes(_message.Id))
                .Concat(ByteArrays.Lf)
                .ToArray();
        }
    }
}
