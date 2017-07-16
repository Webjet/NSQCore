using System.Linq;
using System.Text;

namespace NSQCore.Commands
{
    internal class Nop : ICommand
    {
        private static readonly byte[] NopLf = Encoding.ASCII.GetBytes("NOP\n");

        public byte[] ToByteArray()
        {
            return NopLf.ToArray(); // Make a new one
        }
    }
}
