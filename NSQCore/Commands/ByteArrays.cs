using System.Text;

namespace NSQCore.Commands
{
    internal static class ByteArrays
    {
        public static readonly byte[] Lf = Encoding.ASCII.GetBytes("\n");
        public static readonly byte[] Space = Encoding.ASCII.GetBytes(" ");
    }
}
