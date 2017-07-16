using System.Text;

namespace NSQCore
{
    internal enum FrameType
    {
        Result,
        Error,
        Message
    }

    /// <summary>
    /// A frame of data received from nsqd.
    /// </summary>
    internal class Frame
    {
        public FrameType Type { get; set; }
        public int MessageSize { get; set; }

        public byte[] Data { get; set; }

        public string GetReadableData()
        {
            if (Data == null) return "(null)";
            return Encoding.ASCII.GetString(Data, 0, Data.Length);
        }
    }
}
