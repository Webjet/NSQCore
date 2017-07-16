using System;
using System.Net.Sockets;

namespace NSQCore
{
    internal class FrameReader
    {
        private const int FrameSizeLength = 4;
        private const int FrameTypeLength = 4;

        private readonly NetworkStream _stream;

        private readonly object _lock = new object();
        private readonly byte[] _frameSizeBuffer = new byte[FrameSizeLength];
        private readonly byte[] _frameTypeBuffer = new byte[FrameTypeLength];

        public FrameReader(NetworkStream stream)
        {
            _stream = stream;
        }

        public Frame ReadFrame()
        {
            lock (_lock)
            {
                // MESSAGE FRAME FORMAT:
                //   4 bytes - Int32, size of the frame, excluding this field
                //   4 bytes - Int32, frame type
                //   N bytes - data
                //      8 bytes - Int64, timestamp
                //      2 bytes - UInt16, attempts
                //     16 bytes - Hex-string encoded message ID
                //      N bytes - message body

                // Get the size of the incoming frame
                ReadBytes(_frameSizeBuffer, 0, FrameSizeLength);
                if (BitConverter.IsLittleEndian)
                    Array.Reverse(_frameSizeBuffer);
                var frameLength = BitConverter.ToInt32(_frameSizeBuffer, 0);

                // Read the rest of the frame
                var frame = ReadBytesWithAllocation(frameLength);

                // Get the frame type
                Array.ConstrainedCopy(frame, 0, _frameTypeBuffer, 0, FrameTypeLength);
                if (BitConverter.IsLittleEndian)
                    Array.Reverse(_frameTypeBuffer);
                var frameType = (FrameType)BitConverter.ToInt32(_frameTypeBuffer, 0);

                // Get the data portion of the frame
                var dataLength = frameLength - FrameTypeLength;
                byte[] dataBuffer = new byte[dataLength];
                Array.ConstrainedCopy(frame, FrameTypeLength, dataBuffer, 0, dataLength);

                return new Frame
                {
                    MessageSize = frameLength,
                    Type = frameType,
                    Data = dataBuffer
                };
            }
        }

        private void ReadBytes(byte[] buffer, int offset, int count)
        {
            int bytesRead;
            int bytesLeft = count;

            while ((bytesRead = _stream.Read(buffer, offset, bytesLeft)) > 0)
            {
                offset += bytesRead;
                bytesLeft -= bytesRead;
                if (offset > count) throw new InvalidOperationException("Read too many bytes");
                if (offset == count) break;
            }

            if (bytesLeft > 0)
                throw new SocketException((int)SocketError.SocketError);
        }

        private byte[] ReadBytesWithAllocation(int count)
        {
            byte[] buffer = new byte[count];
            int offset = 0;
            ReadBytes(buffer, offset, count);
            return buffer;
        }
    }
}
