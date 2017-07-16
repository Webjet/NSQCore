using System;

namespace NSQCore
{
    public class CommunicationException : Exception
    {
        public CommunicationException(string message)
            : base(message)
        {
        }
    }
}
