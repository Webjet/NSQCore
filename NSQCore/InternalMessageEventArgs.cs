using System;

namespace NSQCore
{
    /// <summary>
    /// EventArgs for a message raised by the internals of NSQCore.
    /// </summary>
    public class InternalMessageEventArgs : EventArgs
    {
        internal InternalMessageEventArgs(string message)
        {
            Message = message;
        }

        /// <summary>
        /// The message raised.
        /// </summary>
        public string Message { get; }
    }
}
