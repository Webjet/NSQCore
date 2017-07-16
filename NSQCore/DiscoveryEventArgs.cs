using System;
using System.Collections.Generic;

namespace NSQCore
{
    public class DiscoveryEventArgs : EventArgs
    {
        public List<NsqAddress> NsqAddresses { get; }

        public DiscoveryEventArgs(List<NsqAddress> addresses)
        {
            NsqAddresses = addresses;
        }
    }
}
