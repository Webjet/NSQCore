using System;

namespace NSQCore
{
    /// <summary>
    /// The address of an nsqlookupd instance.
    /// </summary>
    public struct LookupAddress : IEquatable<LookupAddress>
    {
        public string HostName { get; }
        public int HttpPort { get; }

        public LookupAddress(string hostName, int httpPort)
            : this()
        {
            HostName = hostName;
            HttpPort = httpPort;
        }

        public override string ToString()
        {
            return "HostName = " + HostName + ", HttpPort = " + HttpPort;
        }

        public override bool Equals(object obj)
        {
            if (obj is LookupAddress)
                return Equals((LookupAddress)obj);

            return false;
        }

        public override int GetHashCode()
        {
            // See: http://stackoverflow.com/a/263416/19818
            const int BASE = 151;
            const int mixer = 2011;
            unchecked // Overflow is fine
            {
                int hash = BASE;
                hash = hash * mixer + HttpPort.GetHashCode();
                if (HostName != null)
                    hash = hash * mixer + HostName.GetHashCode();

                return hash;
            }
        }

        /// <summary>
        /// The address of an nsqd instance.
        /// </summary>
        public bool Equals(LookupAddress other)
        {
            return HttpPort == other.HttpPort
                   && string.Equals(HostName, other.HostName, StringComparison.CurrentCultureIgnoreCase);
        }
    }

    /// <summary>
    /// The address of an nsqd instance.
    /// </summary>
    public struct NsqAddress : IEquatable<NsqAddress>
    {
        public string BroadcastAddress { get; }
        public string HostName { get; }
        public int HttpPort { get; }
        public int TcpPort { get; }

        public NsqAddress(string broadcastAddress, string hostName, int tcpPort, int httpPort)
            : this()
        {
            BroadcastAddress = broadcastAddress;
            HostName = hostName;
            TcpPort = tcpPort;
            HttpPort = httpPort;
        }

        public override string ToString()
        {
            return "BroadcastAddress = " + BroadcastAddress + ", TcpPort = " + TcpPort;
        }

        public override int GetHashCode()
        {
            // See: http://stackoverflow.com/a/263416/19818
            const int BASE = 151;
            const int mixer = 2011;
            unchecked // Overflow is fine
            {
                int hash = BASE;

                hash = hash * mixer + HttpPort.GetHashCode();
                hash = hash * mixer + TcpPort.GetHashCode();

                if (BroadcastAddress != null)
                    hash = hash * mixer + BroadcastAddress.GetHashCode();

                if (HostName != null)
                    hash = hash * mixer + HostName.GetHashCode();

                return hash;
            }
        }

        public override bool Equals(object obj)
        {
            if (obj is NsqAddress)
                return Equals((NsqAddress)obj);

            return false;
        }

        public bool Equals(NsqAddress other)
        {
            return HttpPort == other.HttpPort
                   && TcpPort == other.TcpPort
                   && string.Equals(BroadcastAddress, other.BroadcastAddress, StringComparison.CurrentCultureIgnoreCase)
                   && string.Equals(HostName, other.HostName, StringComparison.CurrentCultureIgnoreCase);
        }
    }
}
