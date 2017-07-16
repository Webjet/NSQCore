using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;

namespace NSQCore
{
    /// <summary>
    /// Configures the behavior of an NSQ consumer connection.
    /// </summary>
    public class ConsumerOptions
    {
        /// <summary>
        /// EndPoints for nsqlookupd instances to use. If any are present,
        /// this overrides the NsqEndPoint property.
        /// </summary>
        public HashSet<DnsEndPoint> LookupEndPoints { get; }

        /// <summary>
        /// The EndPoint to a single nsqd service to use. If any Lookup endpoints
        /// are present, this setting is ignored.
        /// </summary>
        public DnsEndPoint NsqEndPoint { get; set; }

        /// <summary>
        /// The topic to which to subscribe.
        /// </summary>
        public Topic Topic { get; set; }

        /// <summary>
        /// The channel to which to subscribe.
        /// </summary>
        public Channel Channel { get; set; }


        /// <summary>
        /// An identifier for this particular consumer.
        /// </summary>
        public string ClientId { get; set; }

        /// <summary>
        /// The hostname of the computer connected to NSQ.
        /// </summary>
        public string HostName { get; set; }

        public TimeSpan LookupPeriod { get; set; }

        /// <summary>
        /// The initial delay before attempting reconnection if the connection to NSQ fails.
        /// By default, the delay will be doubled on each attempt until reconnection, up to
        /// a maximum of <c>ReconnectionMaxDelay</c>.
        /// </summary>
        public TimeSpan ReconnectionDelay { get; set; }


        /// <summary>
        /// The maximum delay between reconnection attempts.
        /// </summary>
        public TimeSpan ReconnectionMaxDelay { get; set; }

        private const string LookupdKey = "lookupd";
        private const string NsqdKey = "nsqd";
        private const string TopicKey = "topic";
        private const string ChannelKey = "channel";
        private const string ClientidKey = "clientid";
        private const string HostnameKey = "hostname";
        private const string LookupperiodKey = "lookupperiod";
        private const string ReconnectiondelayKey = "reconnectiondelay";
        private const string ReconnectionmaxdelayKey = "reconnectionmaxdelay";

        private const int DefaultLookupdHttpPort = 4061;
        private const int DefaultNsqdTcpPort = 4050;

        /// <summary>
        /// Creates a default set of options.
        /// </summary>
        public ConsumerOptions()
        {
            LookupEndPoints = new HashSet<DnsEndPoint>();

            ClientId = "NSQCore";
            HostName = "localhost";// Dns.GetHostName();
            LookupPeriod = TimeSpan.FromSeconds(15);
            ReconnectionDelay = TimeSpan.FromSeconds(1);
            ReconnectionMaxDelay = TimeSpan.FromSeconds(30);
        }

        /// <summary>
        /// Parses a connection string into a <c>ConsumerOptions</c> instance.
        /// </summary>
        /// <param name="connectionString">A semicolon-delimited list of key=value pairs of connection options.</param>
        /// <returns></returns>
        public static ConsumerOptions Parse(string connectionString)
        {
            var options = new ConsumerOptions();
            var parts = ParseIntoSegments(connectionString);

            if (parts.Contains(LookupdKey))
            {
                foreach (var endPoint in ParseEndPoints(parts[LookupdKey], DefaultLookupdHttpPort))
                {
                    options.LookupEndPoints.Add(endPoint);
                }

            }
            else if (parts.Contains(NsqdKey))
            {
                options.NsqEndPoint = ParseEndPoints(parts[NsqdKey], DefaultNsqdTcpPort).Last();
            }
            else
            {
                throw new ArgumentException("Must provide either nsqlookupd or nsqd endpoints");
            }

            if (parts.Contains(ClientidKey))
            {
                options.ClientId = parts[ClientidKey].Last();
            }

            if (parts.Contains(HostnameKey))
            {
                options.HostName = parts[HostnameKey].Last();
            }

            if (parts.Contains(LookupperiodKey))
            {
                options.LookupPeriod = TimeSpan.FromSeconds(int.Parse(parts[LookupperiodKey].Last()));
            }

            if (parts.Contains(TopicKey))
            {
                options.Topic = parts[TopicKey].Last();
            }

            if (parts.Contains(ChannelKey))
            {
                options.Channel = parts[ChannelKey].Last();
            }

            if (parts.Contains(ReconnectiondelayKey))
            {
                options.ReconnectionDelay = TimeSpan.FromSeconds(int.Parse(parts[ReconnectiondelayKey].Last()));
            }

            if (parts.Contains(ReconnectionmaxdelayKey))
            {
                options.ReconnectionMaxDelay = TimeSpan.FromSeconds(int.Parse(parts[ReconnectionmaxdelayKey].Last()));
            }

            return options;
        }

        private static ILookup<string, string> ParseIntoSegments(string connectionString)
        {
            return
                connectionString.Split(new[] { ";" }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(part => part.Split(new[] { "=" }, StringSplitOptions.RemoveEmptyEntries))
                    .Where(part => part.Length == 1 || part.Length == 2)
                    .Select(part =>
                    {
                        if (part.Length == 2)
                            return part;
                        return new[] { LookupdKey, part[0] };
                    })
                    .ToLookup(
                        part => part[0].ToLowerInvariant().Trim(),
                        part => part[1].Trim());

        }

        private static IEnumerable<DnsEndPoint> ParseEndPoints(IEnumerable<string> list, int defaultPort)
        {
            return list
                .Select(endpoint => endpoint.Trim())
                .Select(endpoint => endpoint.Split(new[] { ':' }, 2))
                .Select(endpointParts => new DnsEndPoint(endpointParts[0], endpointParts.Length == 2 ? int.Parse(endpointParts[1]) : defaultPort));
        }
    }
}
