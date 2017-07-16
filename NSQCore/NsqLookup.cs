using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace NSQCore
{
    /// <summary>
    /// A client for querying an instance of nsqlookupd.
    /// </summary>
    public class NsqLookup
    {
        private readonly HttpClient _httpClient = new HttpClient();
        private readonly SemaphoreSlim _webClientLock = new SemaphoreSlim(1, 1);
        private static readonly byte[] Empty = new byte[0];
        /// <summary>
        /// Creates a new instance of <c>NsqLookup</c>.
        /// </summary>
        /// <param name="host">The host name or IP address of the nsqlookupd instance.</param>
        /// <param name="port">The HTTP port of the nsqlookupd instance.</param>
        public NsqLookup(string host, int port)
        {
            _httpClient.BaseAddress = new Uri("http://" + host + ":" + port);
        }

        /// <summary>
        /// Looks up the nsqd instances which are producing a topic.
        /// </summary>
        public Task<List<NsqAddress>> LookupAsync(Topic topic)
        {
            return RequestListAsync("/lookup?topic=" + topic, response =>
            {
                return
                    ((JArray)response["producers"])
                    .Select(producer => new NsqAddress(
                        (string)producer["broadcast_address"],
                        (string)producer["hostname"],
                        (int)producer["tcp_port"],
                        (int)producer["http_port"]))
                    .ToList();
            });
        }

        /// <summary>
        /// Queries the list of topics known to this nsqlookupd instance.
        /// </summary>
        public Task<List<Topic>> TopicsAsync()
        {
            return RequestListAsync("/topics", response =>
            {
                return response["topics"]
                    .Select(t => new Topic((string)t))
                    .ToList();
            });
        }

        /// <summary>
        /// Queries the channels known to this nsqlookupd instance.
        /// </summary>
        /// <param name="topic">The topic to query.</param>
        public Task<List<Channel>> ChannelsAsync(Topic topic)
        {
            return RequestListAsync("/channels?topic=" + topic, response =>
            {
                return response["channels"]
                    .Select(t => new Channel((string)t))
                    .ToList();
            });
        }

        /// <summary>
        /// Queries the nsqd nodes known to this nsqlookupd instance.
        /// </summary>
        public Task<List<NsqAddress>> NodesAsync()
        {
            return RequestListAsync("/nodes", response =>
            {
                return
                    ((JArray)response["producers"])
                    .Select(producer => new NsqAddress(
                        (string)producer["broadcast_address"],
                        (string)producer["hostname"],
                        (int)producer["tcp_port"],
                        (int)producer["http_port"]))
                    .ToList();
            });
        }

        /// <summary>
        /// Deletes a topic.
        /// </summary>
        public Task DeleteTopicAsync(Topic topic)
        {
           return PostAsync("/topic/delete?topic=" + topic, _ => true);
        }

        /// <summary>
        /// Deletes a channel.
        /// </summary>
        public Task DeleteChannelAsync(Topic topic, Channel channel)
        {
            var url = "/channel/delete?topic=" + topic + "&channel=" + channel;
            return PostAsync(url, _ => true);
        }

        /// <summary>
        /// Tombstones a topic for an nsqd instance.
        /// </summary>
        public Task TombstoneTopicAsync(Topic topic, NsqAddress producer)
        {
            var url = string.Format("/topic/tombstone?topic={0}&node={1}:{2}", topic, producer.BroadcastAddress, producer.HttpPort);
            return PostAsync(url, _ => true);
        }

        /// <summary>
        /// Queries the version of the nsqlookupd instance.
        /// </summary>
        public Task<string> VersionAsync()
        {
            return GetAsync("/info", response => (string)response["version"]);
        }

        /// <summary>
        /// Queries the nsqlookupd instance for liveliness.
        /// </summary>
        /// <returns>True if nsqlookupd returns "OK".</returns>
        public Task<bool> PingAsync()
        {
            var response = _httpClient.GetAsync("ping").ConfigureAwait(false).GetAwaiter().GetResult();
            return Task.FromResult(response.IsSuccessStatusCode);
        }

        private async Task<List<T>> RequestListAsync<T>(string url, Func<JObject, List<T>> handler)
        {
            var result = await GetAsync(url, handler).ConfigureAwait(false);
            return result ?? new List<T>();
        }


        private Task<T> PostAsync<T>(string url, Func<byte[], T> handler)
        {
            return PostAsync(url, Empty, handler);
        }

        private Task<T> PostAsync<T>(string url, byte[] data, Func<byte[], T> handler)
        {
            return HttpClientWrapper.PostAsync(_httpClient, _webClientLock, url, data, handler);
        }

        private Task<T> GetAsync<T>(string url, Func<JObject, T> handler)
        {
            return HttpClientWrapper.GetAsync(_httpClient, _webClientLock, url, handler);
        }
    }
}
