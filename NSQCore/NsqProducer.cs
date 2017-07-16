using System;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace NSQCore
{
    /// <summary>
    /// A client for nsqd which delivers messages using the HTTP protocol.
    /// </summary>
    public sealed class NsqProducer
    {
        private readonly HttpClient _httpClient = new HttpClient();
        private readonly SemaphoreSlim _webClientLock = new SemaphoreSlim(1, 1);

        private static readonly byte[] Empty = new byte[0];

        /// <summary>
        /// Creates a new client.
        /// </summary>
        /// <param name="host">The host name or IP address of the nsqd instance.</param>
        /// <param name="port">The HTTP port of the nsqd instance.</param>
        public NsqProducer(string host, int port)
        {
            _httpClient.BaseAddress = new Uri("http://" + host + ":" + port);
        }

        /// <summary>
        /// Creates a new topic on the nsqd instance.
        /// </summary>
        public Task CreateTopicAsync(Topic topic)
        {
            return PostAsync("/topic/create?topic=" + topic, _ => true);
        }

        /// <summary>
        /// Deletes a topic from the nsqd instance.
        /// </summary>
        public Task DeleteTopicAsync(Topic topic)
        {
            return PostAsync("/topic/delete?topic=" + topic, _ => true);
        }

        /// <summary>
        /// Clears all messages from the topic on the nsqd instance.
        /// </summary>
        public Task EmptyTopicAsync(Topic topic)
        {
            return PostAsync("/topic/empty?topic=" + topic, _ => true);
        }

        /// <summary>
        /// Pauses a topic on the nsqd instance.
        /// </summary>
        public Task PauseTopicAsync(Topic topic)
        {
            return PostAsync("/topic/pause?topic=" + topic, _ => true);
        }

        /// <summary>
        /// Unpauses a topic on the nsqd instance.
        /// </summary>
        public Task UnpauseTopicAsync(Topic topic)
        {
            return PostAsync("/topic/unpause?topic=" + topic, _ => true);
        }

        /// <summary>
        /// Creates a channel on the nsqd instance.
        /// </summary>
        public Task CreateChannelAsync(Topic topic, Channel channel)
        {
            return PostAsync("/channel/create?topic=" + topic + "&channel=" + channel, _ => true);
        }

        /// <summary>
        /// Deletes a channel from a topic on the nsqd instance.
        /// </summary>
        public Task DeleteChannelAsync(Topic topic, Channel channel)
        {
            return PostAsync("/channel/delete?topic=" + topic + "&channel=" + channel, _ => true);
        }

        /// <summary>
        /// Clears all messages from a channel on the nsqd instance.
        /// </summary>
        public Task EmptyChannelAsync(Topic topic, Channel channel)
        {
            return PostAsync("/channel/empty?topic=" + topic + "&channel=" + channel, _ => true);
        }

        /// <summary>
        /// Pauses a channel on the nsqd instance.
        /// </summary>
        public Task PauseChannelAsync(Topic topic, Channel channel)
        {
            return PostAsync("/channel/pause?topic=" + topic + "&channel=" + channel, _ => true);
        }

        /// <summary>
        /// Unpauses a channel on the nsqd instance.
        /// </summary>
        public Task UnpauseChannelAsync(Topic topic, Channel channel)
        {
            return PostAsync("/channel/unpause?topic=" + topic + "&channel=" + channel, _ => true);
        }

        /// <summary>
        /// Determines the liveliness of the nsqd instance.
        /// </summary>
        public Task<bool> PingAsync()
        {
            var response=  _httpClient.GetAsync("ping").ConfigureAwait(false).GetAwaiter().GetResult();
            return Task.FromResult(response.IsSuccessStatusCode);
        }

        /// <summary>
        /// Queries for runtime statistics of the nsqd instance.
        /// </summary>
        public Task<NsqStatistics> StatisticsAsync()
        {
            return GetAsync("/stats?format=json", response => response.ToObject<NsqStatistics>());
        }

        

        /// <summary>
        /// Publishes a message to the nsqd instance.
        /// </summary>
        public Task PublishAsync(Topic topic, MessageBody data)
        {
            if (data.IsNull)
                throw new ArgumentOutOfRangeException("data", "Must provide data to publish");

            return PostAsync("/pub?topic=" + topic, data, _ => true);
        }

        /// <summary>
        /// Publishes multiple messages to the nsqd instance in a single HTTP request.
        /// </summary>
        public Task PublishAsync(Topic topic, MessageBody[] messages)
        {
            byte[] totalArray;
            checked
            {
                var dataLength = messages.Sum(msg => ((byte[])msg).Length);
                var totalLength = dataLength + (4 * messages.Length) + 4;
                totalArray = new byte[totalLength];
            }

            Array.Copy(BitConverter.GetBytes(messages.Length), totalArray, 4);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(totalArray, 0, 4);

            var offsetIntoTotalArray = 4;
            foreach (MessageBody messageBody in messages)
            {
                var messageLength = ((byte[])messageBody).Length;
                Array.Copy(BitConverter.GetBytes(messageLength), 0, totalArray, offsetIntoTotalArray, 4);
                if (BitConverter.IsLittleEndian)
                    Array.Reverse(totalArray, offsetIntoTotalArray, 4);
                offsetIntoTotalArray += 4;
                Array.Copy(messageBody, 0, totalArray, offsetIntoTotalArray, messageLength);
                offsetIntoTotalArray += messageLength;
            }

            return PostAsync("/mpub?binary=true&topic=" + topic, totalArray, _ => true);
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
