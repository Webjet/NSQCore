using System.Collections.Generic;
using Newtonsoft.Json;

namespace NSQCore
{
    /// <summary>
    /// Statistics from an nsqd instance.
    /// </summary>
    public class NsqStatistics
    {
        /// <summary>
        /// Describes the health of an nsqd instance. Will be "OK" for a healthy instance.
        /// </summary>
        [JsonProperty("health")]
        public string Health { get; internal set; }

        /// <summary>
        /// The version of nsqd running.
        /// </summary>
        [JsonProperty("version")]
        public string Version { get; internal set; }

        /// <summary>
        /// When the nsqd instance was started.
        /// </summary>
        [JsonProperty("start_time")]
        public int StartTime { get; internal set; }

        /// <summary>
        /// A list of statistics of the topics produced by this nsqd instance.
        /// </summary>
        [JsonProperty("topics")]
        public List<TopicStatistics> Topics { get; internal set; }

        internal NsqStatistics() { }
    }

    /// <summary>
    /// Statistics for a particular topic in an nsqd instance.
    /// </summary>
    public class TopicStatistics
    {
        /// <summary>
        /// The name of the topic.
        /// </summary>
        [JsonProperty("topic_name")]
        public Topic Name { get; internal set; }

        /// <summary>
        /// The number of messages yet to be handled in the topic.
        /// </summary>
        [JsonProperty("depth")]
        public long Depth { get; internal set; }

        /// <summary>
        /// The number of messages which have been persisted to backend storage.
        /// </summary>
        [JsonProperty("backend_depth")]
        public long BackendDepth { get; internal set; }

        /// <summary>
        /// The number of messages which have been delivered to this topic.
        /// </summary>
        [JsonProperty("message_count")]
        public long MessageCount { get; internal set; }

        /// <summary>
        /// Indicates whether the topic has been paused.
        /// A paused topic will not be delivered to any channels.
        /// </summary>
        [JsonProperty("paused")]
        public bool Paused { get; internal set; }

        /// <summary>
        /// A list of statistics of the channels for this topic.
        /// </summary>
        [JsonProperty("channels")]
        public List<ChannelStatistics> Channels { get; internal set; }

        internal TopicStatistics() { }
    }

    /// <summary>
    /// Statistics for a particular topic in an nsqd instance.
    /// </summary>
    public class ChannelStatistics
    {
        /// <summary>
        /// The name of the channel.
        /// </summary>
        [JsonProperty("channel_name")]
        public Channel Name { get; set; }

        /// <summary>
        /// The number of messages yet to be handled in the channel.
        /// </summary>
        [JsonProperty("depth")]
        public long Depth { get; internal set; }

        /// <summary>
        /// The number of messages in the channel which have been persisted to backend storage.
        /// </summary>
        [JsonProperty("backend_depth")]
        public long BackendDepth { get; internal set; }

        /// <summary>
        /// The number of messages in the channel which are currently in-flight (delivered to a consumer but
        /// not yet marked finished).
        /// </summary>
        [JsonProperty("in_flight_count")]
        public long InFlightCount { get; internal set; }

        /// <summary>
        /// the number of messages in the channel which have been re-queued in the channel with a timeout.
        /// </summary>
        [JsonProperty("deferred_count")]
        public long DeferredCount { get; internal set; }

        /// <summary>
        /// The number of messages which have been delivered to the channel.
        /// </summary>
        [JsonProperty("message_count")]
        public long MessageCount { get; internal set; }

        /// <summary>
        /// The number of times a message has been delivered to a consumer but not been finished before timing out.
        /// The timeout duration is determined by the configuration of the nsqd instance.
        /// </summary>
        [JsonProperty("timeout_count")]
        public long TimeoutCount { get; internal set; }

        /// <summary>
        /// Indicates whether the channel has been paused.
        /// A paused topic will not be delivered to any channels.
        /// </summary>
        [JsonProperty("paused")]
        public bool Paused { get; internal set; }

        internal ChannelStatistics() { }
    }
}
