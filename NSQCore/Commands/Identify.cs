using System;
using System.IO;
using System.Linq;
using System.Text;
using Newtonsoft.Json;

namespace NSQCore.Commands
{
    internal class Identify : ICommand
    {
        private readonly JsonSerializer _serializer = new JsonSerializer();
        private readonly IdentifyRequest _identifyRequest;

        public Identify(ConsumerOptions options)
        {
            _identifyRequest = new IdentifyRequest
            {
                client_id = options.ClientId,
                hostname = options.HostName,
                feature_negotiation = true,
                tls_v1 = false,
                snappy = false,
                deflate = false
            };
        }

        private static readonly byte[] IdentifyLf = Encoding.ASCII.GetBytes("IDENTIFY\n");

        public byte[] ToByteArray()
        {
            byte[] body;
            using (var stream = new MemoryStream(1024))
            using (var writer = new StreamWriter(stream))
            using (var jsonWriter = new JsonTextWriter(writer))
            {
                _serializer.Serialize(jsonWriter, _identifyRequest);
                jsonWriter.Flush();
                writer.Flush();
                body = stream.ToArray();
            }

            byte[] length = BitConverter.GetBytes(body.Length);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(length);

            return IdentifyLf
                .Concat(length)
                .Concat(body)
                .ToArray();
        }

        public IdentifyResponse ParseIdentifyResponse(byte[] data)
        {
            using (var stream = new MemoryStream(data))
            using (var reader = new StreamReader(stream, Encoding.UTF8))
            using (var jsonReader = new JsonTextReader(reader))
            {
                return _serializer.Deserialize<IdentifyResponse>(jsonReader);
            }
        }

        private class IdentifyRequest
        {
            public string client_id { get; set; }
            public string hostname { get; set; }
            public bool feature_negotiation { get; set; }

            [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
            public int? heartbeat_interval { get; set; }

            [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
            public int? output_buffer_size { get; set; }

            [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
            public int? output_buffer_timeout { get; set; }

            public bool tls_v1 { get; set; }
            public bool snappy { get; set; }
            public bool deflate { get; set; }
            public int deflate_level { get; set; }

            public int sample_rate { get; set; }
            [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
            public int? msg_timeout { get; set; }
            public string user_agent { get { return "NSQCore/1.0"; } }
        }
    }

    internal class IdentifyResponse
    {
        [JsonProperty("max_rdy_count")]
        public long MaxReadyCount { get; set; }

        [JsonProperty("version")]
        public string Version { get; set; }

        [JsonProperty("max_msg_timeout")]
        public long MaxMessageTimeoutMilliseconds { get; set; }

        [JsonProperty("msg_timeout")]
        public long MessageTimeoutMilliseconds { get; set; }

        [JsonProperty("tls_v1")]
        public bool Tls { get; set; }

        [JsonProperty("deflate")]
        public bool Deflate { get; set; }

        [JsonProperty("deflate_level")]
        public int DeflateLevel { get; set; }

        [JsonProperty("max_deflate_level")]
        public int MaxDeflateLevel { get; set; }

        [JsonProperty("snappy")]
        public bool Snappy { get; set; }

        [JsonProperty("sample_rate")]
        public int SampleRate { get; set; }

        [JsonProperty("auth_required")]
        public bool AuthRequired { get; set; }

        [JsonProperty("output_buffer_size")]
        public int OutputBufferSize { get; set; }

        [JsonProperty("output_buffer_timeout")]
        public long OutputBufferTimeout { get; set; }
    }
}
