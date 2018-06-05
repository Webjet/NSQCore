using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace NSQCore
{
    public class NsqLookupConsumer : INsqConsumer
    {
        private readonly Dictionary<DnsEndPoint, NsqTcpConnection> _connections = new Dictionary<DnsEndPoint, NsqTcpConnection>();
        private readonly List<NsqLookup> _lookupServers = new List<NsqLookup>();
        private readonly ConsumerOptions _options;
        private readonly Task _firstConnectionTask;
        private readonly TaskCompletionSource<bool> _firstConnectionTaskCompletionSource = new TaskCompletionSource<bool>();

        private Timer _lookupTimer;
        private bool _firstDiscoveryCycle = true;
        private int _maxInFlight;
        private int _started;
        private bool _disposed;
        private ILogger _logger;

        // No need to ever reconnect, we'll reconnect on the next lookup cycle
        private static readonly NoRetryBackoffStrategy NoRetryBackoff = new NoRetryBackoffStrategy();

        public EventHandler<InternalMessageEventArgs> InternalMessages;
        public EventHandler<DiscoveryEventArgs> DiscoveryCompleted;

        internal void OnDiscoveryCompleted(List<NsqAddress> addresses)
        {
            var handler = DiscoveryCompleted;
            handler?.Invoke(this, new DiscoveryEventArgs(addresses));
        }

        internal void OnInternalMessage(string format, object arg0)
        {
            var handler = InternalMessages;
            handler?.Invoke(this, new InternalMessageEventArgs(string.Format(format, arg0)));
        }

        internal void OnInternalMessage(string format, params object[] args)
        {
            var handler = InternalMessages;
            handler?.Invoke(this, new InternalMessageEventArgs(string.Format(format, args)));
        }

        public NsqLookupConsumer(ConsumerOptions options, ILogger logger)
        {
            _options = options;
            _logger = logger;

            foreach (var lookupEndPoint in options.LookupEndPoints)
            {
                _lookupServers.Add(new NsqLookup(lookupEndPoint.Host, lookupEndPoint.Port));
            }

            _firstConnectionTask = _firstConnectionTaskCompletionSource.Task;
        }

        public NsqLookupConsumer(string connectionString, ILogger logger)
            : this(ConsumerOptions.Parse(connectionString), logger)
        {
        }

        public async Task ConnectAndWaitAsync(MessageHandler handler)
        {
            ThrowIfDisposed();
            Connect(handler);
            await _firstConnectionTask.ConfigureAwait(false);
        }

        public void Connect(MessageHandler handler)
        {
            ThrowIfDisposed();
            var wasStarted = Interlocked.CompareExchange(ref _started, 1, 0);
            if (wasStarted != 0) return;

            OnInternalMessage("Starting LookupTask");
            _lookupTimer = new Timer(LookupTask, handler, TimeSpan.Zero, _options.LookupPeriod);
        }

        private void LookupTask(object messageHandler)
        {
            MessageHandler handler = (MessageHandler)messageHandler;
            // OnInternalMessage("Begin lookup cycle");
            int beginningCount, endingCount,
                added = 0, removed = 0;

            List<NsqAddress> nsqAddresses;
            lock (_connections)
            {
                var tasks = _lookupServers.Select(server => server.LookupAsync(_options.Topic)).ToList();
                var delay = Task.Delay(5000);
                Task.WhenAny(Task.WhenAll(tasks), delay).Wait();

                nsqAddresses =
                    tasks.Where(t => t.Status == TaskStatus.RanToCompletion)
                        .SelectMany(t => t.Result)
                        .Distinct()
                        .ToList();

                var servers =
                    nsqAddresses
                        .Select(add => new DnsEndPoint(add.BroadcastAddress, add.TcpPort))
                        .ToList();

                var currentEndPoints = _connections.Keys.ToList();
                var newEndPoints = servers.Except(currentEndPoints).ToList();
                var removedEndPoints = currentEndPoints.Except(servers).ToList();

                foreach (var endPoint in removedEndPoints)
                {
                    var connection = _connections[endPoint];
                    _connections.Remove(endPoint);
                    connection.Dispose();
                    removed++;
                }

                foreach (var endPoint in newEndPoints)
                {
                    if (!_connections.ContainsKey(endPoint))
                    {
                        var connection = new NsqTcpConnection(endPoint, _options, NoRetryBackoff);
                        connection.InternalMessages +=
                            (sender, e) => OnInternalMessage("{0}: {1}", endPoint, e.Message);
                        try
                        {
                            connection.Connect(handler);
                            _connections[endPoint] = connection;
                            added++;
                        }
                        catch (Exception ex)
                        {
                            OnInternalMessage("Connection to endpoint {0} failed: {1}", endPoint, ex.Message);
                        }
                    }
                }

                beginningCount = currentEndPoints.Count;
                endingCount = _connections.Count;

                SetMaxInFlightWithoutWaitingForInitialConnectionAsync(_maxInFlight).Wait();
            }

            var message = string.Format("End lookup cycle. BeginningCount = {0}, EndingCount = {1}, Added = {2}, Removed = {3}", beginningCount, endingCount, added, removed);
            OnInternalMessage(message);
            if (added != 0 || removed != 0 || _firstDiscoveryCycle)
            {
                _logger?.LogInformation(message);
            }

            if (_firstDiscoveryCycle)
            {
                _firstConnectionTaskCompletionSource.TrySetResult(true);
                _firstDiscoveryCycle = false;
            }

            OnDiscoveryCompleted(nsqAddresses);

            
        }

        public async Task PublishAsync(Topic topic, MessageBody message)
        {
            ThrowIfDisposed();
            await _firstConnectionTask.ConfigureAwait(false);

            List<NsqTcpConnection> connections;
            lock (_connections)
            {
                connections = _connections.Values.ToList();
            }

            if (connections.Count == 0)
                throw new CommunicationException("No NSQ connections are available");

            foreach (var thing in connections)
            {
                try
                {
                    await thing.PublishAsync(topic, message).ConfigureAwait(false);
                    return;
                }
                catch
                {
                }
            }

            throw new CommunicationException("Write failed against all NSQ connections");
        }

        public async Task SetMaxInFlightAsync(int maxInFlight)
        {
            ThrowIfDisposed();
            _maxInFlight = maxInFlight;
            await _firstConnectionTask.ConfigureAwait(false);
            await SetMaxInFlightWithoutWaitingForInitialConnectionAsync(maxInFlight).ConfigureAwait(false);
        }

        // I need a better name for this
        private async Task SetMaxInFlightWithoutWaitingForInitialConnectionAsync(int maxInFlight)
        {
            if (maxInFlight < 0)
                throw new ArgumentOutOfRangeException("maxInFlight", "MaxInFlight must be non-negative.");

            List<NsqTcpConnection> connections;
            lock (_connections)
            {
                connections = _connections.Values.ToList();
            }

            if (connections.Count == 0) return;

            int maxInFlightPerServer = maxInFlight / connections.Count;
            int remainder = maxInFlight % connections.Count;

            var tasks = new List<Task>(connections.Count);
            foreach (var connection in connections)
            {
                int max = maxInFlightPerServer;
                if (remainder > 0)
                {
                    remainder -= 1;
                    if (max < int.MaxValue)
                        max += 1;
                }

                var setMaxTask = connection.SetMaxInFlightAsync(max)
                    .ContinueWith(t =>
                    {
                        if (t.Status == TaskStatus.Faulted)
                        {
                            connection.Dispose();
                            OnInternalMessage("Setting MaxInFlight on {0} threw: {1}", connection.EndPoint, t.Exception.GetBaseException().Message);
                        }
                    });
                tasks.Add(setMaxTask);
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);
        }

        public void Dispose()
        {
            lock (_connections)
            {
                _disposed = true;

                _lookupTimer.Dispose();

                foreach (var connection in _connections.Values)
                    connection.Dispose();
            }
        }

        private void ThrowIfDisposed()
        {
            if (_disposed) throw new ObjectDisposedException("NsqLookupConnection");
        }
    }
}
