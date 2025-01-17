﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Pipelines.Sockets.Unofficial;
using StackExchange.Redis.Maintenance;
using StackExchange.Redis.Profiling;

namespace StackExchange.Redis
{
    /// <summary>
    /// Represents an inter-related group of connections to redis servers.
    /// A reference to this should be held and re-used.
    /// </summary>
    /// <remarks>https://stackexchange.github.io/StackExchange.Redis/PipelinesMultiplexers</remarks>
    public sealed partial class ConnectionMultiplexer : IInternalConnectionMultiplexer // implies : IConnectionMultiplexer and : IDisposable
    {
        [Flags]
        private enum FeatureFlags
        {
            None,
            PreventThreadTheft = 1,
        }

        private static FeatureFlags s_featureFlags;

        /// <summary>
        /// Enables or disables a feature flag.
        /// This should only be used under support guidance, and should not be rapidly toggled.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [Browsable(false)]
        public static void SetFeatureFlag(string flag, bool enabled)
        {
            if (Enum.TryParse<FeatureFlags>(flag, true, out var flags))
            {
                if (enabled) s_featureFlags |= flags;
                else s_featureFlags &= ~flags;
            }
        }

        static ConnectionMultiplexer()
        {
            bool value = false;
            try
            {   // attempt to detect a known problem scenario
                value = SynchronizationContext.Current?.GetType()?.Name
                    == "LegacyAspNetSynchronizationContext";
            }
            catch { }
            SetFeatureFlag(nameof(FeatureFlags.PreventThreadTheft), value);
        }

        /// <summary>
        /// Returns the state of a feature flag.
        /// This should only be used under support guidance.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [Browsable(false)]
        public static bool GetFeatureFlag(string flag)
            => Enum.TryParse<FeatureFlags>(flag, true, out var flags)
            && (s_featureFlags & flags) == flags;

        internal static bool PreventThreadTheft => (s_featureFlags & FeatureFlags.PreventThreadTheft) != 0;

        private static TaskFactory _factory = null;

#if DEBUG
        private static int _collectedWithoutDispose;
        internal static int CollectedWithoutDispose => Thread.VolatileRead(ref _collectedWithoutDispose);
        /// <summary>
        /// Invoked by the garbage collector.
        /// </summary>
        ~ConnectionMultiplexer()
        {
            Interlocked.Increment(ref _collectedWithoutDispose);
        }
#endif

        bool IInternalConnectionMultiplexer.AllowConnect
        {
            get => AllowConnect;
            set => AllowConnect = value;
        }

        bool IInternalConnectionMultiplexer.IgnoreConnect
        {
            get => IgnoreConnect;
            set => IgnoreConnect = value;
        }

        /// <summary>
        /// For debugging: when not enabled, servers cannot connect.
        /// </summary>
        internal volatile bool AllowConnect = true;

        /// <summary>
        /// For debugging: when not enabled, end-connect is silently ignored (to simulate a long-running connect).
        /// </summary>
        internal volatile bool IgnoreConnect;

        /// <summary>
        /// Tracks overall connection multiplexer counts.
        /// </summary>
        internal int _connectAttemptCount = 0, _connectCompletedCount = 0, _connectionCloseCount = 0;

        /// <summary>
        /// Provides a way of overriding the default Task Factory.
        /// If not set, it will use the default <see cref="Task.Factory"/>.
        /// Useful when top level code sets it's own factory which may interfere with Redis queries.
        /// </summary>
        [Obsolete("No longer used, will be removed in 3.0.")]
        public static TaskFactory Factory
        {
            get => _factory ?? Task.Factory;
            set => _factory = value;
        }

        /// <summary>
        /// Get summary statistics associated with all servers in this multiplexer.
        /// </summary>
        public ServerCounters GetCounters()
        {
            var snapshot = GetServerSnapshot();

            var counters = new ServerCounters(null);
            for (int i = 0; i < snapshot.Length; i++)
            {
                counters.Add(snapshot[i].GetCounters());
            }
            return counters;
        }

        /// <summary>
        /// Gets the client-name that will be used on all new connections.
        /// </summary>
        /// <remarks>
        /// We null coalesce here instead of in Options so that we don't populate it everywhere (e.g. .ToString()), given it's a default.
        /// </remarks>
        public string ClientName => RawConfig.ClientName ?? RawConfig.Defaults.ClientName;

        /// <summary>
        /// Gets the configuration of the connection.
        /// </summary>
        public string Configuration => RawConfig.ToString();

        internal void OnConnectionFailed(EndPoint endpoint, ConnectionType connectionType, ConnectionFailureType failureType, Exception exception, bool reconfigure, string physicalName)
        {
            if (_isDisposed) return;
            var handler = ConnectionFailed;
            if (handler != null)
            {
                CompleteAsWorker(new ConnectionFailedEventArgs(handler, this, endpoint, connectionType, failureType, exception, physicalName));
            }
            if (reconfigure)
            {
                ReconfigureIfNeeded(endpoint, false, "connection failed");
            }
        }

        internal void OnInternalError(Exception exception, EndPoint endpoint = null, ConnectionType connectionType = ConnectionType.None, [CallerMemberName] string origin = null)
        {
            try
            {
                if (_isDisposed) return;
                Trace("Internal error: " + origin + ", " + exception == null ? "unknown" : exception.Message);
                var handler = InternalError;
                if (handler != null)
                {
                    CompleteAsWorker(new InternalErrorEventArgs(handler, this, endpoint, connectionType, exception, origin));
                }
            }
            catch
            {
                // Our internal error event failed...whatcha gonna do, exactly?
            }
        }

        internal void OnConnectionRestored(EndPoint endpoint, ConnectionType connectionType, string physicalName)
        {
            if (_isDisposed) return;
            var handler = ConnectionRestored;
            if (handler != null)
            {
                CompleteAsWorker(new ConnectionFailedEventArgs(handler, this, endpoint, connectionType, ConnectionFailureType.None, null, physicalName));
            }
            ReconfigureIfNeeded(endpoint, false, "connection restored");
        }

        private void OnEndpointChanged(EndPoint endpoint, EventHandler<EndPointEventArgs> handler)
        {
            if (_isDisposed) return;
            if (handler != null)
            {
                CompleteAsWorker(new EndPointEventArgs(handler, this, endpoint));
            }
        }

        internal void OnConfigurationChanged(EndPoint endpoint) => OnEndpointChanged(endpoint, ConfigurationChanged);
        internal void OnConfigurationChangedBroadcast(EndPoint endpoint) => OnEndpointChanged(endpoint, ConfigurationChangedBroadcast);

        /// <summary>
        /// Raised when a server replied with an error message.
        /// </summary>
        public event EventHandler<RedisErrorEventArgs> ErrorMessage;
        internal void OnErrorMessage(EndPoint endpoint, string message)
        {
            if (_isDisposed) return;
            var handler = ErrorMessage;
            if (handler != null)
            {
                CompleteAsWorker(new RedisErrorEventArgs(handler, this, endpoint, message));
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2202:Do not dispose objects multiple times")]
        private static void Write<T>(ZipArchive zip, string name, Task task, Action<T, StreamWriter> callback)
        {
            var entry = zip.CreateEntry(name, CompressionLevel.Optimal);
            using (var stream = entry.Open())
            using (var writer = new StreamWriter(stream))
            {
                TaskStatus status = task.Status;
                switch (status)
                {
                    case TaskStatus.RanToCompletion:
                        T val = ((Task<T>)task).Result;
                        callback(val, writer);
                        break;
                    case TaskStatus.Faulted:
                        writer.WriteLine(string.Join(", ", task.Exception.InnerExceptions.Select(x => x.Message)));
                        break;
                    default:
                        writer.WriteLine(status.ToString());
                        break;
                }
            }
        }
        /// <summary>
        /// Write the configuration of all servers to an output stream.
        /// </summary>
        /// <param name="destination">The destination stream to write the export to.</param>
        /// <param name="options">The options to use for this export.</param>
        public void ExportConfiguration(Stream destination, ExportOptions options = ExportOptions.All)
        {
            if (destination == null) throw new ArgumentNullException(nameof(destination));

            // What is possible, given the command map?
            ExportOptions mask = 0;
            if (CommandMap.IsAvailable(RedisCommand.INFO)) mask |= ExportOptions.Info;
            if (CommandMap.IsAvailable(RedisCommand.CONFIG)) mask |= ExportOptions.Config;
            if (CommandMap.IsAvailable(RedisCommand.CLIENT)) mask |= ExportOptions.Client;
            if (CommandMap.IsAvailable(RedisCommand.CLUSTER)) mask |= ExportOptions.Cluster;
            options &= mask;

            using (var zip = new ZipArchive(destination, ZipArchiveMode.Create, true))
            {
                var arr = GetServerSnapshot();
                foreach (var server in arr)
                {
                    const CommandFlags flags = CommandFlags.None;
                    if (!server.IsConnected) continue;
                    var api = GetServer(server.EndPoint);

                    List<Task> tasks = new List<Task>();
                    if ((options & ExportOptions.Info) != 0)
                    {
                        tasks.Add(api.InfoRawAsync(flags: flags));
                    }
                    if ((options & ExportOptions.Config) != 0)
                    {
                        tasks.Add(api.ConfigGetAsync(flags: flags));
                    }
                    if ((options & ExportOptions.Client) != 0)
                    {
                        tasks.Add(api.ClientListAsync(flags: flags));
                    }
                    if ((options & ExportOptions.Cluster) != 0)
                    {
                        tasks.Add(api.ClusterNodesRawAsync(flags: flags));
                    }

                    WaitAllIgnoreErrors(tasks.ToArray());

                    int index = 0;
                    var prefix = Format.ToString(server.EndPoint);
                    if ((options & ExportOptions.Info) != 0)
                    {
                        Write<string>(zip, prefix + "/info.txt", tasks[index++], WriteNormalizingLineEndings);
                    }
                    if ((options & ExportOptions.Config) != 0)
                    {
                        Write<KeyValuePair<string, string>[]>(zip, prefix + "/config.txt", tasks[index++], (settings, writer) =>
                        {
                            foreach (var setting in settings)
                            {
                                writer.WriteLine("{0}={1}", setting.Key, setting.Value);
                            }
                        });
                    }
                    if ((options & ExportOptions.Client) != 0)
                    {
                        Write<ClientInfo[]>(zip, prefix + "/clients.txt", tasks[index++], (clients, writer) =>
                        {
                            if (clients == null)
                            {
                                writer.WriteLine(NoContent);
                            }
                            else
                            {
                                foreach (var client in clients)
                                {
                                    writer.WriteLine(client.Raw);
                                }
                            }
                        });
                    }
                    if ((options & ExportOptions.Cluster) != 0)
                    {
                        Write<string>(zip, prefix + "/nodes.txt", tasks[index++], WriteNormalizingLineEndings);
                    }
                }
            }
        }

        internal async Task MakePrimaryAsync(ServerEndPoint server, ReplicationChangeOptions options, LogProxy log)
        {
            var cmd = server.GetFeatures().ReplicaCommands ? RedisCommand.REPLICAOF : RedisCommand.SLAVEOF;
            CommandMap.AssertAvailable(cmd);

            if (!RawConfig.AllowAdmin) throw ExceptionFactory.AdminModeNotEnabled(IncludeDetailInExceptions, cmd, null, server);

            if (server == null) throw new ArgumentNullException(nameof(server));
            var srv = new RedisServer(this, server, null);
            if (!srv.IsConnected) throw ExceptionFactory.NoConnectionAvailable(this, null, server, GetServerSnapshot(), command: cmd);

            const CommandFlags flags = CommandFlags.NoRedirect;
            Message msg;

            log?.WriteLine($"Checking {Format.ToString(srv.EndPoint)} is available...");
            try
            {
                await srv.PingAsync(flags); // if it isn't happy, we're not happy
            }
            catch (Exception ex)
            {
                log?.WriteLine($"Operation failed on {Format.ToString(srv.EndPoint)}, aborting: {ex.Message}");
                throw;
            }

            var nodes = GetServerSnapshot().ToArray(); // Have to array because async/await
            RedisValue newPrimary = Format.ToString(server.EndPoint);

            RedisKey tieBreakerKey = default(RedisKey);
            // try and write this everywhere; don't worry if some folks reject our advances
            if ((options & ReplicationChangeOptions.SetTiebreaker) != 0 && !string.IsNullOrWhiteSpace(RawConfig.TieBreaker)
                && CommandMap.IsAvailable(RedisCommand.SET))
            {
                tieBreakerKey = RawConfig.TieBreaker;

                foreach (var node in nodes)
                {
                    if (!node.IsConnected || node.IsReplica) continue;
                    log?.WriteLine($"Attempting to set tie-breaker on {Format.ToString(node.EndPoint)}...");
                    msg = Message.Create(0, flags | CommandFlags.FireAndForget, RedisCommand.SET, tieBreakerKey, newPrimary);
                    try
                    {
                        await node.WriteDirectAsync(msg, ResultProcessor.DemandOK);
                    }
                    catch { }
                }
            }

            // stop replicating, promote to a standalone primary
            log?.WriteLine($"Making {Format.ToString(srv.EndPoint)} a primary...");
            try
            {
                await srv.ReplicaOfAsync(null, flags);
            }
            catch (Exception ex)
            {
                log?.WriteLine($"Operation failed on {Format.ToString(srv.EndPoint)}, aborting: {ex.Message}");
                throw;
            }

            // also, in case it was a replica a moment ago, and hasn't got the tie-breaker yet, we re-send the tie-breaker to this one
            if (!tieBreakerKey.IsNull && !server.IsReplica)
            {
                log?.WriteLine($"Resending tie-breaker to {Format.ToString(server.EndPoint)}...");
                msg = Message.Create(0, flags | CommandFlags.FireAndForget, RedisCommand.SET, tieBreakerKey, newPrimary);
                try
                {
                    await server.WriteDirectAsync(msg, ResultProcessor.DemandOK);
                }
                catch { }
            }

            // There's an inherent race here in zero-latency environments (e.g. when Redis is on localhost) when a broadcast is specified
            // The broadcast can get back from redis and trigger a reconfigure before we get a chance to get to ReconfigureAsync() below
            // This results in running an outdated reconfiguration and the .CompareExchange() (due to already running a reconfiguration)
            // failing...making our needed reconfiguration a no-op.
            // If we don't block *that* run, then *our* run (at low latency) gets blocked. Then we're waiting on the
            // ConfigurationOptions.ConfigCheckSeconds interval to identify the current (created by this method call) topology correctly.
            var blockingReconfig = Interlocked.CompareExchange(ref activeConfigCause, "Block: Pending Primary Reconfig", null) == null;

            // Try and broadcast the fact a change happened to all members
            // We want everyone possible to pick it up.
            // We broadcast before *and after* the change to remote members, so that they don't go without detecting a change happened.
            // This eliminates the race of pub/sub *then* re-slaving happening, since a method both precedes and follows.
            async Task BroadcastAsync(ServerEndPoint[] serverNodes)
            {
                if ((options & ReplicationChangeOptions.Broadcast) != 0 && ConfigurationChangedChannel != null
                    && CommandMap.IsAvailable(RedisCommand.PUBLISH))
                {
                    RedisValue channel = ConfigurationChangedChannel;
                    foreach (var node in serverNodes)
                    {
                        if (!node.IsConnected) continue;
                        log?.WriteLine($"Broadcasting via {Format.ToString(node.EndPoint)}...");
                        msg = Message.Create(-1, flags | CommandFlags.FireAndForget, RedisCommand.PUBLISH, channel, newPrimary);
                        await node.WriteDirectAsync(msg, ResultProcessor.Int64);
                    }
                }
            }

            // Send a message before it happens - because afterwards a new replica may be unresponsive
            await BroadcastAsync(nodes);

            if ((options & ReplicationChangeOptions.ReplicateToOtherEndpoints) != 0)
            {
                foreach (var node in nodes)
                {
                    if (node == server || node.ServerType != ServerType.Standalone) continue;

                    log?.WriteLine($"Replicating to {Format.ToString(node.EndPoint)}...");
                    msg = RedisServer.CreateReplicaOfMessage(node, server.EndPoint, flags);
                    await node.WriteDirectAsync(msg, ResultProcessor.DemandOK);
                }
            }

            // ...and send one after it happens - because the first broadcast may have landed on a secondary client
            // and it can reconfigure before any topology change actually happened. This is most likely to happen
            // in low-latency environments.
            await BroadcastAsync(nodes);

            // and reconfigure the muxer
            log?.WriteLine("Reconfiguring all endpoints...");
            // Yes, there is a tiny latency race possible between this code and the next call, but it's far more minute than before.
            // The effective gap between 0 and > 0 (likely off-box) latency is something that may never get hit here by anyone.
            if (blockingReconfig)
            {
                Interlocked.Exchange(ref activeConfigCause, null);
            }
            if (!await ReconfigureAsync(first: false, reconfigureAll: true, log, srv.EndPoint, cause: nameof(MakePrimaryAsync)))
            {
                log?.WriteLine("Verifying the configuration was incomplete; please verify");
            }
        }

        internal void CheckMessage(Message message)
        {
            if (!RawConfig.AllowAdmin && message.IsAdmin)
                throw ExceptionFactory.AdminModeNotEnabled(IncludeDetailInExceptions, message.Command, message, null);
            if (message.Command != RedisCommand.UNKNOWN) CommandMap.AssertAvailable(message.Command);

            // using >= here because we will be adding 1 for the command itself (which is an argument for the purposes of the multi-bulk protocol)
            if (message.ArgCount >= PhysicalConnection.REDIS_MAX_ARGS) throw ExceptionFactory.TooManyArgs(message.CommandAndKey, message.ArgCount);
        }
        private const string NoContent = "(no content)";
        private static void WriteNormalizingLineEndings(string source, StreamWriter writer)
        {
            if (source == null)
            {
                writer.WriteLine(NoContent);
            }
            else
            {
                using (var reader = new StringReader(source))
                {
                    string line;
                    while ((line = reader.ReadLine()) != null)
                        writer.WriteLine(line); // normalize line endings
                }
            }
        }

        /// <summary>
        /// Raised whenever a physical connection fails.
        /// </summary>
        public event EventHandler<ConnectionFailedEventArgs> ConnectionFailed;

        /// <summary>
        /// Raised whenever an internal error occurs (this is primarily for debugging).
        /// </summary>
        public event EventHandler<InternalErrorEventArgs> InternalError;

        /// <summary>
        /// Raised whenever a physical connection is established.
        /// </summary>
        public event EventHandler<ConnectionFailedEventArgs> ConnectionRestored;

        /// <summary>
        /// Raised when configuration changes are detected.
        /// </summary>
        public event EventHandler<EndPointEventArgs> ConfigurationChanged;

        /// <summary>
        /// Raised when nodes are explicitly requested to reconfigure via broadcast.
        /// This usually means primary/replica changes.
        /// </summary>
        public event EventHandler<EndPointEventArgs> ConfigurationChangedBroadcast;

        /// <summary>
        /// Raised when server indicates a maintenance event is going to happen.
        /// </summary>
        public event EventHandler<ServerMaintenanceEvent> ServerMaintenanceEvent;

        /// <summary>
        /// Gets the synchronous timeout associated with the connections.
        /// </summary>
        public int TimeoutMilliseconds { get; }

        /// <summary>
        /// Gets the asynchronous timeout associated with the connections.
        /// </summary>
        internal int AsyncTimeoutMilliseconds { get; }

        /// <summary>
        /// Gets all endpoints defined on the multiplexer.
        /// </summary>
        /// <param name="configuredOnly">Whether to get only the endpoints specified explicitly in the config.</param>
        public EndPoint[] GetEndPoints(bool configuredOnly = false)
        {
            if (configuredOnly) return RawConfig.EndPoints.ToArray();

            return _serverSnapshot.GetEndPoints();
        }

        internal void InvokeServerMaintenanceEvent(ServerMaintenanceEvent e)
            => ServerMaintenanceEvent?.Invoke(this, e);

        internal bool TryResend(int hashSlot, Message message, EndPoint endpoint, bool isMoved)
        {
            return ServerSelectionStrategy.TryResend(hashSlot, message, endpoint, isMoved);
        }

        /// <summary>
        /// Wait for a given asynchronous operation to complete (or timeout).
        /// </summary>
        /// <param name="task">The task to wait on.</param>
        public void Wait(Task task)
        {
            if (task == null) throw new ArgumentNullException(nameof(task));
            try
            {
                if (!task.Wait(TimeoutMilliseconds)) throw new TimeoutException();
            }
            catch (AggregateException aex) when (IsSingle(aex))
            {
                throw aex.InnerExceptions[0];
            }
        }

        /// <summary>
        /// Wait for a given asynchronous operation to complete (or timeout).
        /// </summary>
        /// <typeparam name="T">The type contains in the task to wait on.</typeparam>
        /// <param name="task">The task to wait on.</param>
        public T Wait<T>(Task<T> task)
        {
            if (task == null) throw new ArgumentNullException(nameof(task));
            try
            {
                if (!task.Wait(TimeoutMilliseconds)) throw new TimeoutException();
            }
            catch (AggregateException aex) when (IsSingle(aex))
            {
                throw aex.InnerExceptions[0];
            }
            return task.Result;
        }

        private static bool IsSingle(AggregateException aex)
        {
            try { return aex != null && aex.InnerExceptions.Count == 1; }
            catch { return false; }
        }

        /// <summary>
        /// Wait for the given asynchronous operations to complete (or timeout).
        /// </summary>
        /// <param name="tasks">The tasks to wait on.</param>
        public void WaitAll(params Task[] tasks)
        {
            if (tasks == null) throw new ArgumentNullException(nameof(tasks));
            if (tasks.Length == 0) return;
            if (!Task.WaitAll(tasks, TimeoutMilliseconds)) throw new TimeoutException();
        }

        private bool WaitAllIgnoreErrors(Task[] tasks) => WaitAllIgnoreErrors(tasks, TimeoutMilliseconds);

        private static bool WaitAllIgnoreErrors(Task[] tasks, int timeout)
        {
            if (tasks == null) throw new ArgumentNullException(nameof(tasks));
            if (tasks.Length == 0) return true;
            var watch = ValueStopwatch.StartNew();
            try
            {
                // If no error, great
                if (Task.WaitAll(tasks, timeout)) return true;
            }
            catch
            { }
            // If we get problems, need to give the non-failing ones time to be fair and reasonable
            for (int i = 0; i < tasks.Length; i++)
            {
                var task = tasks[i];
                if (!task.IsCanceled && !task.IsCompleted && !task.IsFaulted)
                {
                    var remaining = timeout - watch.ElapsedMilliseconds;
                    if (remaining <= 0) return false;
                    try
                    {
                        task.Wait(remaining);
                    }
                    catch
                    { }
                }
            }
            return false;
        }

        internal bool AuthSuspect { get; private set; }
        internal void SetAuthSuspect() => AuthSuspect = true;

        private static void LogWithThreadPoolStats(LogProxy log, string message, out int busyWorkerCount)
        {
            busyWorkerCount = 0;
            if (log != null)
            {
                var sb = new StringBuilder();
                sb.Append(message);
                busyWorkerCount = PerfCounterHelper.GetThreadPoolStats(out string iocp, out string worker, out string workItems);
                sb.Append(", IOCP: ").Append(iocp).Append(", WORKER: ").Append(worker);
                if (workItems != null)
                {
                    sb.Append(", POOL: ").Append(workItems);
                }
                log?.WriteLine(sb.ToString());
            }
        }

        private static bool AllComplete(Task[] tasks)
        {
            for (int i = 0; i < tasks.Length; i++)
            {
                var task = tasks[i];
                if (!task.IsCanceled && !task.IsCompleted && !task.IsFaulted)
                    return false;
            }
            return true;
        }

        private static async Task<bool> WaitAllIgnoreErrorsAsync(string name, Task[] tasks, int timeoutMilliseconds, LogProxy log, [CallerMemberName] string caller = null, [CallerLineNumber] int callerLineNumber = 0)
        {
            if (tasks == null) throw new ArgumentNullException(nameof(tasks));
            if (tasks.Length == 0)
            {
                log?.WriteLine("No tasks to await");
                return true;
            }

            if (AllComplete(tasks))
            {
                log?.WriteLine("All tasks are already complete");
                return true;
            }

            var watch = ValueStopwatch.StartNew();
            LogWithThreadPoolStats(log, $"Awaiting {tasks.Length} {name} task completion(s) for {timeoutMilliseconds}ms", out _);
            try
            {
                // if none error, great
                var remaining = timeoutMilliseconds - watch.ElapsedMilliseconds;
                if (remaining <= 0)
                {
                    LogWithThreadPoolStats(log, "Timeout before awaiting for tasks", out _);
                    return false;
                }

                var allTasks = Task.WhenAll(tasks).ObserveErrors();
                bool all = await allTasks.TimeoutAfter(timeoutMs: remaining).ObserveErrors().ForAwait();
                LogWithThreadPoolStats(log, all ? $"All {tasks.Length} {name} tasks completed cleanly" : $"Not all {name} tasks completed cleanly (from {caller}#{callerLineNumber}, timeout {timeoutMilliseconds}ms)", out _);
                return all;
            }
            catch
            { }

            // if we get problems, need to give the non-failing ones time to finish
            // to be fair and reasonable
            for (int i = 0; i < tasks.Length; i++)
            {
                var task = tasks[i];
                if (!task.IsCanceled && !task.IsCompleted && !task.IsFaulted)
                {
                    var remaining = timeoutMilliseconds - watch.ElapsedMilliseconds;
                    if (remaining <= 0)
                    {
                        LogWithThreadPoolStats(log, "Timeout awaiting tasks", out _);
                        return false;
                    }
                    try
                    {
                        await Task.WhenAny(task, Task.Delay(remaining)).ObserveErrors().ForAwait();
                    }
                    catch
                    { }
                }
            }
            LogWithThreadPoolStats(log, "Finished awaiting tasks", out _);
            return false;
        }

        /// <summary>
        /// Raised when a hash-slot has been relocated.
        /// </summary>
        public event EventHandler<HashSlotMovedEventArgs> HashSlotMoved;

        internal void OnHashSlotMoved(int hashSlot, EndPoint old, EndPoint @new)
        {
            var handler = HashSlotMoved;
            if (handler != null)
            {
                CompleteAsWorker(new HashSlotMovedEventArgs(handler, this, hashSlot, old, @new));
            }
        }

        /// <summary>
        /// Compute the hash-slot of a specified key.
        /// </summary>
        /// <param name="key">The key to get a hash slot ID for.</param>
        public int HashSlot(RedisKey key) => ServerSelectionStrategy.HashSlot(key);

        internal ServerEndPoint AnyServer(ServerType serverType, uint startOffset, RedisCommand command, CommandFlags flags, bool allowDisconnected)
        {
            var tmp = GetServerSnapshot();
            int len = tmp.Length;
            ServerEndPoint fallback = null;
            for (int i = 0; i < len; i++)
            {
                var server = tmp[(int)(((uint)i + startOffset) % len)];
                if (server != null && server.ServerType == serverType && server.IsSelectable(command, allowDisconnected))
                {
                    if (server.IsReplica)
                    {
                        switch (flags)
                        {
                            case CommandFlags.DemandReplica:
                            case CommandFlags.PreferReplica:
                                return server;
                            case CommandFlags.PreferMaster:
                                fallback = server;
                                break;
                        }
                    }
                    else
                    {
                        switch (flags)
                        {
                            case CommandFlags.DemandMaster:
                            case CommandFlags.PreferMaster:
                                return server;
                            case CommandFlags.PreferReplica:
                                fallback = server;
                                break;
                        }
                    }
                }
            }
            return fallback;
        }

        private volatile bool _isDisposed;
        internal bool IsDisposed => _isDisposed;

        /// <summary>
        /// Creates a new <see cref="ConnectionMultiplexer"/> instance.
        /// </summary>
        /// <param name="configuration">The string configuration to use for this multiplexer.</param>
        /// <param name="log">The <see cref="TextWriter"/> to log to.</param>
        public static Task<ConnectionMultiplexer> ConnectAsync(string configuration, TextWriter log = null) =>
            ConnectAsync(ConfigurationOptions.Parse(configuration), log);

        /// <summary>
        /// Creates a new <see cref="ConnectionMultiplexer"/> instance.
        /// </summary>
        /// <param name="configuration">The string configuration to use for this multiplexer.</param>
        /// <param name="configure">Action to further modify the parsed configuration options.</param>
        /// <param name="log">The <see cref="TextWriter"/> to log to.</param>
        public static Task<ConnectionMultiplexer> ConnectAsync(string configuration, Action<ConfigurationOptions> configure, TextWriter log = null) =>
            ConnectAsync(ConfigurationOptions.Parse(configuration).Apply(configure), log);

        /// <summary>
        /// Creates a new <see cref="ConnectionMultiplexer"/> instance.
        /// </summary>
        /// <param name="configuration">The configuration options to use for this multiplexer.</param>
        /// <param name="log">The <see cref="TextWriter"/> to log to.</param>
        /// <remarks>Note: For Sentinel, do <b>not</b> specify a <see cref="ConfigurationOptions.CommandMap"/> - this is handled automatically.</remarks>
        public static Task<ConnectionMultiplexer> ConnectAsync(ConfigurationOptions configuration, TextWriter log = null)
        {
            SocketConnection.AssertDependencies();

            if (IsSentinel(configuration))
                return SentinelPrimaryConnectAsync(configuration, log);

            return ConnectImplAsync(PrepareConfig(configuration), log);
        }

        private static async Task<ConnectionMultiplexer> ConnectImplAsync(ConfigurationOptions configuration, TextWriter log = null)
        {
            IDisposable killMe = null;
            EventHandler<ConnectionFailedEventArgs> connectHandler = null;
            ConnectionMultiplexer muxer = null;
            using (var logProxy = LogProxy.TryCreate(log))
            {
                try
                {
                    var sw = ValueStopwatch.StartNew();
                    logProxy?.WriteLine($"Connecting (async) on {RuntimeInformation.FrameworkDescription} (StackExchange.Redis: v{Utils.GetLibVersion()})");

                    muxer = CreateMultiplexer(configuration, logProxy, out connectHandler);
                    killMe = muxer;
                    Interlocked.Increment(ref muxer._connectAttemptCount);
                    bool configured = await muxer.ReconfigureAsync(first: true, reconfigureAll: false, logProxy, null, "connect").ObserveErrors().ForAwait();
                    if (!configured)
                    {
                        throw ExceptionFactory.UnableToConnect(muxer, muxer.failureMessage);
                    }
                    killMe = null;
                    Interlocked.Increment(ref muxer._connectCompletedCount);

                    if (muxer.ServerSelectionStrategy.ServerType == ServerType.Sentinel)
                    {
                        // Initialize the Sentinel handlers
                        muxer.InitializeSentinel(logProxy);
                    }

                    await configuration.AfterConnectAsync(muxer, logProxy != null ? logProxy.WriteLine : LogProxy.NullWriter).ForAwait();

                    logProxy?.WriteLine($"Total connect time: {sw.ElapsedMilliseconds:n0} ms");

                    return muxer;
                }
                finally
                {
                    if (connectHandler != null) muxer.ConnectionFailed -= connectHandler;
                    if (killMe != null) try { killMe.Dispose(); } catch { }
                }
            }
        }

        private static bool IsSentinel(ConfigurationOptions configuration)
        {
            return !string.IsNullOrEmpty(configuration?.ServiceName);
        }

        internal static ConfigurationOptions PrepareConfig(object configuration, bool sentinel = false)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            ConfigurationOptions config;
            if (configuration is string s)
            {
                config = ConfigurationOptions.Parse(s);
            }
            else if (configuration is ConfigurationOptions configurationOptions)
            {
                config = (configurationOptions).Clone();
            }
            else
            {
                throw new ArgumentException("Invalid configuration object", nameof(configuration));
            }
            if (config.EndPoints.Count == 0) throw new ArgumentException("No endpoints specified", nameof(configuration));

            if (sentinel)
            {
                config.SetSentinelDefaults();

                return config;
            }

            config.SetDefaultPorts();

            return config;
        }
        private static ConnectionMultiplexer CreateMultiplexer(ConfigurationOptions configuration, LogProxy log, out EventHandler<ConnectionFailedEventArgs> connectHandler)
        {
            var muxer = new ConnectionMultiplexer(configuration);
            connectHandler = null;
            if (log != null)
            {
                // Create a detachable event-handler to log detailed errors if something happens during connect/handshake
                connectHandler = (_, a) =>
                {
                    try
                    {
                        lock (log.SyncLock) // Keep the outer and any inner errors contiguous
                        {
                            var ex = a.Exception;
                            log?.WriteLine($"Connection failed: {Format.ToString(a.EndPoint)} ({a.ConnectionType}, {a.FailureType}): {ex?.Message ?? "(unknown)"}");
                            while ((ex = ex.InnerException) != null)
                            {
                                log?.WriteLine($"> {ex.Message}");
                            }
                        }
                    }
                    catch { }
                };
                muxer.ConnectionFailed += connectHandler;
            }
            return muxer;
        }

        /// <summary>
        /// Creates a new <see cref="ConnectionMultiplexer"/> instance.
        /// </summary>
        /// <param name="configuration">The string configuration to use for this multiplexer.</param>
        /// <param name="log">The <see cref="TextWriter"/> to log to.</param>
        public static ConnectionMultiplexer Connect(string configuration, TextWriter log = null) =>
            Connect(ConfigurationOptions.Parse(configuration), log);

        /// <summary>
        /// Creates a new <see cref="ConnectionMultiplexer"/> instance.
        /// </summary>
        /// <param name="configuration">The string configuration to use for this multiplexer.</param>
        /// <param name="configure">Action to further modify the parsed configuration options.</param>
        /// <param name="log">The <see cref="TextWriter"/> to log to.</param>
        public static ConnectionMultiplexer Connect(string configuration, Action<ConfigurationOptions> configure, TextWriter log = null) =>
            Connect(ConfigurationOptions.Parse(configuration).Apply(configure), log);

        /// <summary>
        /// Creates a new <see cref="ConnectionMultiplexer"/> instance.
        /// </summary>
        /// <param name="configuration">The configuration options to use for this multiplexer.</param>
        /// <param name="log">The <see cref="TextWriter"/> to log to.</param>
        /// <remarks>Note: For Sentinel, do <b>not</b> specify a <see cref="ConfigurationOptions.CommandMap"/> - this is handled automatically.</remarks>
        public static ConnectionMultiplexer Connect(ConfigurationOptions configuration, TextWriter log = null)
        {
            SocketConnection.AssertDependencies();

            if (IsSentinel(configuration))
            {
                return SentinelPrimaryConnect(configuration, log);
            }

            return ConnectImpl(PrepareConfig(configuration), log);
        }

        /// <summary>
        /// Create a new <see cref="ConnectionMultiplexer"/> instance that connects to a Sentinel server.
        /// </summary>
        /// <param name="configuration">The string configuration to use for this multiplexer.</param>
        /// <param name="log">The <see cref="TextWriter"/> to log to.</param>
        public static ConnectionMultiplexer SentinelConnect(string configuration, TextWriter log = null)
        {
            SocketConnection.AssertDependencies();
            return ConnectImpl(PrepareConfig(configuration, sentinel: true), log);
        }

        /// <summary>
        /// Create a new <see cref="ConnectionMultiplexer"/> instance that connects to a Sentinel server.
        /// </summary>
        /// <param name="configuration">The string configuration to use for this multiplexer.</param>
        /// <param name="log">The <see cref="TextWriter"/> to log to.</param>
        public static Task<ConnectionMultiplexer> SentinelConnectAsync(string configuration, TextWriter log = null)
        {
            SocketConnection.AssertDependencies();
            return ConnectImplAsync(PrepareConfig(configuration, sentinel: true), log);
        }

        /// <summary>
        /// Create a new <see cref="ConnectionMultiplexer"/> instance that connects to a Sentinel server.
        /// </summary>
        /// <param name="configuration">The configuration options to use for this multiplexer.</param>
        /// <param name="log">The <see cref="TextWriter"/> to log to.</param>
        public static ConnectionMultiplexer SentinelConnect(ConfigurationOptions configuration, TextWriter log = null)
        {
            SocketConnection.AssertDependencies();
            return ConnectImpl(PrepareConfig(configuration, sentinel: true), log);
        }

        /// <summary>
        /// Create a new <see cref="ConnectionMultiplexer"/> instance that connects to a Sentinel server.
        /// </summary>
        /// <param name="configuration">The configuration options to use for this multiplexer.</param>
        /// <param name="log">The <see cref="TextWriter"/> to log to.</param>
        public static Task<ConnectionMultiplexer> SentinelConnectAsync(ConfigurationOptions configuration, TextWriter log = null)
        {
            SocketConnection.AssertDependencies();
            return ConnectImplAsync(PrepareConfig(configuration, sentinel: true), log);
        }

        /// <summary>
        /// Create a new <see cref="ConnectionMultiplexer"/> instance that connects to a sentinel server, discovers the current primary server
        /// for the specified <see cref="ConfigurationOptions.ServiceName"/> in the config and returns a managed connection to the current primary server.
        /// </summary>
        /// <param name="configuration">The string configuration to use for this multiplexer.</param>
        /// <param name="log">The <see cref="TextWriter"/> to log to.</param>
        private static ConnectionMultiplexer SentinelPrimaryConnect(string configuration, TextWriter log = null)
        {
            return SentinelPrimaryConnect(PrepareConfig(configuration, sentinel: true), log);
        }

        /// <summary>
        /// Create a new <see cref="ConnectionMultiplexer"/> instance that connects to a sentinel server, discovers the current primary server
        /// for the specified <see cref="ConfigurationOptions.ServiceName"/> in the config and returns a managed connection to the current primary server.
        /// </summary>
        /// <param name="configuration">The configuration options to use for this multiplexer.</param>
        /// <param name="log">The <see cref="TextWriter"/> to log to.</param>
        private static ConnectionMultiplexer SentinelPrimaryConnect(ConfigurationOptions configuration, TextWriter log = null)
        {
            var sentinelConnection = SentinelConnect(configuration, log);

            var muxer = sentinelConnection.GetSentinelMasterConnection(configuration, log);
            // Set reference to sentinel connection so that we can dispose it
            muxer.sentinelConnection = sentinelConnection;

            return muxer;
        }

        /// <summary>
        /// Create a new <see cref="ConnectionMultiplexer"/> instance that connects to a sentinel server, discovers the current primary server
        /// for the specified <see cref="ConfigurationOptions.ServiceName"/> in the config and returns a managed connection to the current primary server.
        /// </summary>
        /// <param name="configuration">The string configuration to use for this multiplexer.</param>
        /// <param name="log">The <see cref="TextWriter"/> to log to.</param>
        private static Task<ConnectionMultiplexer> SentinelPrimaryConnectAsync(string configuration, TextWriter log = null)
        {
            return SentinelPrimaryConnectAsync(PrepareConfig(configuration, sentinel: true), log);
        }

        /// <summary>
        /// Create a new <see cref="ConnectionMultiplexer"/> instance that connects to a sentinel server, discovers the current primary server
        /// for the specified <see cref="ConfigurationOptions.ServiceName"/> in the config and returns a managed connection to the current primary server.
        /// </summary>
        /// <param name="configuration">The configuration options to use for this multiplexer.</param>
        /// <param name="log">The <see cref="TextWriter"/> to log to.</param>
        private static async Task<ConnectionMultiplexer> SentinelPrimaryConnectAsync(ConfigurationOptions configuration, TextWriter log = null)
        {
            var sentinelConnection = await SentinelConnectAsync(configuration, log).ForAwait();

            var muxer = sentinelConnection.GetSentinelMasterConnection(configuration, log);
            // Set reference to sentinel connection so that we can dispose it
            muxer.sentinelConnection = sentinelConnection;

            return muxer;
        }

        private static ConnectionMultiplexer ConnectImpl(ConfigurationOptions configuration, TextWriter log)
        {
            IDisposable killMe = null;
            EventHandler<ConnectionFailedEventArgs> connectHandler = null;
            ConnectionMultiplexer muxer = null;
            using (var logProxy = LogProxy.TryCreate(log))
            {
                try
                {
                    var sw = ValueStopwatch.StartNew();
                    logProxy?.WriteLine($"Connecting (sync) on {RuntimeInformation.FrameworkDescription} (StackExchange.Redis: v{Utils.GetLibVersion()})");

                    muxer = CreateMultiplexer(configuration, logProxy, out connectHandler);
                    killMe = muxer;
                    Interlocked.Increment(ref muxer._connectAttemptCount);
                    // note that task has timeouts internally, so it might take *just over* the regular timeout
                    var task = muxer.ReconfigureAsync(first: true, reconfigureAll: false, logProxy, null, "connect");

                    if (!task.Wait(muxer.SyncConnectTimeout(true)))
                    {
                        task.ObserveErrors();
                        if (muxer.RawConfig.AbortOnConnectFail)
                        {
                            throw ExceptionFactory.UnableToConnect(muxer, "ConnectTimeout");
                        }
                        else
                        {
                            muxer.LastException = ExceptionFactory.UnableToConnect(muxer, "ConnectTimeout");
                        }
                    }

                    if (!task.Result) throw ExceptionFactory.UnableToConnect(muxer, muxer.failureMessage);
                    killMe = null;
                    Interlocked.Increment(ref muxer._connectCompletedCount);

                    if (muxer.ServerSelectionStrategy.ServerType == ServerType.Sentinel)
                    {
                        // Initialize the Sentinel handlers
                        muxer.InitializeSentinel(logProxy);
                    }

                    configuration.AfterConnectAsync(muxer, logProxy != null ? logProxy.WriteLine : LogProxy.NullWriter).Wait(muxer.SyncConnectTimeout(true));

                    logProxy?.WriteLine($"Total connect time: {sw.ElapsedMilliseconds:n0} ms");

                    return muxer;
                }
                finally
                {
                    if (connectHandler != null && muxer != null) muxer.ConnectionFailed -= connectHandler;
                    if (killMe != null) try { killMe.Dispose(); } catch { }
                }
            }
        }

        private string failureMessage;
        private readonly Hashtable servers = new Hashtable();
        private volatile ServerSnapshot _serverSnapshot = ServerSnapshot.Empty;

        ReadOnlySpan<ServerEndPoint> IInternalConnectionMultiplexer.GetServerSnapshot() => GetServerSnapshot();
        internal ReadOnlySpan<ServerEndPoint> GetServerSnapshot() => _serverSnapshot.Span;
        private sealed class ServerSnapshot
        {
            public static ServerSnapshot Empty { get; } = new ServerSnapshot(Array.Empty<ServerEndPoint>(), 0);
            private ServerSnapshot(ServerEndPoint[] arr, int count)
            {
                _arr = arr;
                _count = count;
            }
            private readonly ServerEndPoint[] _arr;
            private readonly int _count;
            public ReadOnlySpan<ServerEndPoint> Span => new ReadOnlySpan<ServerEndPoint>(_arr, 0, _count);

            internal ServerSnapshot Add(ServerEndPoint value)
            {
                if (value == null) return this;

                ServerEndPoint[] arr;
                if (_arr.Length > _count)
                {
                    arr = _arr;
                }
                else
                {
                    // no more room; need a new array
                    int newLen = _arr.Length << 1;
                    if (newLen == 0) newLen = 4;
                    arr = new ServerEndPoint[newLen];
                    _arr.CopyTo(arr, 0);
                }
                arr[_count] = value;
                return new ServerSnapshot(arr, _count + 1);
            }

            internal EndPoint[] GetEndPoints()
            {
                if (_count == 0) return Array.Empty<EndPoint>();

                var arr = new EndPoint[_count];
                for (int i = 0; i < _count; i++)
                {
                    arr[i] = _arr[i].EndPoint;
                }
                return arr;
            }
        }

        internal ServerEndPoint GetServerEndPoint(EndPoint endpoint, LogProxy log = null, bool activate = true)
        {
            if (endpoint == null) return null;
            var server = (ServerEndPoint)servers[endpoint];
            if (server == null)
            {
                bool isNew = false;
                lock (servers)
                {
                    server = (ServerEndPoint)servers[endpoint];
                    if (server == null)
                    {
                        if (_isDisposed) throw new ObjectDisposedException(ToString());

                        server = new ServerEndPoint(this, endpoint);
                        servers.Add(endpoint, server);
                        isNew = true;
                        _serverSnapshot = _serverSnapshot.Add(server);
                    }
                }
                // spin up the connection if this is new
                if (isNew && activate) server.Activate(ConnectionType.Interactive, log);
            }
            return server;
        }

        internal readonly CommandMap CommandMap;

        private ConnectionMultiplexer(ConfigurationOptions configuration)
        {
            IncludeDetailInExceptions = true;
            IncludePerformanceCountersInExceptions = false;

            RawConfig = configuration ?? throw new ArgumentNullException(nameof(configuration));

            var map = CommandMap = configuration.CommandMap;
            if (!string.IsNullOrWhiteSpace(configuration.Password)) map.AssertAvailable(RedisCommand.AUTH);

            if (!map.IsAvailable(RedisCommand.ECHO) && !map.IsAvailable(RedisCommand.PING) && !map.IsAvailable(RedisCommand.TIME))
            { // I mean really, give me a CHANCE! I need *something* to check the server is available to me...
                // see also: SendTracer (matching logic)
                map.AssertAvailable(RedisCommand.EXISTS);
            }

            TimeoutMilliseconds = configuration.SyncTimeout;
            AsyncTimeoutMilliseconds = configuration.AsyncTimeout;

            OnCreateReaderWriter(configuration);
            ServerSelectionStrategy = new ServerSelectionStrategy(this);

            var configChannel = configuration.ConfigurationChannel;
            if (!string.IsNullOrWhiteSpace(configChannel))
            {
                ConfigurationChangedChannel = Encoding.UTF8.GetBytes(configChannel);
            }
            lastHeartbeatTicks = Environment.TickCount;
        }

        partial void OnCreateReaderWriter(ConfigurationOptions configuration);

        internal const int MillisecondsPerHeartbeat = 1000;
        private sealed class TimerToken
        {
            public TimerToken(ConnectionMultiplexer muxer)
            {
                _ref = new WeakReference(muxer);
            }
            private Timer _timer;
            public void SetTimer(Timer timer) => _timer = timer;
            private readonly WeakReference _ref;

            private static readonly TimerCallback Heartbeat = state =>
            {
                var token = (TimerToken)state;
                var muxer = (ConnectionMultiplexer)(token._ref?.Target);
                if (muxer != null)
                {
                    muxer.OnHeartbeat();
                }
                else
                {
                    // the muxer got disposed from out of us; kill the timer
                    var tmp = token._timer;
                    token._timer = null;
                    if (tmp != null) try { tmp.Dispose(); } catch { }
                }
            };

            internal static IDisposable Create(ConnectionMultiplexer connection)
            {
                var token = new TimerToken(connection);
                var timer = new Timer(Heartbeat, token, MillisecondsPerHeartbeat, MillisecondsPerHeartbeat);
                token.SetTimer(timer);
                return timer;
            }
        }

        private int _activeHeartbeatErrors;
        private void OnHeartbeat()
        {
            try
            {
                int now = Environment.TickCount;
                Interlocked.Exchange(ref lastHeartbeatTicks, now);
                Interlocked.Exchange(ref lastGlobalHeartbeatTicks, now);
                Trace("heartbeat");

                var tmp = GetServerSnapshot();
                for (int i = 0; i < tmp.Length; i++)
                    tmp[i].OnHeartbeat();
            }
            catch (Exception ex)
            {
                if (Interlocked.CompareExchange(ref _activeHeartbeatErrors, 1, 0) == 0)
                {
                    try
                    {
                        OnInternalError(ex);
                    }
                    finally
                    {
                        Interlocked.Exchange(ref _activeHeartbeatErrors, 0);
                    }
                }
            }
        }

        private int lastHeartbeatTicks;
        private static int lastGlobalHeartbeatTicks = Environment.TickCount;
        internal long LastHeartbeatSecondsAgo
        {
            get
            {
                if (pulse == null) return -1;
                return unchecked(Environment.TickCount - Thread.VolatileRead(ref lastHeartbeatTicks)) / 1000;
            }
        }

        internal Exception LastException { get; set; }

        internal static long LastGlobalHeartbeatSecondsAgo => unchecked(Environment.TickCount - Thread.VolatileRead(ref lastGlobalHeartbeatTicks)) / 1000;

        /// <summary>
        /// Obtain a pub/sub subscriber connection to the specified server.
        /// </summary>
        /// <param name="asyncState">The async state object to pass to the created <see cref="RedisSubscriber"/>.</param>
        public ISubscriber GetSubscriber(object asyncState = null)
        {
            if (!RawConfig.Proxy.SupportsPubSub())
            {
                throw new NotSupportedException($"The pub/sub API is not available via {RawConfig.Proxy}");
            }
            return new RedisSubscriber(this, asyncState);
        }

        /// <summary>
        /// Applies common DB number defaults and rules.
        /// </summary>
        internal int ApplyDefaultDatabase(int db)
        {
            if (db == -1)
            {
                db = RawConfig.DefaultDatabase.GetValueOrDefault();
            }
            else if (db < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(db));
            }

            if (db != 0 && !RawConfig.Proxy.SupportsDatabases())
            {
                throw new NotSupportedException($"{RawConfig.Proxy} only supports database 0");
            }

            return db;
        }

        /// <summary>
        /// Obtain an interactive connection to a database inside redis.
        /// </summary>
        /// <param name="db">The ID to get a database for.</param>
        /// <param name="asyncState">The async state to pass into the resulting <see cref="RedisDatabase"/>.</param>
        public IDatabase GetDatabase(int db = -1, object asyncState = null)
        {
            db = ApplyDefaultDatabase(db);

            // if there's no async-state, and the DB is suitable, we can hand out a re-used instance
            return (asyncState == null && db <= MaxCachedDatabaseInstance)
                ? GetCachedDatabaseInstance(db) : new RedisDatabase(this, db, asyncState);
        }

        // DB zero is stored separately, since 0-only is a massively common use-case
        private const int MaxCachedDatabaseInstance = 16; // 17 items - [0,16]
        // Side note: "databases 16" is the default in redis.conf; happy to store one extra to get nice alignment etc
        private IDatabase dbCacheZero;
        private IDatabase[] dbCacheLow;
        private IDatabase GetCachedDatabaseInstance(int db) // note that we already trust db here; only caller checks range
        {
            // Note: we don't need to worry about *always* returning the same instance.
            // If two threads ask for db 3 at the same time, it is OK for them to get
            // different instances, one of which (arbitrarily) ends up cached for later use.
            if (db == 0)
            {
                return dbCacheZero ??= new RedisDatabase(this, 0, null);
            }
            var arr = dbCacheLow ??= new IDatabase[MaxCachedDatabaseInstance];
            return arr[db - 1] ??= new RedisDatabase(this, db, null);
        }

        /// <summary>
        /// Obtain a configuration API for an individual server.
        /// </summary>
        /// <param name="host">The host to get a server for.</param>
        /// <param name="port">The port for <paramref name="host"/> to get a server for.</param>
        /// <param name="asyncState">The async state to pass into the resulting <see cref="RedisServer"/>.</param>
        public IServer GetServer(string host, int port, object asyncState = null) => GetServer(Format.ParseEndPoint(host, port), asyncState);

        /// <summary>
        /// Obtain a configuration API for an individual server.
        /// </summary>
        /// <param name="hostAndPort">The "host:port" string to get a server for.</param>
        /// <param name="asyncState">The async state to pass into the resulting <see cref="RedisServer"/>.</param>
        public IServer GetServer(string hostAndPort, object asyncState = null) => GetServer(Format.TryParseEndPoint(hostAndPort), asyncState);

        /// <summary>
        /// Obtain a configuration API for an individual server.
        /// </summary>
        /// <param name="host">The host to get a server for.</param>
        /// <param name="port">The port for <paramref name="host"/> to get a server for.</param>
        public IServer GetServer(IPAddress host, int port) => GetServer(new IPEndPoint(host, port));

        /// <summary>
        /// Obtain a configuration API for an individual server.
        /// </summary>
        /// <param name="endpoint">The endpoint to get a server for.</param>
        /// <param name="asyncState">The async state to pass into the resulting <see cref="RedisServer"/>.</param>
        public IServer GetServer(EndPoint endpoint, object asyncState = null)
        {
            if (endpoint == null) throw new ArgumentNullException(nameof(endpoint));
            if (!RawConfig.Proxy.SupportsServerApi())
            {
                throw new NotSupportedException($"The server API is not available via {RawConfig.Proxy}");
            }
            var server = (ServerEndPoint)servers[endpoint];
            if (server == null) throw new ArgumentException("The specified endpoint is not defined", nameof(endpoint));
            return new RedisServer(this, server, asyncState);
        }

        /// <summary>
        /// The number of operations that have been performed on all connections.
        /// </summary>
        public long OperationCount
        {
            get
            {
                long total = 0;
                var snapshot = GetServerSnapshot();
                for (int i = 0; i < snapshot.Length; i++) total += snapshot[i].OperationCount;
                return total;
            }
        }

        private string activeConfigCause;

        internal bool ReconfigureIfNeeded(EndPoint blame, bool fromBroadcast, string cause, bool publishReconfigure = false, CommandFlags flags = CommandFlags.None)
        {
            if (fromBroadcast)
            {
                OnConfigurationChangedBroadcast(blame);
            }
            string activeCause = Volatile.Read(ref activeConfigCause);
            if (activeCause == null)
            {
                bool reconfigureAll = fromBroadcast || publishReconfigure;
                Trace("Configuration change detected; checking nodes", "Configuration");
                ReconfigureAsync(first: false, reconfigureAll, null, blame, cause, publishReconfigure, flags).ObserveErrors();
                return true;
            }
            else
            {
                Trace("Configuration change skipped; already in progress via " + activeCause, "Configuration");
                return false;
            }
        }

        /// <summary>
        /// Reconfigure the current connections based on the existing configuration.
        /// </summary>
        /// <param name="log">The <see cref="TextWriter"/> to log to.</param>
        public async Task<bool> ConfigureAsync(TextWriter log = null)
        {
            using (var logProxy = LogProxy.TryCreate(log))
            {
                return await ReconfigureAsync(first: false, reconfigureAll: true, logProxy, null, "configure").ObserveErrors();
            }
        }

        /// <summary>
        /// Reconfigure the current connections based on the existing configuration.
        /// </summary>
        /// <param name="log">The <see cref="TextWriter"/> to log to.</param>
        public bool Configure(TextWriter log = null)
        {
            // Note we expect ReconfigureAsync to internally allow [n] duration,
            // so to avoid near misses, here we wait 2*[n].
            using (var logProxy = LogProxy.TryCreate(log))
            {
                var task = ReconfigureAsync(first: false, reconfigureAll: true, logProxy, null, "configure");
                if (!task.Wait(SyncConnectTimeout(false)))
                {
                    task.ObserveErrors();
                    if (RawConfig.AbortOnConnectFail)
                    {
                        throw new TimeoutException();
                    }
                    else
                    {
                        LastException = new TimeoutException("ConnectTimeout");
                    }
                    return false;
                }
                return task.Result;
            }
        }

        internal int SyncConnectTimeout(bool forConnect)
        {
            int retryCount = forConnect ? RawConfig.ConnectRetry : 1;
            if (retryCount <= 0) retryCount = 1;

            int timeout = RawConfig.ConnectTimeout;
            if (timeout >= int.MaxValue / retryCount) return int.MaxValue;

            timeout *= retryCount;
            if (timeout >= int.MaxValue - 500) return int.MaxValue;
            return timeout + Math.Min(500, timeout);
        }

        /// <summary>
        /// Provides a text overview of the status of all connections.
        /// </summary>
        public string GetStatus()
        {
            using (var sw = new StringWriter())
            {
                GetStatus(sw);
                return sw.ToString();
            }
        }

        /// <summary>
        /// Provides a text overview of the status of all connections.
        /// </summary>
        /// <param name="log">The <see cref="TextWriter"/> to log to.</param>
        public void GetStatus(TextWriter log)
        {
            using (var proxy = LogProxy.TryCreate(log))
            {
                GetStatus(proxy);
            }
        }

        internal void GetStatus(LogProxy log)
        {
            if (log == null) return;

            var tmp = GetServerSnapshot();
            log?.WriteLine("Endpoint Summary:");
            foreach (var server in tmp)
            {
                log?.WriteLine(prefix: "  ", message: server.Summary());
                log?.WriteLine(prefix: "  ", message: server.GetCounters().ToString());
                log?.WriteLine(prefix: "  ", message: server.GetProfile());
            }
            log?.WriteLine($"Sync timeouts: {Interlocked.Read(ref syncTimeouts)}; async timeouts: {Interlocked.Read(ref asyncTimeouts)}; fire and forget: {Interlocked.Read(ref fireAndForgets)}; last heartbeat: {LastHeartbeatSecondsAgo}s ago");
        }

        private void ActivateAllServers(LogProxy log)
        {
            foreach (var server in GetServerSnapshot())
            {
                server.Activate(ConnectionType.Interactive, log);
                if (server.SupportsSubscriptions)
                {
                    // Intentionally not logging the sub connection
                    server.Activate(ConnectionType.Subscription, null);
                }
            }
        }

        /// <summary>
        /// Triggers a reconfigure of this multiplexer.
        /// This re-assessment of all server endpoints to get the current topology and adjust, the same as if we had first connected.
        /// TODO: Naming?
        /// </summary>
        public Task<bool> ReconfigureAsync(string reason) =>
            ReconfigureAsync(first: false, reconfigureAll: false, log: null, blame: null, cause: reason);

        internal async Task<bool> ReconfigureAsync(bool first, bool reconfigureAll, LogProxy log, EndPoint blame, string cause, bool publishReconfigure = false, CommandFlags publishReconfigureFlags = CommandFlags.None)
        {
            if (_isDisposed) throw new ObjectDisposedException(ToString());
            bool showStats = log is not null;

            bool ranThisCall = false;
            try
            {
                // Note that we *always* exchange the reason (first one counts) to prevent duplicate runs
                ranThisCall = Interlocked.CompareExchange(ref activeConfigCause, cause, null) == null;

                if (!ranThisCall)
                {
                    log?.WriteLine($"Reconfiguration was already in progress due to: {activeConfigCause}, attempted to run for: {cause}");
                    return false;
                }
                Trace("Starting reconfiguration...");
                Trace(blame != null, "Blaming: " + Format.ToString(blame));

                log?.WriteLine(RawConfig.ToString(includePassword: false));
                log?.WriteLine();

                if (first)
                {
                    if (RawConfig.ResolveDns && RawConfig.HasDnsEndPoints())
                    {
                        var dns = RawConfig.ResolveEndPointsAsync(this, log).ObserveErrors();
                        if (!await dns.TimeoutAfter(TimeoutMilliseconds).ForAwait())
                        {
                            throw new TimeoutException("Timeout resolving endpoints");
                        }
                    }
                    foreach (var endpoint in RawConfig.EndPoints)
                    {
                        GetServerEndPoint(endpoint, log, false);
                    }
                    ActivateAllServers(log);
                }
                int attemptsLeft = first ? RawConfig.ConnectRetry : 1;

                bool healthy = false;
                do
                {
                    if (first)
                    {
                        attemptsLeft--;
                    }
                    int standaloneCount = 0, clusterCount = 0, sentinelCount = 0;
                    var endpoints = RawConfig.EndPoints;
                    bool useTieBreakers = !string.IsNullOrWhiteSpace(RawConfig.TieBreaker);
                    log?.WriteLine($"{endpoints.Count} unique nodes specified ({(useTieBreakers ? "with" : "without")} tiebreaker)");

                    if (endpoints.Count == 0)
                    {
                        throw new InvalidOperationException("No nodes to consider");
                    }
                    List<ServerEndPoint> primaries = new List<ServerEndPoint>(endpoints.Count);

                    ServerEndPoint[] servers = null;
                    bool encounteredConnectedClusterServer = false;
                    ValueStopwatch? watch = null;

                    int iterCount = first ? 2 : 1;
                    // This is fix for https://github.com/StackExchange/StackExchange.Redis/issues/300
                    // auto discoverability of cluster nodes is made synchronous.
                    // We try to connect to endpoints specified inside the user provided configuration
                    // and when we encounter an endpoint to which we are able to successfully connect,
                    // we get the list of cluster nodes from that endpoint and try to proactively connect
                    // to listed nodes instead of relying on auto configure.
                    for (int iter = 0; iter < iterCount; ++iter)
                    {
                        if (endpoints == null) break;

                        var available = new Task<string>[endpoints.Count];
                        servers = new ServerEndPoint[available.Length];

                        RedisKey tieBreakerKey = useTieBreakers ? (RedisKey)RawConfig.TieBreaker : default(RedisKey);

                        for (int i = 0; i < available.Length; i++)
                        {
                            Trace("Testing: " + Format.ToString(endpoints[i]));

                            var server = GetServerEndPoint(endpoints[i]);
                            //server.ReportNextFailure();
                            servers[i] = server;

                            // This awaits either the endpoint's initial connection, or a tracer if we're already connected
                            // (which is the reconfigure case)
                            available[i] = server.OnConnectedAsync(log, sendTracerIfConnected: true, autoConfigureIfConnected: reconfigureAll);
                        }

                        watch ??= ValueStopwatch.StartNew();
                        var remaining = RawConfig.ConnectTimeout - watch.Value.ElapsedMilliseconds;
                        log?.WriteLine($"Allowing {available.Length} endpoint(s) {TimeSpan.FromMilliseconds(remaining)} to respond...");
                        Trace("Allowing endpoints " + TimeSpan.FromMilliseconds(remaining) + " to respond...");
                        var allConnected = await WaitAllIgnoreErrorsAsync("available", available, remaining, log).ForAwait();

                        if (!allConnected)
                        {
                            // If we failed, log the details so we can debug why per connection
                            for (var i = 0; i < servers.Length; i++)
                            {
                                var server = servers[i];
                                var task = available[i];
                                var bs = server.GetBridgeStatus(ConnectionType.Interactive);

                                log?.WriteLine($"  Server[{i}] ({Format.ToString(server)}) Status: {task.Status} (inst: {bs.MessagesSinceLastHeartbeat}, qs: {bs.Connection.MessagesSentAwaitingResponse}, in: {bs.Connection.BytesAvailableOnSocket}, qu: {bs.MessagesSinceLastHeartbeat}, aw: {bs.IsWriterActive}, in-pipe: {bs.Connection.BytesInReadPipe}, out-pipe: {bs.Connection.BytesInWritePipe}, bw: {bs.BacklogStatus}, rs: {bs.Connection.ReadStatus}. ws: {bs.Connection.WriteStatus})");
                            }
                        }

                        log?.WriteLine("Endpoint summary:");
                        // Log current state after await
                        foreach (var server in servers)
                        {
                            log?.WriteLine($"  {Format.ToString(server.EndPoint)}: Endpoint is {server.ConnectionState}");
                        }

                        EndPointCollection updatedClusterEndpointCollection = null;
                        for (int i = 0; i < available.Length; i++)
                        {
                            var task = available[i];
                            var server = servers[i];
                            Trace(Format.ToString(endpoints[i]) + ": " + task.Status);
                            if (task.IsFaulted)
                            {
                                server.SetUnselectable(UnselectableFlags.DidNotRespond);
                                var aex = task.Exception;
                                foreach (var ex in aex.InnerExceptions)
                                {
                                    log?.WriteLine($"  {Format.ToString(server)}: Faulted: {ex.Message}");
                                    failureMessage = ex.Message;
                                }
                            }
                            else if (task.IsCanceled)
                            {
                                server.SetUnselectable(UnselectableFlags.DidNotRespond);
                                log?.WriteLine($"  {Format.ToString(server)}: Connect task canceled");
                            }
                            else if (task.IsCompleted)
                            {
                                if (task.Result != "Disconnected")
                                {
                                    server.ClearUnselectable(UnselectableFlags.DidNotRespond);
                                    log?.WriteLine($"  {Format.ToString(server)}: Returned with success as {server.ServerType} {(server.IsReplica ? "replica" : "primary")} (Source: {task.Result})");

                                    // Count the server types
                                    switch (server.ServerType)
                                    {
                                        case ServerType.Twemproxy:
                                        case ServerType.Envoyproxy:
                                        case ServerType.Standalone:
                                            standaloneCount++;
                                            break;
                                        case ServerType.Sentinel:
                                            sentinelCount++;
                                            break;
                                        case ServerType.Cluster:
                                            clusterCount++;
                                            break;
                                    }

                                    if (clusterCount > 0 && !encounteredConnectedClusterServer && CommandMap.IsAvailable(RedisCommand.CLUSTER))
                                    {
                                        // We have encountered a connected server with a cluster type for the first time.
                                        // so we will get list of other nodes from this server using "CLUSTER NODES" command
                                        // and try to connect to these other nodes in the next iteration
                                        encounteredConnectedClusterServer = true;
                                        updatedClusterEndpointCollection = await GetEndpointsFromClusterNodes(server, log).ForAwait();
                                    }

                                    // Set the server UnselectableFlags and update primaries list
                                    switch (server.ServerType)
                                    {
                                        case ServerType.Twemproxy:
                                        case ServerType.Envoyproxy:
                                        case ServerType.Sentinel:
                                        case ServerType.Standalone:
                                        case ServerType.Cluster:
                                            server.ClearUnselectable(UnselectableFlags.ServerType);
                                            if (server.IsReplica)
                                            {
                                                server.ClearUnselectable(UnselectableFlags.RedundantPrimary);
                                            }
                                            else
                                            {
                                                primaries.Add(server);
                                            }
                                            break;
                                        default:
                                            server.SetUnselectable(UnselectableFlags.ServerType);
                                            break;
                                    }
                                }
                                else
                                {
                                    server.SetUnselectable(UnselectableFlags.DidNotRespond);
                                    log?.WriteLine($"  {Format.ToString(server)}: Returned, but incorrectly");
                                }
                            }
                            else
                            {
                                server.SetUnselectable(UnselectableFlags.DidNotRespond);
                                log?.WriteLine($"  {Format.ToString(server)}: Did not respond");
                            }
                        }

                        if (encounteredConnectedClusterServer)
                        {
                            endpoints = updatedClusterEndpointCollection;
                        }
                        else
                        {
                            break; // We do not want to repeat the second iteration
                        }
                    }

                    if (clusterCount == 0)
                    {
                        // Set the serverSelectionStrategy
                        if (RawConfig.Proxy == Proxy.Twemproxy)
                        {
                            ServerSelectionStrategy.ServerType = ServerType.Twemproxy;
                        }
                        else if (RawConfig.Proxy == Proxy.Envoyproxy)
                        {
                            ServerSelectionStrategy.ServerType = ServerType.Envoyproxy;
                        }
                        else if (standaloneCount == 0 && sentinelCount > 0)
                        {
                            ServerSelectionStrategy.ServerType = ServerType.Sentinel;
                        }
                        else if (standaloneCount > 0)
                        {
                            ServerSelectionStrategy.ServerType = ServerType.Standalone;
                        }

                        // If multiple primaries are detected, nominate the preferred one
                        // ...but not if the type of server we're connected to supports and expects multiple primaries
                        // ...for those cases, we want to allow sending to any primary endpoint.
                        if (ServerSelectionStrategy.ServerType.HasSinglePrimary())
                        {
                            var preferred = NominatePreferredPrimary(log, servers, useTieBreakers, primaries);
                            foreach (var primary in primaries)
                            {
                                if (primary == preferred || primary.IsReplica)
                                {
                                    log?.WriteLine($"{Format.ToString(primary)}: Clearing as RedundantPrimary");
                                    primary.ClearUnselectable(UnselectableFlags.RedundantPrimary);
                                }
                                else
                                {
                                    log?.WriteLine($"{Format.ToString(primary)}: Setting as RedundantPrimary");
                                    primary.SetUnselectable(UnselectableFlags.RedundantPrimary);
                                }
                            }
                        }
                    }
                    else
                    {
                        ServerSelectionStrategy.ServerType = ServerType.Cluster;
                        long coveredSlots = ServerSelectionStrategy.CountCoveredSlots();
                        log?.WriteLine($"Cluster: {coveredSlots} of {ServerSelectionStrategy.TotalSlots} slots covered");
                    }
                    if (!first)
                    {
                        // Calling the sync path here because it's all fire and forget
                        long subscriptionChanges = EnsureSubscriptions(CommandFlags.FireAndForget);
                        if (subscriptionChanges == 0)
                        {
                            log?.WriteLine("No subscription changes necessary");
                        }
                        else
                        {
                            log?.WriteLine($"Subscriptions attempting reconnect: {subscriptionChanges}");
                        }
                    }
                    if (showStats)
                    {
                        GetStatus(log);
                    }

                    string stormLog = GetStormLog();
                    if (!string.IsNullOrWhiteSpace(stormLog))
                    {
                        log?.WriteLine();
                        log?.WriteLine(stormLog);
                    }
                    healthy = standaloneCount != 0 || clusterCount != 0 || sentinelCount != 0;
                    if (first && !healthy && attemptsLeft > 0)
                    {
                        log?.WriteLine("Resetting failing connections to retry...");
                        ResetAllNonConnected();
                        log?.WriteLine($"  Retrying - attempts left: {attemptsLeft}...");
                    }
                    //WTF("?: " + attempts);
                } while (first && !healthy && attemptsLeft > 0);

                if (first && RawConfig.AbortOnConnectFail && !healthy)
                {
                    return false;
                }
                if (first)
                {
                    log?.WriteLine("Starting heartbeat...");
                    pulse = TimerToken.Create(this);
                }
                if (publishReconfigure)
                {
                    try
                    {
                        log?.WriteLine("Broadcasting reconfigure...");
                        PublishReconfigureImpl(publishReconfigureFlags);
                    }
                    catch
                    { }
                }
                return true;
            }
            catch (Exception ex)
            {
                Trace(ex.Message);
                throw;
            }
            finally
            {
                Trace("Exiting reconfiguration...");
                OnTraceLog(log);
                if (ranThisCall) Interlocked.Exchange(ref activeConfigCause, null);
                if (!first) OnConfigurationChanged(blame);
                Trace("Reconfiguration exited");
            }
        }

        private async Task<EndPointCollection> GetEndpointsFromClusterNodes(ServerEndPoint server, LogProxy log)
        {
            var message = Message.Create(-1, CommandFlags.None, RedisCommand.CLUSTER, RedisLiterals.NODES);
            try
            {
                var clusterConfig = await ExecuteAsyncImpl(message, ResultProcessor.ClusterNodes, null, server).ForAwait();
                var clusterEndpoints = new EndPointCollection(clusterConfig.Nodes.Select(node => node.EndPoint).ToList());
                // Loop through nodes in the cluster and update nodes relations to other nodes
                ServerEndPoint serverEndpoint = null;
                foreach (EndPoint endpoint in clusterEndpoints)
                {
                    serverEndpoint = GetServerEndPoint(endpoint);
                    if (serverEndpoint != null)
                    {
                        serverEndpoint.UpdateNodeRelations(clusterConfig);
                    }
                }
                return clusterEndpoints;
            }
            catch (Exception ex)
            {
                log?.WriteLine($"Encountered error while updating cluster config: {ex.Message}");
                return null;
            }
        }

        private void ResetAllNonConnected()
        {
            var snapshot = GetServerSnapshot();
            foreach (var server in snapshot)
            {
                server.ResetNonConnected();
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "Used - it's a partial")]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "Partial - may use instance data")]
        partial void OnTraceLog(LogProxy log, [CallerMemberName] string caller = null);

        private static ServerEndPoint NominatePreferredPrimary(LogProxy log, ServerEndPoint[] servers, bool useTieBreakers, List<ServerEndPoint> primaries)
        {
            log?.WriteLine("Election summary:");

            Dictionary<string, int> uniques = null;
            if (useTieBreakers)
            {
                // Count the votes
                uniques = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                for (int i = 0; i < servers.Length; i++)
                {
                    var server = servers[i];
                    string serverResult = server.TieBreakerResult;

                    if (string.IsNullOrWhiteSpace(serverResult))
                    {
                        log?.WriteLine($"  Election: {Format.ToString(server)} had no tiebreaker set");
                    }
                    else
                    {
                        log?.WriteLine($"  Election: {Format.ToString(server)} nominates: {serverResult}");
                        if (!uniques.TryGetValue(serverResult, out int count)) count = 0;
                        uniques[serverResult] = count + 1;
                    }
                }
            }

            switch (primaries.Count)
            {
                case 0:
                    log?.WriteLine("  Election: No primaries detected");
                    return null;
                case 1:
                    log?.WriteLine($"  Election: Single primary detected: {Format.ToString(primaries[0].EndPoint)}");
                    return primaries[0];
                default:
                    log?.WriteLine("  Election: Multiple primaries detected...");
                    if (useTieBreakers && uniques != null)
                    {
                        switch (uniques.Count)
                        {
                            case 0:
                                log?.WriteLine("  Election: No nominations by tie-breaker");
                                break;
                            case 1:
                                string unanimous = uniques.Keys.Single();
                                log?.WriteLine($"  Election: Tie-breaker unanimous: {unanimous}");
                                var found = SelectServerByElection(servers, unanimous, log);
                                if (found != null)
                                {
                                    log?.WriteLine($"  Election: Elected: {Format.ToString(found.EndPoint)}");
                                    return found;
                                }
                                break;
                            default:
                                log?.WriteLine("  Election is contested:");
                                ServerEndPoint highest = null;
                                bool arbitrary = false;
                                foreach (var pair in uniques.OrderByDescending(x => x.Value))
                                {
                                    log?.WriteLine($"    Election: {pair.Key} has {pair.Value} votes");
                                    if (highest == null)
                                    {
                                        highest = SelectServerByElection(servers, pair.Key, log);
                                        if (highest != null)
                                        {
                                            // any more with this vote? if so: arbitrary
                                            arbitrary = uniques.Where(x => x.Value == pair.Value).Skip(1).Any();
                                        }
                                    }
                                }
                                if (highest != null)
                                {
                                    if (arbitrary)
                                    {
                                        log?.WriteLine($"  Election: Choosing primary arbitrarily: {Format.ToString(highest.EndPoint)}");
                                    }
                                    else
                                    {
                                        log?.WriteLine($"  Election: Elected: {Format.ToString(highest.EndPoint)}");
                                    }
                                    return highest;
                                }
                                break;
                        }
                    }
                    break;
            }

            log?.WriteLine($"  Election: Choosing primary arbitrarily: {Format.ToString(primaries[0].EndPoint)}");
            return primaries[0];
        }

        private static ServerEndPoint SelectServerByElection(ServerEndPoint[] servers, string endpoint, LogProxy log)
        {
            if (servers == null || string.IsNullOrWhiteSpace(endpoint)) return null;
            for (int i = 0; i < servers.Length; i++)
            {
                if (string.Equals(Format.ToString(servers[i].EndPoint), endpoint, StringComparison.OrdinalIgnoreCase))
                    return servers[i];
            }
            log?.WriteLine("...but we couldn't find that");
            var deDottedEndpoint = DeDotifyHost(endpoint);
            for (int i = 0; i < servers.Length; i++)
            {
                if (string.Equals(DeDotifyHost(Format.ToString(servers[i].EndPoint)), deDottedEndpoint, StringComparison.OrdinalIgnoreCase))
                {
                    log?.WriteLine($"...but we did find instead: {deDottedEndpoint}");
                    return servers[i];
                }
            }
            return null;
        }

        private static string DeDotifyHost(string input)
        {
            if (string.IsNullOrWhiteSpace(input)) return input; // GIGO

            if (!char.IsLetter(input[0])) return input; // Need first char to be alpha for this to work

            int periodPosition = input.IndexOf('.');
            if (periodPosition <= 0) return input; // No period or starts with a period? Then nothing useful to split

            int colonPosition = input.IndexOf(':');
            if (colonPosition > 0)
            {
                // Has a port specifier
#if NETCOREAPP
                return string.Concat(input.AsSpan(0, periodPosition), input.AsSpan(colonPosition));
#else
                return input.Substring(0, periodPosition) + input.Substring(colonPosition);
#endif
            }
            else
            {
                return input.Substring(0, periodPosition);
            }
        }

        internal void UpdateClusterRange(ClusterConfiguration configuration)
        {
            if (configuration == null) return;
            foreach (var node in configuration.Nodes)
            {
                if (node.IsReplica || node.Slots.Count == 0) continue;
                foreach (var slot in node.Slots)
                {
                    var server = GetServerEndPoint(node.EndPoint);
                    if (server != null) ServerSelectionStrategy.UpdateClusterRange(slot.From, slot.To, server);
                }
            }
        }

        private IDisposable pulse;

        internal ServerEndPoint SelectServer(Message message) =>
            message == null ? null : ServerSelectionStrategy.Select(message);

        internal ServerEndPoint SelectServer(RedisCommand command, CommandFlags flags, in RedisKey key) =>
            ServerSelectionStrategy.Select(command, key, flags);

        internal ServerEndPoint SelectServer(RedisCommand command, CommandFlags flags, in RedisChannel channel) =>
            ServerSelectionStrategy.Select(command, channel, flags);

        private bool PrepareToPushMessageToBridge<T>(Message message, ResultProcessor<T> processor, IResultBox<T> resultBox, ref ServerEndPoint server)
        {
            message.SetSource(processor, resultBox);

            if (server == null)
            {
                // Infer a server automatically
                server = SelectServer(message);

                // If we didn't find one successfully, and we're allowed, queue for any viable server
                if (server == null && message != null && RawConfig.BacklogPolicy.QueueWhileDisconnected)
                {
                    server = ServerSelectionStrategy.Select(message, allowDisconnected: true);
                }
            }
            else // A server was specified - do we trust their choice, though?
            {
                if (message.IsPrimaryOnly() && server.IsReplica)
                {
                    throw ExceptionFactory.PrimaryOnly(IncludeDetailInExceptions, message.Command, message, server);
                }

                switch (server.ServerType)
                {
                    case ServerType.Cluster:
                        if (message.GetHashSlot(ServerSelectionStrategy) == ServerSelectionStrategy.MultipleSlots)
                        {
                            throw ExceptionFactory.MultiSlot(IncludeDetailInExceptions, message);
                        }
                        break;
                }

                // If we're not allowed to queue while disconnected, we'll bomb out below.
                if (!server.IsConnected && !RawConfig.BacklogPolicy.QueueWhileDisconnected)
                {
                    // Well, that's no use!
                    server = null;
                }
            }

            if (server != null)
            {
                var profilingSession = _profilingSessionProvider?.Invoke();
                if (profilingSession != null)
                {
                    message.SetProfileStorage(ProfiledCommand.NewWithContext(profilingSession, server));
                }

                if (message.Db >= 0)
                {
                    int availableDatabases = server.Databases;
                    if (availableDatabases > 0 && message.Db >= availableDatabases)
                    {
                        throw ExceptionFactory.DatabaseOutfRange(IncludeDetailInExceptions, message.Db, message, server);
                    }
                }

                Trace("Queuing on server: " + message);
                return true;
            }
            Trace("No server or server unavailable - aborting: " + message);
            return false;
        }
        private ValueTask<WriteResult> TryPushMessageToBridgeAsync<T>(Message message, ResultProcessor<T> processor, IResultBox<T> resultBox, ref ServerEndPoint server)
            => PrepareToPushMessageToBridge(message, processor, resultBox, ref server) ? server.TryWriteAsync(message) : new ValueTask<WriteResult>(WriteResult.NoConnectionAvailable);

        [Obsolete("prefer async")]
        private WriteResult TryPushMessageToBridgeSync<T>(Message message, ResultProcessor<T> processor, IResultBox<T> resultBox, ref ServerEndPoint server)
            => PrepareToPushMessageToBridge(message, processor, resultBox, ref server) ? server.TryWriteSync(message) : WriteResult.NoConnectionAvailable;

        /// <summary>
        /// Gets the client name for this multiplexer.
        /// </summary>
        public override string ToString()
        {
            string s = ClientName;
            if (string.IsNullOrWhiteSpace(s)) s = GetType().Name;
            return s;
        }

        internal readonly byte[] ConfigurationChangedChannel; // This gets accessed for every received event; let's make sure we can process it "raw"
        internal readonly byte[] UniqueId = Guid.NewGuid().ToByteArray(); // Unique identifier used when tracing

        /// <summary>
        /// Gets or sets whether asynchronous operations should be invoked in a way that guarantees their original delivery order.
        /// </summary>
        [Obsolete("Not supported; if you require ordered pub/sub, please see " + nameof(ChannelMessageQueue) + ", will be removed in 3.0", false)]
        public bool PreserveAsyncOrder
        {
            get => false;
            set { }
        }

        /// <summary>
        /// Indicates whether any servers are connected.
        /// </summary>
        public bool IsConnected
        {
            get
            {
                var tmp = GetServerSnapshot();
                for (int i = 0; i < tmp.Length; i++)
                    if (tmp[i].IsConnected) return true;
                return false;
            }
        }

        /// <summary>
        /// Indicates whether any servers are currently trying to connect.
        /// </summary>
        public bool IsConnecting
        {
            get
            {
                var tmp = GetServerSnapshot();
                for (int i = 0; i < tmp.Length; i++)
                    if (tmp[i].IsConnecting) return true;
                return false;
            }
        }

        internal ConfigurationOptions RawConfig { get; }

        internal ServerSelectionStrategy ServerSelectionStrategy { get; }

        internal Timer sentinelPrimaryReconnectTimer;

        internal Dictionary<string, ConnectionMultiplexer> sentinelConnectionChildren = new Dictionary<string, ConnectionMultiplexer>();
        internal ConnectionMultiplexer sentinelConnection = null;

        /// <summary>
        /// Initializes the connection as a Sentinel connection and adds the necessary event handlers to track changes to the managed primaries.
        /// </summary>
        /// <param name="logProxy">The writer to log to, if any.</param>
        internal void InitializeSentinel(LogProxy logProxy)
        {
            if (ServerSelectionStrategy.ServerType != ServerType.Sentinel)
            {
                return;
            }

            // Subscribe to sentinel change events
            ISubscriber sub = GetSubscriber();

            if (sub.SubscribedEndpoint("+switch-master") == null)
            {
                sub.Subscribe("+switch-master", (_, message) =>
                {
                    string[] messageParts = ((string)message).Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                    EndPoint switchBlame = Format.TryParseEndPoint(string.Format("{0}:{1}", messageParts[1], messageParts[2]));

                    lock (sentinelConnectionChildren)
                    {
                        // Switch the primary if we have connections for that service
                        if (sentinelConnectionChildren.ContainsKey(messageParts[0]))
                        {
                            ConnectionMultiplexer child = sentinelConnectionChildren[messageParts[0]];

                            // Is the connection still valid?
                            if (child.IsDisposed)
                            {
                                child.ConnectionFailed -= OnManagedConnectionFailed;
                                child.ConnectionRestored -= OnManagedConnectionRestored;
                                sentinelConnectionChildren.Remove(messageParts[0]);
                            }
                            else
                            {
                                SwitchPrimary(switchBlame, sentinelConnectionChildren[messageParts[0]]);
                            }
                        }
                    }
                }, CommandFlags.FireAndForget);
            }

            // If we lose connection to a sentinel server,
            // we need to reconfigure to make sure we still have a subscription to the +switch-master channel
            ConnectionFailed += (sender, e) =>
            {
                // Reconfigure to get subscriptions back online
                ReconfigureAsync(first: false, reconfigureAll: true, logProxy, e.EndPoint, "Lost sentinel connection", false).Wait();
            };

            // Subscribe to new sentinels being added
            if (sub.SubscribedEndpoint("+sentinel") == null)
            {
                sub.Subscribe("+sentinel", (_, message) =>
                {
                    string[] messageParts = ((string)message).Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                    UpdateSentinelAddressList(messageParts[0]);
                }, CommandFlags.FireAndForget);
            }
        }

        /// <summary>
        /// Returns a managed connection to the primary server indicated by the <see cref="ConfigurationOptions.ServiceName"/> in the config.
        /// </summary>
        /// <param name="config">The configuration to be used when connecting to the primary.</param>
        /// <param name="log">The writer to log to, if any.</param>
        public ConnectionMultiplexer GetSentinelMasterConnection(ConfigurationOptions config, TextWriter log = null)
        {
            if (ServerSelectionStrategy.ServerType != ServerType.Sentinel)
            {
                throw new RedisConnectionException(ConnectionFailureType.UnableToConnect,
                    "Sentinel: The ConnectionMultiplexer is not a Sentinel connection. Detected as: " + ServerSelectionStrategy.ServerType);
            }

            if (string.IsNullOrEmpty(config.ServiceName))
                throw new ArgumentException("A ServiceName must be specified.");

            lock (sentinelConnectionChildren)
            {
                if (sentinelConnectionChildren.TryGetValue(config.ServiceName, out var sentinelConnectionChild) && !sentinelConnectionChild.IsDisposed)
                    return sentinelConnectionChild;
            }

            bool success = false;
            ConnectionMultiplexer connection = null;

            var sw = ValueStopwatch.StartNew();
            do
            {
                // Get an initial endpoint - try twice
                EndPoint newPrimaryEndPoint = GetConfiguredPrimaryForService(config.ServiceName)
                                             ?? GetConfiguredPrimaryForService(config.ServiceName);

                if (newPrimaryEndPoint == null)
                {
                    throw new RedisConnectionException(ConnectionFailureType.UnableToConnect,
                        $"Sentinel: Failed connecting to configured primary for service: {config.ServiceName}");
                }

                EndPoint[] replicaEndPoints = GetReplicasForService(config.ServiceName)
                                           ?? GetReplicasForService(config.ServiceName);

                // Replace the primary endpoint, if we found another one
                // If not, assume the last state is the best we have and minimize the race
                if (config.EndPoints.Count == 1)
                {
                    config.EndPoints[0] = newPrimaryEndPoint;
                }
                else
                {
                    config.EndPoints.Clear();
                    config.EndPoints.TryAdd(newPrimaryEndPoint);
                }

                foreach (var replicaEndPoint in replicaEndPoints)
                {
                    config.EndPoints.TryAdd(replicaEndPoint);
                }

                connection = ConnectImpl(config, log);

                // verify role is primary according to:
                // https://redis.io/topics/sentinel-clients
                if (connection.GetServer(newPrimaryEndPoint)?.Role().Value == RedisLiterals.master)
                {
                    success = true;
                    break;
                }

                Thread.Sleep(100);
            } while (sw.ElapsedMilliseconds < config.ConnectTimeout);

            if (!success)
            {
                throw new RedisConnectionException(ConnectionFailureType.UnableToConnect,
                    $"Sentinel: Failed connecting to configured primary for service: {config.ServiceName}");
            }

            // Attach to reconnect event to ensure proper connection to the new primary
            connection.ConnectionRestored += OnManagedConnectionRestored;

            // If we lost the connection, run a switch to a least try and get updated info about the primary
            connection.ConnectionFailed += OnManagedConnectionFailed;

            lock (sentinelConnectionChildren)
            {
                sentinelConnectionChildren[connection.RawConfig.ServiceName] = connection;
            }

            // Perform the initial switchover
            SwitchPrimary(RawConfig.EndPoints[0], connection, log);

            return connection;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Roslynator", "RCS1075:Avoid empty catch clause that catches System.Exception.", Justification = "We don't care.")]
        internal void OnManagedConnectionRestored(object sender, ConnectionFailedEventArgs e)
        {
            ConnectionMultiplexer connection = (ConnectionMultiplexer)sender;

            var oldTimer = Interlocked.Exchange(ref connection.sentinelPrimaryReconnectTimer, null);
            oldTimer?.Dispose();

            try
            {
                // Run a switch to make sure we have update-to-date
                // information about which primary we should connect to
                SwitchPrimary(e.EndPoint, connection);

                try
                {
                    // Verify that the reconnected endpoint is a primary,
                    // and the correct one otherwise we should reconnect
                    if (connection.GetServer(e.EndPoint).IsReplica || e.EndPoint != connection.currentSentinelPrimaryEndPoint)
                    {
                        // This isn't a primary, so try connecting again
                        SwitchPrimary(e.EndPoint, connection);
                    }
                }
                catch (Exception)
                {
                    // If we get here it means that we tried to reconnect to a server that is no longer
                    // considered a primary by Sentinel and was removed from the list of endpoints.

                    // If we caught an exception, we may have gotten a stale endpoint
                    // we are not aware of, so retry
                    SwitchPrimary(e.EndPoint, connection);
                }
            }
            catch (Exception)
            {
                // Log, but don't throw in an event handler
                // TODO: Log via new event handler? a la ConnectionFailed?
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Roslynator", "RCS1075:Avoid empty catch clause that catches System.Exception.", Justification = "We don't care.")]
        internal void OnManagedConnectionFailed(object sender, ConnectionFailedEventArgs e)
        {
            ConnectionMultiplexer connection = (ConnectionMultiplexer)sender;
            // Periodically check to see if we can reconnect to the proper primary.
            // This is here in case we lost our subscription to a good sentinel instance
            // or if we miss the published primary change.
            if (connection.sentinelPrimaryReconnectTimer == null)
            {
                connection.sentinelPrimaryReconnectTimer = new Timer(_ =>
                {
                    try
                    {
                        // Attempt, but do not fail here
                        SwitchPrimary(e.EndPoint, connection);
                    }
                    catch (Exception)
                    {
                    }
                    finally
                    {
                        connection.sentinelPrimaryReconnectTimer?.Change(TimeSpan.FromSeconds(1), Timeout.InfiniteTimeSpan);
                    }
                }, null, TimeSpan.Zero, Timeout.InfiniteTimeSpan);
            }
        }

        internal EndPoint GetConfiguredPrimaryForService(string serviceName) =>
            GetServerSnapshot()
                .ToArray()
                .Where(s => s.ServerType == ServerType.Sentinel)
                .AsParallel()
                .Select(s =>
                {
                    try { return GetServer(s.EndPoint).SentinelGetMasterAddressByName(serviceName); }
                    catch { return null; }
                })
                .FirstOrDefault(r => r != null);

        internal EndPoint currentSentinelPrimaryEndPoint;

        internal EndPoint[] GetReplicasForService(string serviceName) =>
            GetServerSnapshot()
                .ToArray()
                .Where(s => s.ServerType == ServerType.Sentinel)
                .AsParallel()
                .Select(s =>
                {
                    try { return GetServer(s.EndPoint).SentinelGetReplicaAddresses(serviceName); }
                    catch { return null; }
                })
                .FirstOrDefault(r => r != null);

        /// <summary>
        /// Switches the SentinelMasterConnection over to a new primary.
        /// </summary>
        /// <param name="switchBlame">The endpoint responsible for the switch.</param>
        /// <param name="connection">The connection that should be switched over to a new primary endpoint.</param>
        /// <param name="log">The writer to log to, if any.</param>
        internal void SwitchPrimary(EndPoint switchBlame, ConnectionMultiplexer connection, TextWriter log = null)
        {
            if (log == null) log = TextWriter.Null;

            using (var logProxy = LogProxy.TryCreate(log))
            {
                string serviceName = connection.RawConfig.ServiceName;

                // Get new primary - try twice
                EndPoint newPrimaryEndPoint = GetConfiguredPrimaryForService(serviceName)
                                           ?? GetConfiguredPrimaryForService(serviceName)
                                           ?? throw new RedisConnectionException(ConnectionFailureType.UnableToConnect,
                                                $"Sentinel: Failed connecting to switch primary for service: {serviceName}");

                connection.currentSentinelPrimaryEndPoint = newPrimaryEndPoint;

                if (!connection.servers.Contains(newPrimaryEndPoint))
                {
                    EndPoint[] replicaEndPoints = GetReplicasForService(serviceName)
                                               ?? GetReplicasForService(serviceName);

                    connection.servers.Clear();
                    connection.RawConfig.EndPoints.Clear();
                    connection.RawConfig.EndPoints.TryAdd(newPrimaryEndPoint);
                    foreach (var replicaEndPoint in replicaEndPoints)
                    {
                        connection.RawConfig.EndPoints.TryAdd(replicaEndPoint);
                    }
                    Trace($"Switching primary to {newPrimaryEndPoint}");
                    // Trigger a reconfigure
                    connection.ReconfigureAsync(first: false, reconfigureAll: false, logProxy, switchBlame,
                        $"Primary switch {serviceName}", false, CommandFlags.PreferMaster).Wait();

                    UpdateSentinelAddressList(serviceName);
                }
            }
        }

        internal void UpdateSentinelAddressList(string serviceName)
        {
            var firstCompleteRequest = GetServerSnapshot()
                                        .ToArray()
                                        .Where(s => s.ServerType == ServerType.Sentinel)
                                        .AsParallel()
                                        .Select(s =>
                                        {
                                            try { return GetServer(s.EndPoint).SentinelGetSentinelAddresses(serviceName); }
                                            catch { return null; }
                                        })
                                        .FirstOrDefault(r => r != null);

            // Ignore errors, as having an updated sentinel list is not essential
            if (firstCompleteRequest == null)
                return;

            bool hasNew = false;
            foreach (EndPoint newSentinel in firstCompleteRequest.Where(x => !RawConfig.EndPoints.Contains(x)))
            {
                hasNew = true;
                RawConfig.EndPoints.TryAdd(newSentinel);
            }

            if (hasNew)
            {
                // Reconfigure the sentinel multiplexer if we added new endpoints
                ReconfigureAsync(first: false, reconfigureAll: true, null, RawConfig.EndPoints[0], "Updating Sentinel List", false).Wait();
            }
        }

        /// <summary>
        /// Close all connections and release all resources associated with this object.
        /// </summary>
        /// <param name="allowCommandsToComplete">Whether to allow all in-queue commands to complete first.</param>
        public void Close(bool allowCommandsToComplete = true)
        {
            if (_isDisposed) return;

            OnClosing(false);
            _isDisposed = true;
            _profilingSessionProvider = null;
            using (var tmp = pulse)
            {
                pulse = null;
            }

            if (allowCommandsToComplete)
            {
                var quits = QuitAllServers();
                WaitAllIgnoreErrors(quits);
            }
            DisposeAndClearServers();
            OnCloseReaderWriter();
            OnClosing(true);
            Interlocked.Increment(ref _connectionCloseCount);
        }

        partial void OnCloseReaderWriter();

        private void DisposeAndClearServers()
        {
            lock (servers)
            {
                var iter = servers.GetEnumerator();
                while (iter.MoveNext())
                {
                    var server = (ServerEndPoint)iter.Value;
                    server.Dispose();
                }
                servers.Clear();
            }
        }

        private Task[] QuitAllServers()
        {
            var quits = new Task[2 * servers.Count];
            lock (servers)
            {
                var iter = servers.GetEnumerator();
                int index = 0;
                while (iter.MoveNext())
                {
                    var server = (ServerEndPoint)iter.Value;
                    quits[index++] = server.Close(ConnectionType.Interactive);
                    quits[index++] = server.Close(ConnectionType.Subscription);
                }
            }
            return quits;
        }

        /// <summary>
        /// Close all connections and release all resources associated with this object.
        /// </summary>
        /// <param name="allowCommandsToComplete">Whether to allow all in-queue commands to complete first.</param>
        public async Task CloseAsync(bool allowCommandsToComplete = true)
        {
            _isDisposed = true;
            using (var tmp = pulse)
            {
                pulse = null;
            }

            if (allowCommandsToComplete)
            {
                var quits = QuitAllServers();
                await WaitAllIgnoreErrorsAsync("quit", quits, RawConfig.AsyncTimeout, null).ForAwait();
            }

            DisposeAndClearServers();
        }

        /// <summary>
        /// Release all resources associated with this object.
        /// </summary>
        public void Dispose()
        {
            GC.SuppressFinalize(this);
            Close(!_isDisposed);
            sentinelConnection?.Dispose();
            var oldTimer = Interlocked.Exchange(ref sentinelPrimaryReconnectTimer, null);
            oldTimer?.Dispose();
        }

        internal Task<T> ExecuteAsyncImpl<T>(Message message, ResultProcessor<T> processor, object state, ServerEndPoint server)
        {
            if (_isDisposed) throw new ObjectDisposedException(ToString());

            if (message == null)
            {
                return CompletedTask<T>.Default(state);
            }

            TaskCompletionSource<T> tcs = null;
            IResultBox<T> source = null;
            if (!message.IsFireAndForget)
            {
                source = TaskResultBox<T>.Create(out tcs, state);
            }
            var write = TryPushMessageToBridgeAsync(message, processor, source, ref server);
            if (!write.IsCompletedSuccessfully) return ExecuteAsyncImpl_Awaited<T>(this, write, tcs, message, server);

            if (tcs == null)
            {
                return CompletedTask<T>.Default(null); // F+F explicitly does not get async-state
            }
            else
            {
                var result = write.Result;
                if (result != WriteResult.Success)
                {
                    var ex = GetException(result, message, server);
                    ThrowFailed(tcs, ex);
                }
                return tcs.Task;
            }
        }

        private static async Task<T> ExecuteAsyncImpl_Awaited<T>(ConnectionMultiplexer @this, ValueTask<WriteResult> write, TaskCompletionSource<T> tcs, Message message, ServerEndPoint server)
        {
            var result = await write.ForAwait();
            if (result != WriteResult.Success)
            {
                var ex = @this.GetException(result, message, server);
                ThrowFailed(tcs, ex);
            }
            return tcs == null ? default(T) : await tcs.Task.ForAwait();
        }

        internal Exception GetException(WriteResult result, Message message, ServerEndPoint server) => result switch
        {
            WriteResult.Success => null,
            WriteResult.NoConnectionAvailable => ExceptionFactory.NoConnectionAvailable(this, message, server),
            WriteResult.TimeoutBeforeWrite => ExceptionFactory.Timeout(this, "The timeout was reached before the message could be written to the output buffer, and it was not sent", message, server, result),
            _ => ExceptionFactory.ConnectionFailure(IncludeDetailInExceptions, ConnectionFailureType.ProtocolFailure, "An unknown error occurred when writing the message", server),
        };

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA1816:Dispose methods should call SuppressFinalize", Justification = "Intentional observation")]
        internal static void ThrowFailed<T>(TaskCompletionSource<T> source, Exception unthrownException)
        {
            try
            {
                throw unthrownException;
            }
            catch (Exception ex)
            {
                source.TrySetException(ex);
                GC.KeepAlive(source.Task.Exception);
                GC.SuppressFinalize(source.Task);
            }
        }

        internal T ExecuteSyncImpl<T>(Message message, ResultProcessor<T> processor, ServerEndPoint server)
        {
            if (_isDisposed) throw new ObjectDisposedException(ToString());

            if (message == null) // Fire-and forget could involve a no-op, represented by null - for example Increment by 0
            {
                return default(T);
            }

            if (message.IsFireAndForget)
            {
#pragma warning disable CS0618 // Type or member is obsolete
                TryPushMessageToBridgeSync(message, processor, null, ref server);
#pragma warning restore CS0618
                Interlocked.Increment(ref fireAndForgets);
                return default(T);
            }
            else
            {
                var source = SimpleResultBox<T>.Get();

                lock (source)
                {
#pragma warning disable CS0618 // Type or member is obsolete
                    var result = TryPushMessageToBridgeSync(message, processor, source, ref server);
#pragma warning restore CS0618
                    if (result != WriteResult.Success)
                    {
                        throw GetException(result, message, server);
                    }

                    if (Monitor.Wait(source, TimeoutMilliseconds))
                    {
                        Trace("Timely response to " + message);
                    }
                    else
                    {
                        Trace("Timeout performing " + message);
                        Interlocked.Increment(ref syncTimeouts);
                        throw ExceptionFactory.Timeout(this, null, message, server);
                        // Very important not to return "source" to the pool here
                    }
                }
                // Snapshot these so that we can recycle the box
                var val = source.GetResult(out var ex, canRecycle: true); // now that we aren't locking it...
                if (ex != null) throw ex;
                Trace(message + " received " + val);
                return val;
            }
        }

        /// <summary>
        /// Should exceptions include identifiable details? (key names, additional .Data annotations)
        /// </summary>
        public bool IncludeDetailInExceptions { get; set; }

        /// <summary>
        /// Should exceptions include performance counter details? (CPU usage, etc - note that this can be problematic on some platforms)
        /// </summary>
        public bool IncludePerformanceCountersInExceptions { get; set; }

        internal int haveStormLog = 0;
        internal string stormLogSnapshot;
        /// <summary>
        /// Limit at which to start recording unusual busy patterns (only one log will be retained at a time).
        /// Set to a negative value to disable this feature.
        /// </summary>
        public int StormLogThreshold { get; set; } = 15;

        /// <summary>
        /// Obtains the log of unusual busy patterns.
        /// </summary>
        public string GetStormLog() => Volatile.Read(ref stormLogSnapshot);

        /// <summary>
        /// Resets the log of unusual busy patterns.
        /// </summary>
        public void ResetStormLog()
        {
            Interlocked.Exchange(ref stormLogSnapshot, null);
            Interlocked.Exchange(ref haveStormLog, 0);
        }

        private long syncTimeouts, fireAndForgets, asyncTimeouts;

        internal void OnAsyncTimeout() => Interlocked.Increment(ref asyncTimeouts);

        /// <summary>
        /// Sends request to all compatible clients to reconfigure or reconnect.
        /// </summary>
        /// <param name="flags">The command flags to use.</param>
        /// <returns>The number of instances known to have received the message (however, the actual number can be higher; returns -1 if the operation is pending).</returns>
        public long PublishReconfigure(CommandFlags flags = CommandFlags.None)
        {
            byte[] channel = ConfigurationChangedChannel;
            if (channel == null) return 0;
            if (ReconfigureIfNeeded(null, false, "PublishReconfigure", true, flags))
            {
                return -1;
            }
            else
            {
                return PublishReconfigureImpl(flags);
            }
        }

        private long PublishReconfigureImpl(CommandFlags flags)
        {
            byte[] channel = ConfigurationChangedChannel;
            if (channel == null) return 0;
            return GetSubscriber().Publish(channel, RedisLiterals.Wildcard, flags);
        }

        /// <summary>
        /// Sends request to all compatible clients to reconfigure or reconnect.
        /// </summary>
        /// <param name="flags">The command flags to use.</param>
        /// <returns>The number of instances known to have received the message (however, the actual number can be higher).</returns>
        public Task<long> PublishReconfigureAsync(CommandFlags flags = CommandFlags.None)
        {
            byte[] channel = ConfigurationChangedChannel;
            if (channel == null) return CompletedTask<long>.Default(null);

            return GetSubscriber().PublishAsync(channel, RedisLiterals.Wildcard, flags);
        }

        /// <summary>
        /// Get the hash-slot associated with a given key, if applicable.
        /// This can be useful for grouping operations.
        /// </summary>
        /// <param name="key">The <see cref="RedisKey"/> to determine the hash slot for.</param>
        public int GetHashSlot(RedisKey key) => ServerSelectionStrategy.HashSlot(key);
    }

    internal enum WriteResult
    {
        Success,
        NoConnectionAvailable,
        TimeoutBeforeWrite,
        WriteFailure,
    }
}
