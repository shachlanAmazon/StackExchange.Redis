using log4net;
using Newtonsoft.Json.Linq;
using Polly;
using Polly.Retry;
using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Authentication;
using System.Xml.Linq;
using ZeroFormatter;

namespace Intuit.Tax.DataProvider
{
    public class MemoryDBDataCacheProvider : DataCacheProvider
    {
        private static readonly ILog logger = LogManager.GetLogger("MemoryDBDataCacheProvider");
        private static readonly string MODIFIED_GROUPS_GET_LUA_SCRIPT = GetWriteModifiedAttributesDataLuaScript();
        private static readonly string WRITE_GROUPS_LUA_SCRIPT = GetWriteGroupsLuaScript();

        // do not use this instance directly
        private static ConnectionMultiplexer INSTANCE;

        private static List<ConnectionMultiplexer> INSTANCE_POOL;


        private static object lockObj = new object();

        private IEncryptor iEncryptor;
        private SecretAccessor secretAccessor;
        //private ConnectionMultiplexer connection;

        private static readonly RetryPolicy<long> LONG_RETURN_RETRY_POLICY = CreateRetryPolicy<long>();
        private static readonly RetryPolicy<bool> BOOL_RETURN_RETRY_POLICY = CreateRetryPolicy<bool>();
        private static readonly RetryPolicy<RedisResult> REDIS_RESULT_RETURN_RETRY_POLICY = CreateRetryPolicy<RedisResult>();
        private static readonly RetryPolicy<RedisValue[]> REDIS_VALUES_RETURN_RETRY_POLICY = CreateRetryPolicy<RedisValue[]>();

        private static readonly AverageLogger clearAverageLogger = new AverageLogger("MemoryDB Clear", MemoryDBBaseSettings.ReportingIntervalInSecs);
        private static readonly AverageLogger clearModifiedAverageLogger = new AverageLogger("MemoryDB Clear Modified", MemoryDBBaseSettings.ReportingIntervalInSecs);
        private static readonly AverageLogger existsAverageLogger = new AverageLogger("MemoryDB Exists", MemoryDBBaseSettings.ReportingIntervalInSecs);
        private static readonly AverageLogger getModifiedAverageLogger = new AverageLogger("MemoryDB Get Modified", MemoryDBBaseSettings.ReportingIntervalInSecs);
        private static readonly AverageLogger fileAverageLogger = new AverageLogger("MemoryDB Get File", MemoryDBBaseSettings.ReportingIntervalInSecs);
        private static readonly AverageLogger updateModifiedAverageLogger = new AverageLogger("MemoryDB Update Modified", MemoryDBBaseSettings.ReportingIntervalInSecs);
        private static readonly AverageLogger writeGroupsAverageLogger = new AverageLogger("MemoryDB Write Groups", MemoryDBBaseSettings.ReportingIntervalInSecs);
        private static readonly AverageLogger getGroupsAverageLogger = new AverageLogger("MemoryDB Get Groups", MemoryDBBaseSettings.ReportingIntervalInSecs);


        private readonly ConcurrentDictionary<string, string> lastVersionLookup = new ConcurrentDictionary<string, string>();

        private int dataCacheItemExpirySeconds;
        private bool dataCacheEnableExpiryCheck;

        /// <summary>
        /// General Constructor
        /// </summary>
        /// <param name="dataCacheItemExpirySeconds">Time expiry in seconds</param>
        /// <param name="dataCacheEnableExpiryCheck">Is expiry disabled</param>
        /// <param name="iEncryptor">The encryptor used for encrypting data</param>
        /// <param name="secretAccessor">Used to access secrets</param>
        public MemoryDBDataCacheProvider(int dataCacheItemExpirySeconds, bool dataCacheEnableExpiryCheck, IEncryptor iEncryptor, SecretAccessor secretAccessor)
        : this(dataCacheItemExpirySeconds, dataCacheEnableExpiryCheck, iEncryptor, secretAccessor, null)
        { }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dataCacheItemExpirySeconds">Time expiry in seconds</param>
        /// <param name="dataCacheEnableExpiryCheck">Is expiry disabled</param>
        /// <param name="iEncryptor">The encryptor used for encrypting data</param>
        /// <param name="secretAccessor">Used to access secrets</param>
        /// <param name="connection">MemoryDB Multiplexer</param>
        public MemoryDBDataCacheProvider(int dataCacheItemExpirySeconds, bool dataCacheEnableExpiryCheck,
            IEncryptor iEncryptor, SecretAccessor secretAccessor, ConnectionMultiplexer connection)
        {
            this.iEncryptor = iEncryptor;
            this.secretAccessor = secretAccessor;
            this.dataCacheItemExpirySeconds = dataCacheItemExpirySeconds;
            this.dataCacheEnableExpiryCheck = dataCacheEnableExpiryCheck;

            // Check if the singleton instance has been created
            //if (connection == null)
            //{
            //    this.connection = GetInstance();
            //} else
            //{
            //    this.connection = connection;
            //}
        }

        /// <summary>
        /// Retrieves the singleton instance of the ConnectionMultiplexer, or creates it, if it
        /// doesn't exist yet.
        /// </summary>
        /// <returns></returns>
        private ConnectionMultiplexer GetInstance()
        {
            // double locking
            if (INSTANCE_POOL == null)
            {
                lock (lockObj)
                {
                    if (INSTANCE_POOL == null)
                    {
                        // create instance
                        //INSTANCE = CreateInstance();

                        var pool = new List<ConnectionMultiplexer>();
                        for (int i = 0; i < 30; i++)
                        {
                            pool.Add(CreateInstance());
                        }

                        INSTANCE_POOL = pool;
                    }
                }
            }

            // return singleton instance
            //return INSTANCE;

            return INSTANCE_POOL
                .OrderBy(a => a.GetCounters().TotalOutstanding)
                .First();
        }

        /// <summary>
        /// Creates an instance of the ConnectionMultiplexer for REDIS/MemoryDB based on the various
        /// configurations in our web config.
        /// </summary>
        /// <returns></returns>
        private ConnectionMultiplexer CreateInstance()
        {
            try
            {
                var options = GetConnectionOptions();

                // permissions required:
                // on ~tcs::* &* -@all +@read +@write +@set +@sortedset +@list +@hash +@string +@blocking +@connection +@transaction +@scripting +@pubsub +@keyspace +info +config +client +cluster
                var multiPlexer = ConnectionMultiplexer.Connect(options, new MemoryDBLoggingTextWriter());

                multiPlexer.ServerMaintenanceEvent += (sender, ev) => logger.Warn("ServerMaintenanceEvent: " + ev.RawMessage);
                multiPlexer.ErrorMessage += (sender, ev) => logger.Warn("ErrorMessage: " + ev.Message + " " + ev.EndPoint);
                multiPlexer.InternalError += (sender, ev) => logger.Warn("InternalError: " + ev.Origin + " " + ev.EndPoint, ev.Exception);
                multiPlexer.ConnectionFailed += (sender, ev) => logger.Warn("ConnectionFailed: " + ev.FailureType + " " + ev.EndPoint, ev.Exception);
                multiPlexer.ConnectionRestored += (sender, ev) => logger.Info("ConnectionRestored: " + ev.FailureType + " " + ev.EndPoint, ev.Exception);
                multiPlexer.ConfigurationChanged += (sender, ev) => logger.Info("ConfigurationChanged: " + ev.EndPoint);

                // get endpoints
                var endpoints = multiPlexer.GetEndPoints();
                // if there is only 1 endpoint, then it failed to resolve the other nodes...
                if (endpoints.Length == 1)
                {
                    throw new Exception("Unable to resolve the nodes for the MemoryDB cluster");
                }

                // do a ping to make sure we are actually connected
                multiPlexer.GetDatabase().Ping();

                return multiPlexer;
            }
            catch (Exception e)
            {
                logger.Error(e);
                throw e;
            }
        }

        private ConfigurationOptions GetConnectionOptions()
        {
            return new ConfigurationOptions
            {
                EndPoints = {
                    { MemoryDBBaseSettings.Host, MemoryDBBaseSettings.Port }
                },
                ClientName = Environment.MachineName,

                Ssl = true,
                SslProtocols = SslProtocols.Tls12,

                User = this.secretAccessor.resolve(MemoryDBBaseSettings.UsernameSecretName),
                Password = this.secretAccessor.resolve(MemoryDBBaseSettings.PasswordSecretName),

                ReconnectRetryPolicy = new LinearRetry(MemoryDBBaseSettings.ConnectionRetryDelayInMs),
                AbortOnConnectFail = false,
                ConnectTimeout = MemoryDBBaseSettings.TimeoutInMs,
                AsyncTimeout = MemoryDBBaseSettings.TimeoutInMs,
                SyncTimeout = MemoryDBBaseSettings.TimeoutInMs,
                ConfigCheckSeconds = MemoryDBBaseSettings.ConfigRefreshInSecs,
                HighPrioritySocketThreads = true
            };
        }

        public override void AddOpenProjectHistory(string authId, string projectName, string moduleName, bool copyPerformed)
        { }

        public override long CheckProviderHealth()
        {
            Func<Context, long> requestFtn = context =>
            {
                var timespan = this.GetInstance().GetDatabase().Ping();
                return (long)timespan.TotalMilliseconds;
            };

            // make request to RLS with retries
            return LONG_RETURN_RETRY_POLICY.Execute(requestFtn, new Dictionary<string, object>
            { });
        }

        public override void ClearFile(string authId, string moduleName, string projectName)
        {
            Func<Context, long> requestFtn = context =>
            {
                var timer = new Stopwatch();
                timer.Start();

                var transaction = this.GetInstance().GetDatabase().CreateTransaction();

                var docKey = context["docKey"] as string;
                var lengthKey = context["lengthKey"] as string;
                var hashKey = context["hashKey"] as string;
                var modifiedKey = context["modifiedKey"] as string;

                transaction.KeyDeleteAsync(docKey);
                transaction.KeyDeleteAsync(modifiedKey);
                transaction.KeyDeleteAsync(hashKey);
                transaction.KeyDeleteAsync(lengthKey);

                bool result = transaction.Execute();
                if (!result)
                {
                    throw new MemoryDBTransactionFailureException(
                        string.Format("Message=\"Unable to complete ClearFile transaction\" DocKey={0} HashKey={1} ModifiedKey={2} LengthKey={3}",
                                    docKey, hashKey, modifiedKey, lengthKey));
                }

                timer.Stop();
                clearAverageLogger.Add(timer.ElapsedMilliseconds);

                return 0L;
            };

            // make request to RLS with retries
            LONG_RETURN_RETRY_POLICY.Execute(requestFtn, new Dictionary<string, object>
            {
                { "docKey", GetDocumentKey(authId, moduleName, projectName) },
                { "lengthKey", GetLengthGroupHashKey(authId, moduleName, projectName) },
                { "hashKey", GetHashGroupHashKey(authId, moduleName, projectName) },
                { "modifiedKey", GetModifiedGroupSetKey(authId, moduleName, projectName) },
            });
        }

        public override bool ClearModifiedGroupEntitiesList(string authId, string moduleName, string projectName)
        {
            Func<Context, bool> requestFtn = context =>
            {
                var timer = new Stopwatch();
                timer.Start();

                var modifiedList = context["modifiedList"] as string;
                var response = this.GetInstance().GetDatabase().KeyDelete(modifiedList);

                timer.Stop();
                clearModifiedAverageLogger.Add(timer.ElapsedMilliseconds);

                return response;
            };

            return BOOL_RETURN_RETRY_POLICY.Execute(requestFtn, new Dictionary<string, object>
            {
                { "modifiedList", GetModifiedGroupSetKey(authId, moduleName, projectName) }
            });
        }

        public override void CreateOrOpenFile(string authId, string projectName, string moduleName, bool allowSyncWithReadOnlyDatabase = true)
        { }

        public override void DeleteFile(string authId, string moduleName, string projectName)
        {
            ClearFile(authId, moduleName, projectName);
        }

        public override bool Exists(string authId, string moduleName, string projectName)
        {
            Func<Context, bool> requestFtn = context =>
            {
                var timer = new Stopwatch();
                timer.Start();

                var documentKey = context["documentKey"] as string;
                var response = this.GetInstance().GetDatabase().KeyExists(documentKey);

                timer.Stop();
                existsAverageLogger.Add(timer.ElapsedMilliseconds);

                return response;
            };

            return BOOL_RETURN_RETRY_POLICY.Execute(requestFtn, new Dictionary<string, object>
            {
                { "documentKey", GetDocumentKey(authId, moduleName, projectName) }
            });
        }

        public override bool GetLastOpenProjectDate(string authId, string projectName, string moduleName, out SqlDateTime dateTime)
        {
            dateTime = DateTime.Now;
            return false;
        }

        public override ModifiedGroupEntitiesState GetModifiedGroupEntitiesState(string authId, string moduleName, string projectName)
        {
            var debug = MemoryDBBaseSettings.LuaDebug;
            var lengthKey = GetLengthGroupHashKey(authId, moduleName, projectName);
            var hashKey = GetHashGroupHashKey(authId, moduleName, projectName);

            Func<Context, RedisResult> requestFtn = context =>
            {
                var timer = new Stopwatch();
                timer.Start();

                var script = context["script"] as dynamic;
                var response = this.GetInstance().GetDatabase().ScriptEvaluate(script.script, script.keys, script.values);

                timer.Stop();
                getModifiedAverageLogger.Add(timer.ElapsedMilliseconds);

                return response;
            };

            var scriptResult = REDIS_RESULT_RETURN_RETRY_POLICY.Execute(requestFtn, new Dictionary<string, object>
            {
                { "script", GetWriteModifiedAttributesDataScript(authId, moduleName, projectName, lengthKey, hashKey, debug) }
            });

            if (scriptResult.IsNull
                || (scriptResult.Type != ResultType.SimpleString && scriptResult.Type != ResultType.BulkString)
                || (!scriptResult.ToString().StartsWith("{"))
                || (!scriptResult.ToString().EndsWith("}"))
                )
            {
                throw new MemoryDBLuaScriptFailureException("Error during REDIS script -- " + scriptResult.ToString());
            }

            JObject json = JObject.Parse(scriptResult.ToString());

            if (debug && logger.IsDebugEnabled && json["debug"] != null)
            {
                var debugLogs = json["debug"].ToString();
                logger.Debug(debugLogs);
            }

            var groups = json["modifiedGroups"];
            if (groups == null)
            {
                return new MemoryDBModifiedGroupEntitiesState();
            }

            var groupsData = new List<string>();
            var groupSizes = new Dictionary<string, int>();
            var groupHashes = new Dictionary<string, string>();
            foreach (var jsonNode in json["modifiedGroups"].ToList())
            {
                var groupId = jsonNode["id"].Value<string>();

                var groupData = jsonNode["group"].Value<string>();
                groupsData.Add(groupData);

                var groupSize = jsonNode["length"]?.Value<int>();
                if (groupSize.HasValue)
                {
                    groupSizes.Add(groupId, groupSize.Value);
                }

                var groupHash = jsonNode["hash"]?.Value<string>();
                if (groupHash != null)
                {
                    groupHashes.Add(groupId, groupHash);
                }
            }

            var modifiedGroupEntitiesList = DeserializeGroups(groupsData);
            var trulyModifiedGroupEntitiesList = new List<GroupEntity>();

            // get the list of associated group sizes.
            // if the size is different, we know it's truly modified
            // if the size is the same, we can't be sure, and therefore we will hash it
            // and compare to the hash
            foreach (var groupEntity in modifiedGroupEntitiesList)
            {
                // get the current length+hash - we'll need them regardless since we have to store them
                var currentGroupLength = groupEntity.GroupDataLength;
                var currentGroupHash = groupEntity.GroupDataHash(currentGroupLength);

                // parse the 2 attributes out, since we'll use them twice
                var xmlGroupName = groupEntity.GroupData.Attribute(XmlGroupConstants.XML_GROUP_NAME).Value;
                var xmlGroupInstance = groupEntity.GroupData.Attribute(XmlGroupConstants.XML_GROUP_INSTANCE).Value;

                // build 2 keys to store
                var groupEntityIdentifier = BuildDocumentGroupEntityIdentifier(xmlGroupName, xmlGroupInstance);

                // if the entity is actually modified, then add it to the list, and update the length / hashes for future checks
                if (IsEntityModified(currentGroupLength, currentGroupHash, groupEntityIdentifier, groupSizes, groupHashes))
                {
                    trulyModifiedGroupEntitiesList.Add(groupEntity);
                    groupSizes[groupEntityIdentifier] = currentGroupLength;
                    groupHashes[groupEntityIdentifier] = currentGroupHash;
                }
            }

            return new MemoryDBModifiedGroupEntitiesState(trulyModifiedGroupEntitiesList, lengthKey, groupSizes, hashKey, groupHashes);
        }

        public override IList<string> ListFiles(string authId, string moduleName, bool skipDeleted)
        {
            return null;
        }

        public override IEnumerable<GroupEntity> ReadBatchData(string authId, string moduleName, string projectName, XElement requestData)
        {
            var groupIds = new List<RedisValue>();
            var groups = from g in requestData.Descendants(XmlGroupConstants.XML_GROUP) select g;
            // for each group
            foreach (var group in groups)
            {
                // grab the group name and instance from the attributes
                var groupName = group.Attribute(XmlGroupConstants.XML_GROUP_NAME).Value;
                var instance = group.Attribute(XmlGroupConstants.XML_GROUP_INSTANCE).Value;

                // add to the list of group keys
                groupIds.Add(BuildDocumentGroupEntityIdentifier(groupName, instance));
            }

            var documentKey = GetDocumentKey(authId, moduleName, projectName);
            return GetGroups(documentKey, groupIds);
        }

        public override GroupEntity ReadData(string authId, string moduleName, string projectName, string groupName, string instance)
        {
            var groupTree = new XElement(XmlGroupConstants.XML_GROUPS);

            var groupXElement = new XElement(XmlGroupConstants.XML_GROUP);
            groupXElement.SetAttributeValue(XmlGroupConstants.XML_GROUP_NAME, groupName);
            groupXElement.SetAttributeValue(XmlGroupConstants.XML_GROUP_INSTANCE, instance);

            groupTree.Add(groupXElement);

            return ReadBatchData(authId, moduleName, projectName, groupTree).SingleOrDefault();
        }

        public override IEnumerable<GroupEntity> ReadFile(string authId, string projectName, string moduleName, bool forceReadFromDatabase)
        {
            Func<Context, RedisValue[]> requestFtn = context =>
            {
                var timer = new Stopwatch();
                timer.Start();

                var documentKey = context["documentKey"] as string;
                var response = this.GetInstance().GetDatabase().HashValues(documentKey);

                timer.Stop();
                fileAverageLogger.Add(timer.ElapsedMilliseconds);

                return response;
            };

            var results = REDIS_VALUES_RETURN_RETRY_POLICY.Execute(requestFtn, new Dictionary<string, object>
            {
                { "documentKey", GetDocumentKey(authId, moduleName, projectName) }
            });

            // Convert from base64, and decrypt all the desired groups
            var encryptedGroups = results.Select(encodedEntity => Convert.FromBase64String(encodedEntity.ToString()));
            var encodedGroups = iEncryptor.decryptAll(MemoryDBEncryptionUtil.getKeyName(), encryptedGroups.ToList());

            // finally, deserialize the data back into GroupEntities
            return encodedGroups.Select(encodedGroup => {
                return ZeroFormatterSerializer.Deserialize<GroupEntity>(encodedGroup);
            });
        }

        public override void ReEncryptProject(string authId, string moduleName, string projectName)
        { }

        public override void Reset(ModifiedGroupEntitiesState state)
        {
            Func<Context, long> requestFtn = context =>
            {
                var timer = new Stopwatch();
                timer.Start();

                var modifiedState = context["modifiedState"] as MemoryDBModifiedGroupEntitiesState;

                var transaction = this.GetInstance().GetDatabase().CreateTransaction();

                var hashResult = transaction.HashSetAsync(modifiedState.GetHashDocumentKey(),
                    modifiedState.GetHashDocument()
                    .Select(pair => new HashEntry(pair.Key, pair.Value))
                    .ToArray());

                var lengthResult = transaction.HashSetAsync(modifiedState.GetLengthDocumentKey(),
                    modifiedState.GetLengthDocument()
                    .Select(pair => new HashEntry(pair.Key, pair.Value))
                    .ToArray());

                if (dataCacheEnableExpiryCheck)
                {
                    transaction.KeyExpireAsync(modifiedState.GetHashDocumentKey(), TimeSpan.FromSeconds(dataCacheItemExpirySeconds));
                    transaction.KeyExpireAsync(modifiedState.GetLengthDocumentKey(), TimeSpan.FromSeconds(dataCacheItemExpirySeconds));
                }

                bool result = transaction.Execute();
                if (!result)
                {
                    throw new MemoryDBTransactionFailureException(
                        string.Format("Message=\"Unable to complete Reset transaction\""));
                }

                timer.Stop();
                updateModifiedAverageLogger.Add(timer.ElapsedMilliseconds);

                return 0L;
            };

            LONG_RETURN_RETRY_POLICY.Execute(requestFtn, new Dictionary<string, object>
            {
                { "modifiedState", state as MemoryDBModifiedGroupEntitiesState }
            });
        }

        public override void SyncUser(string authId, string projectName, string moduleName, ref bool copyRequested, string callerIdentifier = null)
        {
            copyRequested = false;
        }

        public override long WriteData(string authId, string moduleName, string projectName, GroupEntity groupEntity, bool forceExistingDataSync = false)
        {
            WriteData(authId, moduleName, projectName, new List<GroupEntity> { groupEntity }, forceExistingDataSync);
            return 0L;
        }

        public override void WriteData(string authId, string moduleName, string projectName, IEnumerable<GroupEntity> groupEntities, bool forceExistingDataSync = false, bool allGroups = false)
        {
            // shortcut if empty
            if (!groupEntities.Any())
                return;

            var debug = MemoryDBBaseSettings.LuaDebug;

            Func<Context, RedisResult> requestFtn = context =>
            {
                var timer = new Stopwatch();
                timer.Start();

                var script = context["script"] as dynamic;
                var response = this.GetInstance().GetDatabase().ScriptEvaluate(script.script, script.keys, script.values);

                timer.Stop();
                writeGroupsAverageLogger.Add(timer.ElapsedMilliseconds);

                return response;
            };

            var scriptResult = REDIS_RESULT_RETURN_RETRY_POLICY.Execute(requestFtn, new Dictionary<string, object>
            {
                { "script", GetWriteDataScript(authId, moduleName, projectName, groupEntities, forceExistingDataSync, debug) }
            });

            if (scriptResult.IsNull
                || (scriptResult.Type != ResultType.SimpleString && scriptResult.Type != ResultType.BulkString)
                || (!debug && scriptResult.ToString() != "OK")
                || (debug && !scriptResult.ToString().Contains("\"status\":\"OK\""))
                )
            {
                throw new MemoryDBLuaScriptFailureException("Error during REDIS script -- " + scriptResult.ToString());
            }

            if (debug && logger.IsDebugEnabled)
            {
                logger.Debug(scriptResult.ToString());
            }
        }

        private dynamic GetWriteDataScript(string authId, string moduleName, string projectName, IEnumerable<GroupEntity> groupEntities, bool forceExistingDataSync, bool debug)
        {
            // keys: doc, modified
            var keys = new List<RedisKey>();
            keys.Add(GetDocumentKey(authId, moduleName, projectName));
            keys.Add(GetModifiedGroupSetKey(authId, moduleName, projectName));

            // argvs: expiry in seconds (or -1), forceExistingDataSync, debug,
            //        groupId_1, groupData_1, ...groupId_n, groupData_n
            var values = new List<RedisValue>();
            values.Add(dataCacheEnableExpiryCheck ? dataCacheItemExpirySeconds : -1);
            values.Add(forceExistingDataSync.ToString());
            values.Add(debug.ToString());

            var sortedList = new SortedList<string, byte[]>();
            // go through each entity that needs to be written to couchbase
            foreach (var groupEntityToStore in groupEntities)
            {
                var serializedEntity = ZeroFormatterSerializer.Serialize(groupEntityToStore);
                var identifer = BuildDocumentGroupEntityIdentifier(groupEntityToStore.Name, groupEntityToStore.Client); ;
                sortedList.Add(identifer, serializedEntity);
            }

            // encrypt all the group data values to be updated
            var encryptedValues = iEncryptor.encryptAll(MemoryDBEncryptionUtil.getKeyName(), MemoryDBEncryptionUtil.getSeed(), sortedList.Values.ToList());

            // update the full document with the newly encoded and encrypted group data
            var groupKeys = sortedList.Keys;
            for (var i = 0; i < sortedList.Count; i++)
            {
                var key = groupKeys[i];
                values.Add(key);
                values.Add(Convert.ToBase64String(encryptedValues[i]));
            }

            return new
            {
                script = WRITE_GROUPS_LUA_SCRIPT,
                keys = keys.ToArray(),
                values = values.ToArray()
            };
        }

        private dynamic GetWriteModifiedAttributesDataScript(string authId, string moduleName, string projectName, string lengthKey, string hashKey, bool debug)
        {
            // keys: doc, size, hash, modified
            var keys = new List<RedisKey>();
            keys.Add(GetDocumentKey(authId, moduleName, projectName));
            keys.Add(lengthKey);
            keys.Add(hashKey);
            keys.Add(GetModifiedGroupSetKey(authId, moduleName, projectName));

            // argvs: debug
            var values = new List<RedisValue>();
            values.Add(debug.ToString());

            return new
            {
                script = MODIFIED_GROUPS_GET_LUA_SCRIPT,
                keys = keys.ToArray(),
                values = values.ToArray()
            };
        }

        public override bool IsCachingEnabled()
        {
            return true;
        }

        public override bool SupportsCaching()
        {
            return true;
        }

        #region Helpers

        /// <summary>
        /// Get the Key of the document that holds all the GroupEntities
        /// </summary>
        /// <param name="authId"></param>
        /// <param name="moduleName"></param>
        /// <param name="fileName"></param>
        /// <returns></returns>
        private static string GetDocumentKey(string authId, string moduleName, string fileName)
        {
            return string.Format("{0}|doc", GetKeyBase(authId, moduleName, fileName));
        }

        /// <summary>
        /// Get the Key of the document that holds all the GroupEntities
        /// </summary>
        /// <param name="authId"></param>
        /// <param name="moduleName"></param>
        /// <param name="fileName"></param>
        /// <returns></returns>
        private static string GetKeyBase(string authId, string moduleName, string fileName)
        {
            return string.Format("tcs::{{{0}|{1}|{2}}}", authId, moduleName, fileName);
        }

        /// <summary>
        /// Build a GroupEntityKey
        /// </summary>
        /// <param name="groupName"></param>
        /// <param name="instance"></param>
        /// <returns></returns>
        private static string BuildDocumentGroupEntityIdentifier(string groupName, string instance)
        {
            return string.Format("{0}|{1}", groupName, instance);
        }

        /// <summary>
        /// Build a GroupEntitySizeKey
        /// </summary>
        /// <param name="groupName"></param>
        /// <param name="instance"></param>
        /// <returns></returns>
        private static string GetLengthGroupHashKey(string authId, string moduleName, string fileName)
        {
            return string.Format("{0}|size", GetKeyBase(authId, moduleName, fileName));
        }

        /// <summary>
        /// Build a GroupEntityHashKey
        /// </summary>
        /// <param name="authId"></param>
        /// <param name="moduleName"></param>
        /// <param name="fileName"></param>
        /// <param name="groupName"></param>
        /// <param name="instance"></param>
        /// <returns></returns>
        private static string GetHashGroupHashKey(string authId, string moduleName, string fileName)
        {
            return string.Format("{0}|hash", GetKeyBase(authId, moduleName, fileName));
        }

        private IEnumerable<GroupEntity> GetGroups(string documentKey, List<RedisValue> groupIdentifiers)
        {
            Func<Context, RedisValue[]> requestFtn = context =>
            {
                var timer = new Stopwatch();
                timer.Start();

                var key = context["documentKey"] as string;
                var ids = context["groupIdentifiers"] as RedisValue[];
                var response = this.GetInstance().GetDatabase().HashGet(key, ids);

                timer.Stop();
                getGroupsAverageLogger.Add(timer.ElapsedMilliseconds);

                return response;
            };

            var results = REDIS_VALUES_RETURN_RETRY_POLICY.Execute(requestFtn, new Dictionary<string, object>
            {
                { "documentKey", documentKey },
                { "groupIdentifiers", groupIdentifiers.ToArray() }
            });

            var groupDataList = results
                                .Where(result => result.HasValue)
                                .Select(result => result.ToString())
                                .ToList();

            return DeserializeGroups(groupDataList);
        }

        private IEnumerable<GroupEntity> DeserializeGroups(List<string> groupDataList)
        {
            // Convert from base64, and decrypt all the desired groups
            var encryptedGroups = groupDataList.Select(encodedEntity => Convert.FromBase64String(encodedEntity));
            var encodedGroups = iEncryptor.decryptAll(MemoryDBEncryptionUtil.getKeyName(), encryptedGroups.ToList());

            // finally, deserialize the data back into GroupEntities
            return encodedGroups.Select(encodedGroup => {
                return ZeroFormatterSerializer.Deserialize<GroupEntity>(encodedGroup);
            });
        }

        /// <summary>
        /// Get Key of the File that hold the list of the Modified GroupEntities
        /// </summary>
        /// <param name="authId"></param>
        /// <param name="moduleName"></param>
        /// <param name="fileName"></param>
        /// <returns></returns>
        private static string GetModifiedGroupSetKey(string authId, string moduleName, string fileName)
        {
            return string.Format("{0}|modified", GetKeyBase(authId, moduleName, fileName));
        }

        /// <summary>
        /// Check if the provided entity has really been modified compared to what's in couchbase.
        /// </summary>
        /// <param name="authId"></param>
        /// <param name="moduleName"></param>
        /// <param name="projectName"></param>
        /// <param name="trulyModifiedGroupEntitiesList"></param>
        /// <param name="groupPayloadLengths"></param>
        /// <param name="groupPayloadHashes"></param>
        private bool IsEntityModified(int currentGroupLength, string currentGroupHash,
                            string groupEntityIdentifier,
                            IDictionary<string, int> groupPayloadLengths, IDictionary<string, string> groupPayloadHashes)
        {
            // get stored length, since this is the first indicator of group data change
            var NOT_FOUND_LENGTH = -1;
            var storedGroupLength = groupPayloadLengths.ContainsKey(groupEntityIdentifier) ? groupPayloadLengths[groupEntityIdentifier] : NOT_FOUND_LENGTH;
            var storedGroupHash = groupPayloadHashes.ContainsKey(groupEntityIdentifier) ? groupPayloadHashes[groupEntityIdentifier] : null;

            return
                // if we don't have a stored group data's length or hash, then we must consider the group as changed
                (storedGroupLength == NOT_FOUND_LENGTH || string.IsNullOrEmpty(storedGroupHash))
                // if the group data size is different, then the group has changed
                || (storedGroupLength != currentGroupLength)
                // if hash is different, the data is different
                || (storedGroupHash != currentGroupHash);
        }

        #endregion

        #region

        private static RetryPolicy<T> CreateRetryPolicy<T>()
        {
            return Policy
                 .Handle<RedisTimeoutException>()
                 .Or<RedisConnectionException>()
                 .OrResult<T>(obj => false)
                 .WaitAndRetry(MemoryDBBaseSettings.RetryConnectionFailuresDelaysInMs, (result, timespan, retry, context) =>
                 {
                     logger.Warn("Connection exception... Retry=" + retry + " Result=" + result.Result, result.Exception);
                 });
        }

        private static string GetWriteModifiedAttributesDataLuaScript()
        {
            return GetEmbeddedLuaScript("GetModifiedGroups.lua");
        }

        private static string GetWriteGroupsLuaScript()
        {
            return GetEmbeddedLuaScript("WriteGroupsScript.lua");
        }

        private static string GetEmbeddedLuaScript(string name)
        {
            var assembly = Assembly.GetExecutingAssembly();
            var resourceName = "MemoryDBDataProvider." + name;

            using (Stream stream = assembly.GetManifestResourceStream(resourceName))
            using (StreamReader reader = new StreamReader(stream))
            {
                return reader.ReadToEnd();
            }
        }

        #endregion
    }
}
