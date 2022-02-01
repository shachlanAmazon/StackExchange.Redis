using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Authentication;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace TestConsole
{
    public class IntRange
    {
        public int start;
        public int length;
        public IntRange (int start, int length)
        {
            this.start = start;
            this.length = length;
        }
        public bool inRange(int value)
        {
            return value >= start && value < end();
        }

        public int end()
        {
            return start + length;
        }
    }

    internal static class Program
    {

        static readonly string writeScript = @"-- setup variables from start of ARGV
local logtable = { }
local expirySeconds = tonumber(ARGV[1])
local isExpiryEnabled = expirySeconds ~= -1
local forcedDataSync = ARGV[2] == ""True""
local isDebug = ARGV[3] == ""True""
logtable['expirySeconds'] = expirySeconds
logtable['isExpiryEnabled'] = isExpiryEnabled
logtable['forcedDataSync'] = forcedDataSync
logtable['forcedDataSyncSkip'] = -1


local groupsUpdateArray = { }
local modifiedIds = { }
--the rest of ARGV are the data group to be written; convert to a name-value table
for i = 4, #ARGV, 2 do
    local field = ARGV[i]
    local data = ARGV[i + 1]
    repeat
        -- if forced data sync, then don't add new entries
        if forcedDataSync then
            if redis.call('HEXISTS', KEYS[1], field) == 0 then
                logtable['forcedDataSyncSkip'] = logtable['forcedDataSyncSkip'] + 1
                break
            end
        end
        table.insert(groupsUpdateArray, field);
            table.insert(groupsUpdateArray, data);
            table.insert(modifiedIds, field);

            until true
end
local updateResults = { }
            --add to the hashmap
           updateResults[KEYS[1]] = redis.call('HSET', KEYS[1], unpack(groupsUpdateArray))
           -- add to modified set
updateResults[KEYS[2]] = redis.call('SADD', KEYS[2], unpack(modifiedIds))
logtable['updateResults'] = updateResults
if (isExpiryEnabled) then
     logtable[KEYS[1]..'.EXPIRE'] = redis.call('EXPIRE', KEYS[1], expirySeconds)
    logtable[KEYS[2]..'.EXPIRE'] = redis.call('EXPIRE', KEYS[2], expirySeconds)
end
if (isDebug) then
     logtable['status'] = 'OK'
    return cjson.encode(logtable)
end
return 'OK'";

        static readonly string readScript = @"-- setup variables from start of ARGV
local logtable = {}
local isDebug = ARGV[1] == ""True""
local groupResultData = { }


local modifiedGroupNames = redis.call('SMEMBERS', KEYS[4])
if(#modifiedGroupNames == 0) then
    return cjson.encode(groupResultData)
end
local groupHashesArray = redis.call('HGETALL', KEYS[3])
local groupHashes = { }
for i = 1, #groupHashesArray, 2 do
    groupHashes[groupHashesArray[i]] = groupHashesArray[i + 1]
end
local groupLengthsArray = redis.call('HGETALL', KEYS[2])
local groupLengths = { }
for i = 1, #groupLengthsArray, 2 do
    groupLengths[groupLengthsArray[i]] = groupLengthsArray[i + 1]
end
local groups = redis.call('HMGET', KEYS[1], unpack(modifiedGroupNames))
local groupsArray = { }
-- the rest of ARGV are the data group to be written; convert to a name-value table
for i=1, #modifiedGroupNames, 1 do
    local groupId = modifiedGroupNames[i]
    local groupLength = groupLengths[groupId]
    local groupHash = groupHashes[groupId]
    local groupData = groups[i]
    groupsArray[i] = { length = groupLength, hash = groupHash, group = groupData, id=groupId
    }
    end
    groupResultData['modifiedGroups'] = groupsArray
if(isDebug) then
    groupResultData['debug'] = logtable
end
return cjson.encode(groupResultData)";


        static readonly string valueHeader = String.Join("", Enumerable.Range(0, 200).Select(index => (index % 10).ToString()).ToArray());
        static readonly string[] firstValues = new string[] { "100", "False", "True" };
        static readonly RedisKey[] writeKeys = new[] { "{node}/firstKey", "'{node}/secondKey" }.Select(key => new RedisKey(key)).ToArray();
        static readonly RedisKey[] readKeys = new[] { "{node}/a", "'{node}/b", "'{node}/c", "'{node}/secondKey", "'{node}/firstKey" }.Select(key => new RedisKey(key)).ToArray();
        static readonly Random random = new Random();  

        private static RedisKey[] getThreadKeys(int threadIndex)
        {
            return new[] { "{node}/firstKey" + threadIndex, "'{node}/secondKey" + threadIndex }.Select(key => new RedisKey(key)).ToArray();
        }

        private static RedisResult runWriteScript(IDatabase db, int threadIndex)
        {
            var numberOfExtraArguments = 3100;
            var args = Enumerable.Concat<string>(firstValues, Enumerable.Range(0, numberOfExtraArguments).Select(index => $"{valueHeader}-{index}-{threadIndex}"))
                .Select(arg => new RedisValue(arg))
                .ToArray();
            try
            {
                return db.ScriptEvaluate(writeScript, getThreadKeys(threadIndex), args);
            }catch (Exception)
            {
                Console.WriteLine($"failed writing {threadIndex}");
                throw;
            }
        }

        private static RedisResult runReadScript(IDatabase db, int threadIndex)
        {
            try { 
                var args = new RedisValue[] { new RedisValue("True") };
                return db.ScriptEvaluate(readScript, readKeys, args);
            }
            catch (Exception)
            {
                throw;
            }
        }

        private static ConfigurationOptions options(string server)
        {
            var timeout = 5000;
            var options = ConfigurationOptions.Parse(server);

            options.ReconnectRetryPolicy = new LinearRetry(500);
            options.AbortOnConnectFail = false;
            options.ConfigCheckSeconds = 120;
            options.HighPrioritySocketThreads = true;
            options.Ssl = true;
            options.SslProtocols = SslProtocols.Tls12;

            options.AsyncTimeout = timeout;
            options.ConnectTimeout = timeout;
            options.SyncTimeout = timeout;
            return options;
        }

        private static void massiveCalls(ConfigurationOptions options)
        {
            IntRange writeRange = new IntRange(0, 2);
            IntRange simpleWriteRange = new IntRange(writeRange.end(), 3);
            IntRange readRange = new IntRange(simpleWriteRange.end(), 2);
            IntRange keyExpireTransaction = new IntRange(readRange.end(), 1);
            IntRange keyDeleteTransaction = new IntRange(keyExpireTransaction.end(), 1);

            List<Thread> list = new List<Thread>();
            for (int i = 0; i < 30; i++)
            {
                var j = i;
                var t = new Thread(() => {
                    var writeFile = new StreamWriter($"c:\\textwriter{j}.txt");
                    var conn = ConnectionMultiplexer.Connect(options, writeFile);
                    var db = conn.GetDatabase();
                    try
                    {
                        for (var counter = 0; counter < 1000; counter++)
                        {
                            var randomCoinToss = random.Next(keyDeleteTransaction.end());
                            if (writeRange.inRange(randomCoinToss))
                            {
                                if (counter % 10 == 0)
                                {
                                    Console.WriteLine($"write script {j} - {counter}");
                                }
                                runWriteScript(db, j);
                            }
                            else if (simpleWriteRange.inRange(randomCoinToss))
                            {
                                if (counter % 10 == 0)
                                {
                                    Console.WriteLine($"set add {j} - {counter}");
                                }
                                db.SetAdd(writeKeys[0], new RedisValue(j.ToString()));
                                db.SetAdd(writeKeys[1], new RedisValue(j.ToString()));
                            }
                            else if (readRange.inRange(randomCoinToss))
                            {
                                if (counter % 10 == 0)
                                {
                                    Console.WriteLine($"read script {j} - {counter}");
                                }
                                runReadScript(db, j);
                            }
                            else if (keyExpireTransaction.inRange(randomCoinToss))
                            {
                                if (counter % 10 == 0)
                                {
                                    Console.WriteLine($"key expire {j} - {counter}");
                                }
                                performKeyExpireTransaction(db, j);
                            }
                            else
                            {
                                if (counter % 10 == 0)
                                {
                                    Console.WriteLine($"key delete {j} - {counter}");
                                }
                                performKeyDeleteTransaction(db, j);
                            }
                        }
                    }
                    finally
                    {
                        Console.WriteLine("Closing " + j);
                        conn.Close();
                        writeFile.Flush();
                        writeFile.Close();

                    }
                });
                list.Add(t);
                t.Start();
            }
            foreach (var t in list)
            {
                t.Join();
            }
        }

        private static void performKeyDeleteTransaction(IDatabase db, int j)
        {
            var transaction = db.CreateTransaction();

            transaction.KeyDeleteAsync(writeKeys[0]);
            transaction.KeyDeleteAsync(writeKeys[1]);

            bool result = transaction.Execute();
        }
        private static void performKeyExpireTransaction(IDatabase db, int j)
        {
            var transaction = db.CreateTransaction();

            transaction.KeyExpireAsync(writeKeys[0], TimeSpan.FromSeconds(10));
            transaction.KeyExpireAsync(writeKeys[1], TimeSpan.FromSeconds(10));

            bool result = transaction.Execute();
        }

        public static void Main()
        {
#pragma warning disable CS0219 // Variable is assigned but its value is never used
            var redisServer = "clustercfg.shachlan-se-test-devo.sstwrm.use1devo.elmo-dev.amazonaws.com:6379";
            var memorydbServer = "clustercfg.stack-exchange-test-memorydb.sstwrm.memorydb-devo.us-east-1.amazonaws.com:6379";
#pragma warning restore CS0219 // Variable is assigned but its value is never used
            var client = ConnectionMultiplexer.Connect(options(redisServer));
            //var client = ConnectionMultiplexer.Connect("stack-exchange-test-no-tls.4l6gyg.clustercfg.memorydb.eu-west-1.amazonaws.com");
            client.GetDatabase().Ping();
            var db = client.GetDatabase();
            db.StringSet("Ahoy", "Matey");
            Console.WriteLine(db.StringGet("Ahoy"));

            var scriptResult = runWriteScript(db, 0);
            Console.WriteLine("Result: " + scriptResult.ToString());

            massiveCalls(options(memorydbServer));
        }
    }
}
