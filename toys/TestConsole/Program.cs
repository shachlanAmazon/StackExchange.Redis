using System;
using System.Linq;
using System.Threading.Tasks;
using StackExchange.Redis;
using StackExchange.Redis.Interfaces;
//using Amazon;

namespace TestConsole
{
    internal class FakeProvider : ICredentialsProvider
    {
        private Random random = new Random();

        public object Clone() => new FakeProvider();
        public string getPassword() => Guid.NewGuid().ToString();
        public string getUser() => Guid.NewGuid().ToString();
    }

    internal static class Program
    {
        private static string getNewPassword(string server, int port, string user)
        {
            //return Amazon.RDS.Util.RDSAuthTokenGenerator.GenerateAuthToken(RegionEndpoint.USEast1, server, port, user);
            return DateTime.Now.ToString();
        }

        public static async Task Main()
        {
            var user = "this is the username";
            var server = "127.0.0.1";
            var port = 6379;
            var pwd = getNewPassword(server, port, user);
            Console.Write("password is {0}\n", pwd);

            var options = ConfigurationOptions.Parse($"{server}:{port}");
            options.CredentialsProvider = new FakeProvider();
            options.ConfigurationChannel = "";
            options.TieBreaker = "";

            var client = ConnectionMultiplexer.Connect(options);
            Console.Write("created multiplexer");
            client.GetDatabase().Ping();
            var db = client.GetDatabase(0);
            Console.WriteLine("Connected");

            // db.StringSet("foo", "bar");
            var counter = 1;
            var interval = 9000;
            while (true)
            {
                var newPassword = pwd + (++counter);
                await client.ReauthenticateAsync();
                Console.WriteLine("get string: " + await db.StringGetAsync("foo"));
                await db.StringSetAsync("foo", "bar");
                Console.WriteLine("get string: " + await db.StringGetAsync("foo"));
                Console.WriteLine("delaying");
                await Task.Delay(interval);
                Console.WriteLine("done delay");
            }
        }
    }
}
