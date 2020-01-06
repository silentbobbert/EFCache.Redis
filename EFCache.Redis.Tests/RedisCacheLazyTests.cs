using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using StackExchange.Redis;

namespace EFCache.Redis.Tests
{
    [TestClass]
    public class RedisCacheLazyTests
    {

        private static readonly Lazy<ConnectionMultiplexer> LazyConnection = new Lazy<ConnectionMultiplexer>(() => 
            ConnectionMultiplexer.Connect(RegularConnectionString));

        private static string RegularConnectionString;
        public RedisCacheLazyTests()
        {
            try
            {
                // See if we have a running copy of redis in a K8s Cluster
                // helm install --name redis-dev --set password=secretpassword --set master.disableCommands= stable/redis
                // kubectl get secret --namespace default redis-dev -o jsonpath="{.data.redis-password}" | base64 --decode
                // kubectl port-forward --namespace default svc/redis-dev-master 6379:6379
                var connString = "localhost:6379,password=secretpassword";

                var cache = new RedisCache(connString);
                RegularConnectionString = connString;
            }
            catch (Exception)
            {
                // Could not connect to redis above, so start a local copy
                RedisStorageEmulatorManager.Instance.StartProcess(false);
            }

        }

        [TestMethod]
        public void Item_cached_with_lazy()
        {
            var cache = new RedisCache(LazyConnection.Value);
            var item = new TestObject { Message = "OK" };

            cache.PutItem("key", item, new string[0], TimeSpan.MaxValue, DateTimeOffset.MaxValue);

            Assert.IsTrue(cache.GetItem("key", out var fromCache));
            Assert.AreEqual(item.Message, ((TestObject)fromCache).Message);

            fromCache = null;

            Assert.IsTrue(cache.GetItem("key", out fromCache));
            Assert.AreEqual(item.Message, ((TestObject)fromCache).Message);
        }
    }
}
