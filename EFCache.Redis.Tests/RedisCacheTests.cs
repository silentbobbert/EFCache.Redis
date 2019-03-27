using System;
using System.Threading;
using EFCache.Redis.Tests.Annotations;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using StackExchange.Redis;

namespace EFCache.Redis.Tests
{
    [Serializable]
    public class TestObject
    {
        public string Message { get; set; }
    }

    
    [TestClass]
    [UsedImplicitly]
    public class RedisCacheTests
    {
        private readonly string RegularConnectionString = "localhost:6379";
        private readonly string AdminConnectionString = "localhost:6379,allowAdmin=true";
        public RedisCacheTests()
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
                AdminConnectionString = string.Join(",", connString, "allowAdmin=true");

            }
            catch (Exception)
            {
                // Could not connect to redis above, so start a local copy
                RedisStorageEmulatorManager.Instance.StartProcess(false);
            }
           
        }

        [TestMethod]
        public void Item_cached()
        {
            var cache = new RedisCache(RegularConnectionString);
            var item = new TestObject { Message = "OK" };

            cache.PutItem("key", item, new string[0], TimeSpan.MaxValue, DateTimeOffset.MaxValue);

            Assert.IsTrue(cache.GetItem("key", out var fromCache));
            Assert.AreEqual(item.Message, ((TestObject)fromCache).Message);

            Assert.IsTrue(cache.GetItem("key", out fromCache));
            Assert.AreEqual(item.Message, ((TestObject)fromCache).Message);
        }

        [TestMethod]
        public void Item_not_returned_after_absolute_expiration_expired()
        {
            var cache = new RedisCache(RegularConnectionString);
            var item = new TestObject { Message = "OK" };

            cache.PutItem("key", item, new string[0], TimeSpan.MaxValue, DateTimeOffset.Now.AddMinutes(-10));

            Assert.IsFalse(cache.GetItem("key", out var fromCache));
            Assert.IsNull(fromCache);
        }

        [TestMethod]
        public void Item_not_returned_after_sliding_expiration_expired()
        {
            var cache = new RedisCache(RegularConnectionString);
            var item = new TestObject { Message = "OK" };

            cache.PutItem("key", item, new string[0], TimeSpan.Zero.Subtract(new TimeSpan(10000)), DateTimeOffset.MaxValue);

            Assert.IsFalse(cache.GetItem("key", out var fromCache));
            Assert.IsNull(fromCache);
        }

        [TestMethod]
        public void Item_still_returned_after_sliding_expiration_period()
        {
            var cache = new RedisCache(RegularConnectionString);
            var item = new TestObject { Message = "OK" };

            // Cache the item with a sliding expiration of 10 seconds
            cache.PutItem("key", item, new string[0], TimeSpan.FromSeconds(10), DateTimeOffset.MaxValue);

            object fromCache = null;
            // In a loop of 20 seconds retrieve the item every 5 second seconds.
            for (var i = 0; i < 4; i++)
            {
                Thread.Sleep(5000); // Wait 5 seconds
                // Retrieve item again. This should update LastAccess and as such keep the item 'alive'
                // Break when item cannot be retrieved
                Assert.IsTrue(cache.GetItem("key", out fromCache));
            }
            Assert.IsNotNull(fromCache);
        }

        [TestMethod]
        public void InvalidateSets_invalidate_items_with_given_sets()
        {
            var cache = new RedisCache(RegularConnectionString);

            cache.PutItem("1", new object(), new[] { "ES1", "ES2" }, TimeSpan.MaxValue, DateTimeOffset.MaxValue);
            cache.PutItem("2", new object(), new[] { "ES2", "ES3" }, TimeSpan.MaxValue, DateTimeOffset.MaxValue);
            cache.PutItem("3", new object(), new[] { "ES1", "ES3", "ES4" }, TimeSpan.MaxValue, DateTimeOffset.MaxValue);
            cache.PutItem("4", new object(), new[] { "ES3", "ES4" }, TimeSpan.MaxValue, DateTimeOffset.MaxValue);

            cache.InvalidateSets(new[] { "ES1", "ES2" });

            Assert.IsFalse(cache.GetItem("1", out _));
            Assert.IsFalse(cache.GetItem("2", out _));
            Assert.IsFalse(cache.GetItem("3", out _));
            Assert.IsTrue(cache.GetItem("4", out _));
        }

        [TestMethod]
        public void InvalidateItem_invalidates_item()
        {
            var cache = new RedisCache(RegularConnectionString);

            cache.PutItem("1", new object(), new[] { "ES1", "ES2" }, TimeSpan.MaxValue, DateTimeOffset.MaxValue);
            cache.InvalidateItem("1");

            Assert.IsFalse(cache.GetItem("1", out _));
        }

        [TestMethod]
        public void Count_returns_numers_of_cached_entries()
        {
            var cache = new RedisCache(AdminConnectionString);

            cache.Purge();

            Assert.AreEqual(0, cache.Count);

            cache.PutItem("1", new object(), new[] { "ES1", "ES2" }, TimeSpan.MaxValue, DateTimeOffset.MaxValue);

            Assert.AreEqual(3, cache.Count); // "1", "ES1", "ES2"

            cache.InvalidateItem("1");

            Assert.AreEqual(0, cache.Count);
        }

        [TestMethod]
        public void Purge_removes_stale_items_from_cache()
        {
            var cache = new RedisCache(AdminConnectionString);

            cache.Purge();

            cache.PutItem("1", new object(), new[] { "ES1", "ES2" }, TimeSpan.MaxValue, DateTimeOffset.Now.AddMinutes(-1));
            cache.PutItem("2", new object(), new[] { "ES1", "ES2" }, TimeSpan.MaxValue, DateTimeOffset.MaxValue);

            Assert.AreEqual(4, cache.Count); // "1", "2", "ES1", "ES2"

            cache.Purge();

            Assert.AreEqual(0, cache.Count);

            Assert.IsFalse(cache.GetItem("1", out _));
            Assert.IsFalse(cache.GetItem("2", out _));
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void GetItem_validates_parameters()
        {
            var unused = new RedisCache(RegularConnectionString).GetItem(null, out _);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void PutItem_validates_key_parameter()
        {
            new RedisCache(RegularConnectionString).PutItem(null, 42, new string[0], TimeSpan.Zero, DateTimeOffset.Now);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void PutItem_validates_dependentEntitySets_parameter()
        {
            new RedisCache(RegularConnectionString).PutItem("1", 42, null, TimeSpan.Zero, DateTimeOffset.Now);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void InvalidateSets_validates_parameters()
        {
            new RedisCache(RegularConnectionString).InvalidateSets(null);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void InvalidateItem_validates_parameters()
        {
            new RedisCache(RegularConnectionString).InvalidateItem(null);
        }

        [TestMethod]
        public void GetItem_does_not_crash_if_cache_is_unavailable()
        {
            var cache = new RedisCache("unknown,abortConnect=false");
            RedisCacheException exception = null;
            cache.CachingFailed += (s, e) => exception = e;

            var success = cache.GetItem("1", out var item);

            Assert.IsFalse(success);
            Assert.IsNull(item);
            Assert.IsNotNull(exception);
            Assert.IsInstanceOfType(exception.InnerException, typeof(RedisConnectionException));
            Assert.AreEqual("Redis | Caching failed for GetItem", exception.Message);
        }

        [TestMethod]
        public void PutItem_does_not_crash_if_cache_is_unavailable()
        {
            var cache = new RedisCache("unknown,abortConnect=false");
            RedisCacheException exception = null;
            cache.CachingFailed += (s, e) => exception = e;

            cache.PutItem("1", new object(), new[] { "ES1", "ES2" }, TimeSpan.MaxValue, DateTimeOffset.MaxValue);

            Assert.IsNotNull(exception);
            Assert.IsInstanceOfType(exception.InnerException, typeof(RedisConnectionException));
        }

        [TestMethod]
        public void InvalidateItem_does_not_crash_if_cache_is_unavailable()
        {
            var cache = new RedisCache("unknown,abortConnect=false");
            RedisCacheException exception = null;
            cache.CachingFailed += (s, e) => exception = e;

            cache.InvalidateItem("1");

            Assert.IsNotNull(exception);
            Assert.IsInstanceOfType(exception.InnerException, typeof(RedisConnectionException));
        }
    }
}
