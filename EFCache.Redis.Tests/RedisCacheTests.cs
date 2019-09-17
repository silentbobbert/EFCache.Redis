using System;
using System.Threading;
using EFCache.Redis.Tests.Annotations;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using StackExchange.Redis;
using System.Linq;
using System.Threading.Tasks;
using System.Diagnostics;

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
        public RedisCacheTests()
        {
            RedisStorageEmulatorManager.Instance.StartProcess(false);
        }

        [TestMethod]
        public void Item_cached()
        {
            var cache = new RedisCache("localhost:6379");
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
            var cache = new RedisCache("localhost:6379");
            var item = new TestObject { Message = "OK" };

            cache.PutItem("key", item, new string[0], TimeSpan.MaxValue, DateTimeOffset.Now.AddMinutes(-10));

            Assert.IsFalse(cache.GetItem("key", out var fromCache));
            Assert.IsNull(fromCache);
        }

        [TestMethod]
        public void Item_not_returned_after_sliding_expiration_expired()
        {
            var cache = new RedisCache("localhost:6379");
            var item = new TestObject { Message = "OK" };

            cache.PutItem("key", item, new string[0], TimeSpan.Zero.Subtract(new TimeSpan(10000)), DateTimeOffset.MaxValue);

            Assert.IsFalse(cache.GetItem("key", out var fromCache));
            Assert.IsNull(fromCache);
        }

        [TestMethod]
        public void Item_still_returned_after_sliding_expiration_period()
        {
            var cache = new RedisCache("localhost:6379");
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
            var cache = new RedisCache("localhost:6379");

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
            var cache = new RedisCache("localhost:6379");

            cache.PutItem("1", new object(), new[] { "ES1", "ES2" }, TimeSpan.MaxValue, DateTimeOffset.MaxValue);
            cache.InvalidateItem("1");

            Assert.IsFalse(cache.GetItem("1", out _));
        }

        [TestMethod]
        public void Count_returns_numers_of_cached_entries()
        {
            var cache = new RedisCache("localhost:6379,allowAdmin=true");

            cache.Purge();

            Assert.AreEqual(0, cache.Count);

            cache.PutItem("1", new object(), new[] { "ES1", "ES2" }, TimeSpan.MaxValue, DateTimeOffset.MaxValue);

            Assert.AreEqual(3, cache.Count); // "1", "ES1", "ES2"

            cache.InvalidateItem("1");

            Assert.AreEqual(0, cache.Count);
        }

        [TestMethod]
        public async Task ThreadingBlockTest()
        {
            var cache = new RedisCache("localhost:6379,allowAdmin=true");

            Exception exception = null;

            cache.LockWaitTimeout = 250;

            cache.CachingFailed += (sender, e) =>
            {
                if (e?.InnerException is LockTimeoutException)
                    exception = e.InnerException;
            };
            cache.Purge();

            Assert.AreEqual(0, cache.Count);

            var crazyLargeResultSet = Enumerable.Range(1, 100000).Select(a => $"String {a}").ToArray();

            cache.PutItem("1", crazyLargeResultSet, new[] { "ES1", "ES2" }, TimeSpan.MaxValue, DateTimeOffset.MaxValue);
            cache.PutItem("2", crazyLargeResultSet, new[] { "ES1", "ES2" }, TimeSpan.MaxValue, DateTimeOffset.MaxValue);
            cache.PutItem("3", crazyLargeResultSet, new[] { "ES1", "ES2" }, TimeSpan.MaxValue, DateTimeOffset.MaxValue);

            //            Assert.Equal(3, cache.Count); // "1", "ES1", "ES2"


            var tasks = new Task[10];

            for (var i = 0; i < 10; i++)
            {
                var icopy = i;
                tasks[i] = Task.Run(() =>
                {
                    var watch = new Stopwatch();
                    watch.Start();
                    Debug.WriteLine($"Invalidate {icopy} start");
                    if (i == 9)
                    cache.InvalidateItem("1");
                    else
                    {
                        object val;
                        cache.GetItem("1", out val);
                    }
                    watch.Stop();
                    Debug.WriteLine($"Invalidate {icopy} complete after {watch.ElapsedMilliseconds}");
                });
            }


            var threadGet = Task.Run(() =>
            {
                Debug.WriteLine($"Get start");
                var watch = new Stopwatch();
                watch.Start();
                object value;
                cache.GetItem("1", out value);
                watch.Stop();
                Debug.WriteLine($"Get complete after {watch.ElapsedMilliseconds}");
            });


            await threadGet;
            await Task.WhenAll(tasks);

            Assert.IsNotNull(exception);
            Assert.IsInstanceOfType(exception, typeof(LockTimeoutException));


        }

        private void Cache_CachingFailed(object sender, RedisCacheException e)
        {
            throw new NotImplementedException();
        }

        [TestMethod]
        public void Purge_removes_stale_items_from_cache()
        {
            var cache = new RedisCache("localhost:6379,allowAdmin=true");

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
            var unused = new RedisCache("localhost:6379").GetItem(null, out _);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void PutItem_validates_key_parameter()
        {
            new RedisCache("localhost:6379").PutItem(null, 42, new string[0], TimeSpan.Zero, DateTimeOffset.Now);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void PutItem_validates_dependentEntitySets_parameter()
        {
            new RedisCache("localhost:6379").PutItem("1", 42, null, TimeSpan.Zero, DateTimeOffset.Now);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void InvalidateSets_validates_parameters()
        {
            new RedisCache("localhost:6379").InvalidateSets(null);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void InvalidateItem_validates_parameters()
        {
            new RedisCache("localhost:6379").InvalidateItem(null);
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
