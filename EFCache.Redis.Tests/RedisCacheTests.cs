using System;
using EFCache.Redis.Tests.Annotations;
using StackExchange.Redis;
using Xunit;

namespace EFCache.Redis.Tests
{
    [Serializable]
    public class TestObject
    {
        public TestObject()
        {

        }
        public string Message { get; set; }
    }

    [UsedImplicitly]
    public class RedisCacheTests
    {
        public RedisCacheTests()
        {
            RedisStorageEmulatorManager.Instance.StartProcess(false);
        }
        [Fact]
        public void Item_cached()
        {
            var cache = new RedisCache("localhost:6379");
            var item = new TestObject { Message = "OK" };

            cache.PutItem("key", item, new string[0], TimeSpan.MaxValue, DateTimeOffset.MaxValue);

            object fromCache;

            Assert.True(cache.GetItem("key", out fromCache));
            Assert.Equal(item.Message, ((TestObject)fromCache).Message);

            Assert.True(cache.GetItem("key", out fromCache));
            Assert.Equal(item.Message, ((TestObject)fromCache).Message);
        }

        [Fact]
        public void Item_not_returned_after_absolute_expiration_expired()
        {
            var cache = new RedisCache("localhost:6379");
            var item = new TestObject { Message = "OK" };

            cache.PutItem("key", item, new string[0], TimeSpan.MaxValue, DateTimeOffset.Now.AddMinutes(-10));

            object fromCache;
            Assert.False(cache.GetItem("key", out fromCache));
            Assert.Null(fromCache);
        }

        [Fact]
        public void Item_not_returned_after_sliding_expiration_expired()
        {
            var cache = new RedisCache("localhost:6379");
            var item = new TestObject { Message = "OK" };

            cache.PutItem("key", item, new string[0], TimeSpan.Zero.Subtract(new TimeSpan(10000)), DateTimeOffset.MaxValue);

            object fromCache;
            Assert.False(cache.GetItem("key", out fromCache));
            Assert.Null(fromCache);
        }

        [Fact]
        public void InvalidateSets_invalidate_items_with_given_sets()
        {
            var cache = new RedisCache("localhost:6379");

            cache.PutItem("1", new object(), new[] { "ES1", "ES2" }, TimeSpan.MaxValue, DateTimeOffset.MaxValue);
            cache.PutItem("2", new object(), new[] { "ES2", "ES3" }, TimeSpan.MaxValue, DateTimeOffset.MaxValue);
            cache.PutItem("3", new object(), new[] { "ES1", "ES3", "ES4" }, TimeSpan.MaxValue, DateTimeOffset.MaxValue);
            cache.PutItem("4", new object(), new[] { "ES3", "ES4" }, TimeSpan.MaxValue, DateTimeOffset.MaxValue);

            cache.InvalidateSets(new[] { "ES1", "ES2" });

            object item;
            Assert.False(cache.GetItem("1", out item));
            Assert.False(cache.GetItem("2", out item));
            Assert.False(cache.GetItem("3", out item));
            Assert.True(cache.GetItem("4", out item));
        }

        [Fact]
        public void InvalidateItem_invalidates_item()
        {
            var cache = new RedisCache("localhost:6379");

            cache.PutItem("1", new object(), new[] { "ES1", "ES2" }, TimeSpan.MaxValue, DateTimeOffset.MaxValue);
            cache.InvalidateItem("1");

            object item;
            Assert.False(cache.GetItem("1", out item));
        }

        [Fact]
        public void Count_returns_numers_of_cached_entries()
        {
            var cache = new RedisCache("localhost:6379,allowAdmin=true");

            cache.Purge();

            Assert.Equal(0, cache.Count);

            cache.PutItem("1", new object(), new[] { "ES1", "ES2" }, TimeSpan.MaxValue, DateTimeOffset.MaxValue);

            Assert.Equal(3, cache.Count); // "1", "ES1", "ES2"

            cache.InvalidateItem("1");

            Assert.Equal(0, cache.Count);
        }

        [Fact]
        public void Purge_removes_stale_items_from_cache()
        {
            var cache = new RedisCache("localhost:6379,allowAdmin=true");

            cache.Purge();

            cache.PutItem("1", new object(), new[] { "ES1", "ES2" }, TimeSpan.MaxValue, DateTimeOffset.Now.AddMinutes(-1));
            cache.PutItem("2", new object(), new[] { "ES1", "ES2" }, TimeSpan.MaxValue, DateTimeOffset.MaxValue);

            Assert.Equal(4, cache.Count); // "1", "2", "ES1", "ES2"

            cache.Purge();

            Assert.Equal(0, cache.Count);

            object item;
            Assert.False(cache.GetItem("1", out item));
            Assert.False(cache.GetItem("2", out item));
        }

        [Fact]
        public void GetItem_validates_parameters()
        {
            object item;

            Assert.Equal(
                "key",
                Assert.Throws<ArgumentNullException>(() => new RedisCache("localhost:6379").GetItem(null, out item)).ParamName);
        }

        [Fact]
        public void PutItem_validates_parameters()
        {
            Assert.Equal(
                "key",
                Assert.Throws<ArgumentNullException>(()
                    => new RedisCache("localhost:6379").PutItem(null, 42, new string[0], TimeSpan.Zero, DateTimeOffset.Now))
                    .ParamName);

            Assert.Equal(
                "dependentEntitySets",
                Assert.Throws<ArgumentNullException>(()
                    => new RedisCache("localhost:6379").PutItem("1", 42, null, TimeSpan.Zero, DateTimeOffset.Now)).ParamName);
        }

        [Fact]
        public void InvalidateSets_validates_parameters()
        {
            Assert.Equal(
                "entitySets",
                Assert.Throws<ArgumentNullException>(() => new RedisCache("localhost:6379").InvalidateSets(null)).ParamName);
        }

        [Fact]
        public void InvalidateItem_validates_parameters()
        {
            Assert.Equal(
                "key",
                Assert.Throws<ArgumentNullException>(() => new RedisCache("localhost:6379").InvalidateItem(null)).ParamName);
        }

        [Fact]
        public void GetItem_does_not_crash_if_cache_is_unavailable()
        {
            var cache = new RedisCache("unknown,abortConnect=false");
            RedisConnectionException exception = null;
            cache.OnConnectionError += (s, e) => exception = e;

            object item;
            var success = cache.GetItem("1", out item);

            Assert.False(success);
            Assert.Null(item);
            Assert.NotNull(exception);
        }

        [Fact]
        public void PutItem_does_not_crash_if_cache_is_unavailable()
        {
            var cache = new RedisCache("unknown,abortConnect=false");
            RedisConnectionException exception = null;
            cache.OnConnectionError += (s, e) => exception = e;

            cache.PutItem("1", new object(), new[] { "ES1", "ES2" }, TimeSpan.MaxValue, DateTimeOffset.MaxValue);

            Assert.NotNull(exception);
        }

        [Fact]
        public void InvalidateItem_does_not_crash_if_cache_is_unavailable()
        {
            var cache = new RedisCache("unknown,abortConnect=false");
            RedisConnectionException exception = null;
            cache.OnConnectionError += (s, e) => exception = e;

            cache.InvalidateItem("1");

            Assert.NotNull(exception);
        }
    }
}
