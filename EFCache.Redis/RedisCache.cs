using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace EFCache.Redis
{
    public class RedisCache : IRedisCache
    {
        private IDatabase _database;
        private readonly ConnectionMultiplexer _redis;
        public event EventHandler<RedisConnectionException> OnConnectionError;
        private const string EntitySetKey = "__EFCache.Redis_EntitySetKey_";
        public RedisCache(string config)
        {
            _redis = ConnectionMultiplexer.Connect(config);
        }
        public bool GetItem(string key, out object value)
        {
            _database = _redis.GetDatabase();

            if (key == null)
            {
                throw new ArgumentNullException("key");
            }

            key = HashKey(key);

            lock (_database)
            {
                var now = DateTimeOffset.Now;

                try {
                    value = _database.Get<CacheEntry>(key);
                } catch (RedisConnectionException e) {
                    value = null;
                    RaiseConnectionError(e);
                }

                if (value == null) return false;

                var entry = (CacheEntry)value;

                if (EntryExpired(entry, now))
                {
                    InvalidateItem(key);
                    value = null;
                }
                else
                {
                    entry.LastAccess = now;
                    value = entry.Value;
                    return true;
                }
            }

            return false;

        }
        private static bool EntryExpired(CacheEntry entry, DateTimeOffset now)
        {
            return entry.AbsoluteExpiration < now || (now - entry.LastAccess) > entry.SlidingExpiration;
        }

        public void PutItem(string key, object value, IEnumerable<string> dependentEntitySets, TimeSpan slidingExpiration, DateTimeOffset absoluteExpiration)
        {
            _database = _redis.GetDatabase();
            if (key == null)
            {
                throw new ArgumentNullException("key");
            }
            
            key = HashKey(key);

            if (dependentEntitySets == null)
            {
                throw new ArgumentNullException("dependentEntitySets");
            }

            lock (_database)
            {
                var entitySets = dependentEntitySets.ToArray();

                try {
                    _database.Set(key, new CacheEntry(value, entitySets, slidingExpiration, absoluteExpiration));
                    foreach (var entitySet in entitySets) {
                        _database.SetAdd(GetEntitySetKey(entitySet), key);
                    }
                } catch (RedisConnectionException e) {
                    RaiseConnectionError(e);
                }

            }
        }

        private static RedisKey GetEntitySetKey(string entitySet)
        {
            return EntitySetKey + entitySet;
        }

        private void RaiseConnectionError(RedisConnectionException e)
        {
            var onConnectionError = OnConnectionError;
            if (onConnectionError != null)
                onConnectionError(this, e);
        }

        private static string HashKey(string key)
        {
            //Looking up large Keys in Redis can be expensive (comparing Large Strings), so if keys are large, hash them, otherwise if keys are short just use as-is
            if (key.Length <= 128) return key;
            using (var sha = new SHA1CryptoServiceProvider())
            {
                key = Convert.ToBase64String(sha.ComputeHash(Encoding.UTF8.GetBytes(key)));
                return key;
            }
        }

        public void InvalidateSets(IEnumerable<string> entitySets)
        {
            _database = _redis.GetDatabase();
            if (entitySets == null)
            {
                throw new ArgumentNullException("entitySets");
            }

            lock (_database)
            {
                var itemsToInvalidate = new HashSet<string>();

                try {
                    foreach (var entitySet in entitySets) {
                        var entitySetKey = GetEntitySetKey(entitySet);
                        var keys = _database.SetMembers(entitySetKey).Select(v => v.ToString());
                        itemsToInvalidate.UnionWith(keys);
                        _database.KeyDelete(EntitySetKey);
                    }
                } catch (RedisConnectionException e) {
                    RaiseConnectionError(e);
                    return;
                }

                foreach (var key in itemsToInvalidate)
                {
                    InvalidateItem(key);
                }
            }
        }

        public void InvalidateItem(string key)
        {
            _database = _redis.GetDatabase();
            if (key == null)
            {
                throw new ArgumentNullException("key");
            }

            key = HashKey(key);

            lock (_database) {
                try {
                    var entry = _database.Get<CacheEntry>(key);

                    if (entry == null) return;

                    _database.KeyDelete(key);

                    foreach (var set in entry.EntitySets) {
                        _database.SetRemove(GetEntitySetKey(set), key);
                    }
                } catch (RedisConnectionException e) {
                    RaiseConnectionError(e);
                }
            }
        }
        public Int64 Count
        {
            get
            {
                _database = _redis.GetDatabase();
                lock (_database)
                {
                    var count = _database.Multiplexer.GetEndPoints()
                        .Sum(endpoint => _database.Multiplexer.GetServer(endpoint).Keys(pattern: "*").LongCount());
                    return count;
                }
            }
        }
        public void Purge()
        {
            _database = _redis.GetDatabase();
            lock (_database)
            {
                foreach (var endPoint in _database.Multiplexer.GetEndPoints())
                {
                    _database.Multiplexer.GetServer(endPoint).FlushDatabase();
                }
            }

        }
    }
}
