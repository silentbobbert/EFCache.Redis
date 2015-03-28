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
        private readonly Dictionary<string, HashSet<string>> _entitySetToKey = new Dictionary<string, HashSet<string>>();
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

                value = _database.Get<CacheEntry>(key);

                if (value == null) return false;

                var entry = value as CacheEntry;

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

                _database.Set(key, new CacheEntry(value, entitySets, slidingExpiration, absoluteExpiration));

                foreach (var entitySet in entitySets)
                {
                    HashSet<string> keys;

                    if (!_entitySetToKey.TryGetValue(entitySet, out keys))
                    {
                        keys = new HashSet<string>();
                        _entitySetToKey[entitySet] = keys;
                    }

                    keys.Add(key);
                }
            }
        }

        private string HashKey(string key)
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

                foreach (var entitySet in entitySets)
                {
                    HashSet<string> keys;

                    if (!_entitySetToKey.TryGetValue(entitySet, out keys)) continue;
                    itemsToInvalidate.UnionWith(keys);

                    _entitySetToKey.Remove(entitySet);
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

            lock (_database)
            {
                var entry = _database.Get<CacheEntry>(key);

                if (entry == null) return;

                _database.KeyDelete(key);

                foreach (var set in entry.EntitySets)
                {
                    HashSet<string> keys;
                    if (_entitySetToKey.TryGetValue(set, out keys))
                    {
                        keys.Remove(key);
                    }
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
