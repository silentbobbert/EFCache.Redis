﻿using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;

namespace EFCache.Redis
{
    public class RedisCache : IRedisCache
    {
        //Note- modifying these objects will alter locking scheme
        private static object _globalLock;//used to put sync locks at global level; only one thread across app will access the code block locked via this
        private object _lock;//used to put instance level lock; only one thread will execute code block per instance

        private IDatabase _database;//lock don't work on this because it is being reassigned each time new connection requested; though _redis.GetDatabase() is thread safe and should be used to let mutiplexor manage connection for best performance. Considering these let's avoid putting lock on it
        private readonly ConnectionMultiplexer _redis;
        private const string EntitySetKey = "__EFCache.Redis_EntitySetKey_";
        public event EventHandler<RedisCacheException> CachingFailed;

        public RedisCache(string config)
        {
            _redis = ConnectionMultiplexer.Connect(config);
        }

        protected virtual void OnCachingFailed(Exception e, [CallerMemberName] string memberName = "")
        {
            var handler = CachingFailed;
            //don't simply digest, let caller handle exception if no handler provided
            if (handler == null)
            {
                throw new RedisCacheException("Redis | Caching failed for " + memberName, e);
            }
            var redisCacheException = new RedisCacheException("Caching failed for " + memberName, e);
            handler(this, redisCacheException);
        }

        public bool GetItem(string key, out object value)
        {
            if (key == null)
            {
                throw new ArgumentNullException("key");
            }
            _database = _redis.GetDatabase();//connect only if arguments are valid to optimize resources 

            key = HashKey(key);
            var now = DateTimeOffset.Now;//local variables are thread safe should be out of sync lock
            
            lock (_lock)
            {
                try {
                    value = _database.Get<CacheEntry>(key);
                } catch (Exception e) {
                    value = null;
                    OnCachingFailed(e);
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
            if (key == null)
            {
                throw new ArgumentNullException("key");
            }

            if (dependentEntitySets == null)
            {
                throw new ArgumentNullException("dependentEntitySets");
            }
            _database = _redis.GetDatabase();
            
            key = HashKey(key);
            var entitySets = dependentEntitySets.ToArray();

            lock (_lock)
            {
                try {
                    _database.Set(key, new CacheEntry(value, entitySets, slidingExpiration, absoluteExpiration));
                    foreach (var entitySet in entitySets) {
                        _database.SetAdd(GetEntitySetKey(entitySet), key);
                    }
                } catch (Exception e) {
                    OnCachingFailed(e);
                }

            }
        }

        private static RedisKey GetEntitySetKey(string entitySet)
        {
            return EntitySetKey + entitySet;
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
            if (entitySets == null)
            {
                throw new ArgumentNullException("entitySets");
            }
            _database = _redis.GetDatabase();
            
            var itemsToInvalidate = new HashSet<string>();

            lock (_lock)
            {
                try {
                    foreach (var entitySet in entitySets) {
                        var entitySetKey = GetEntitySetKey(entitySet);
                        var keys = _database.SetMembers(entitySetKey).Select(v => v.ToString());
                        itemsToInvalidate.UnionWith(keys);
                        _database.KeyDelete(EntitySetKey);
                    }
                } catch (Exception e) {
                    OnCachingFailed(e);
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
            if (key == null)
            {
                throw new ArgumentNullException("key");
            }
            _database = _redis.GetDatabase();

            key = HashKey(key);

            lock (_lock) {
                try {
                    var entry = _database.Get<CacheEntry>(key);

                    if (entry == null) return;

                    _database.KeyDelete(key);

                    foreach (var set in entry.EntitySets) {
                        _database.SetRemove(GetEntitySetKey(set), key);
                    }
                } catch (Exception e) {
                    OnCachingFailed(e);
                }
            }
        }
        public Int64 Count
        {
            get
            {
                _database = _redis.GetDatabase();
                lock (_lock)
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
            lock (_globalLock)
            {
                foreach (var endPoint in _database.Multiplexer.GetEndPoints())
                {
                    _database.Multiplexer.GetServer(endPoint).FlushDatabase();
                }
            }

        }
    }
}
