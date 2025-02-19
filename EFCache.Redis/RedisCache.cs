using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

namespace EFCache.Redis
{
    // ReSharper disable once ClassWithVirtualMembersNeverInherited.Global
    public class RedisCache : IRedisCache
    {
        private const string DefaultCacheIdentifier = "__EFCache.Redis_EntitySetKey_";

        //Note- modifying these objects will alter locking scheme
        private readonly object _lock = new object();//used to put instance level lock; only one thread will execute code block per instance

        private IDatabase _database;//lock don't work on this because it is being reassigned each time new connection requested; though _redis.GetDatabase() is thread safe and should be used to let mutiplexor manage connection for best performance. Considering these let's avoid putting lock on it
        private readonly ConnectionMultiplexer _redis;
        private readonly string _cacheIdentifier;
        public event EventHandler<RedisCacheException> CachingFailed;

        public int LockWaitTimeout { get; set; } = 1000;

        public RedisCache(string config) : this(ConfigurationOptions.Parse(config)) {   }

        // ReSharper disable once MemberCanBePrivate.Global
        public RedisCache(ConfigurationOptions options)
        {
            _redis = ConnectionMultiplexer.Connect(options);
            _cacheIdentifier = DefaultCacheIdentifier; 
        }

        public RedisCache(ConnectionMultiplexer connection, string cacheIdentifier)
        {
            _redis = connection;
            _cacheIdentifier = cacheIdentifier;
        }

        public RedisCache(ConnectionMultiplexer connection)
        {
            _redis = connection;
            _cacheIdentifier = DefaultCacheIdentifier;
        }

        public RedisCache(string config, string cacheIdentifier)
        {
            _redis = ConnectionMultiplexer.Connect(config);
            _cacheIdentifier = cacheIdentifier;
        }
        
        public RedisCache(ConfigurationOptions options, string cacheIdentifier)
        {
            _redis = ConnectionMultiplexer.Connect(options);
            _cacheIdentifier = cacheIdentifier;
        }

        protected virtual void OnCachingFailed(Exception e, [CallerMemberName] string memberName = "")
        {
            var handler = CachingFailed;
            var redisCacheException = new RedisCacheException("Redis | Caching failed for " + memberName, e);
            //don't simply digest, let caller handle exception if no handler provided
            if (handler == null)
            {
                throw redisCacheException;
            }
            handler(this, redisCacheException);
        }

        public bool GetItem(string key, out object value)
        {
            key.GuardAgainstNullOrEmpty(nameof(key));
            EnsureDatabase();

            key = HashKey(key);
            var now = DateTimeOffset.Now; //local variables are thread safe should be out of sync lock
            
            try
            {
                lock (_lock)
                {
                    value = _database.Get<CacheEntry>(key);
                }
                
            } 
            catch (Exception e) 
            {
                value = null;
                OnCachingFailed(e);
            }

            if (value == null)
            {
                return false;
            }

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
                // Update the entry in Redis to save the new LastAccess value.

                var handler = CachingFailed;
                if (Monitor.TryEnter(_lock, LockWaitTimeout))
                { 
                    try
                    {
                        _database.Set(key, entry, GetTimeSpanExpiration(entry.AbsoluteExpiration, now));
                    }
                    catch (Exception e)
                    {
                        // Eventhough an error has occured, we will return true, because the retrieval of the entity was a success
                        OnCachingFailed(e);
                    }
                    finally
                    {
                        Monitor.Exit(_lock);
                    }
                }
                else if (handler != null)
                {
                    OnCachingFailed(new LockTimeoutException($"Timeout of {LockWaitTimeout}ms while waiting for lock (ThreadId: {Thread.CurrentThread.ManagedThreadId})") { ThreadId = Thread.CurrentThread.ManagedThreadId });
                }
                return true;
            }

            return false;
        }

        private void EnsureDatabase() => _database = _database ?? _redis.GetDatabase();

        private static bool EntryExpired(CacheEntry entry, DateTimeOffset now) => entry.AbsoluteExpiration <= now || (now - entry.LastAccess) > entry.SlidingExpiration;

        public void PutItem(string key, object value, IEnumerable<string> dependentEntitySets, TimeSpan slidingExpiration, DateTimeOffset absoluteExpiration)
        {
            key.GuardAgainstNullOrEmpty(nameof(key));
            // ReSharper disable once PossibleMultipleEnumeration - the guard clause should not enumerate, its just checking the reference is not null
            dependentEntitySets.GuardAgainstNull(nameof(dependentEntitySets));

            EnsureDatabase();
            
            key = HashKey(key);
            // ReSharper disable once PossibleMultipleEnumeration - the guard clause should not enumerate, its just checking the reference is not null
            var entitySets = dependentEntitySets.ToArray();

            var handler = CachingFailed;
            if (Monitor.TryEnter(_lock, LockWaitTimeout))
            {
                try
                {
                    foreach (var entitySet in entitySets)
                    {
                        _database.SetAdd(AddCacheQualifier(entitySet), key, CommandFlags.FireAndForget);
                    }

                    _database.Set(key, new CacheEntry(value, entitySets, slidingExpiration, absoluteExpiration), GetTimeSpanExpiration(absoluteExpiration, DateTimeOffset.Now));
                }
                catch (Exception e)
                {
                    OnCachingFailed(e);
                }
                finally
                {
                    Monitor.Exit(_lock);
                }
            }
            else if (handler != null)
            {
                OnCachingFailed(new LockTimeoutException($"Timeout of {LockWaitTimeout}ms while waiting for lock (ThreadId: {Thread.CurrentThread.ManagedThreadId})") { ThreadId = Thread.CurrentThread.ManagedThreadId });
            }
        
        }

        private TimeSpan GetTimeSpanExpiration(DateTimeOffset expiration, DateTimeOffset now)
        {
            return TimeSpan.FromTicks(expiration.UtcTicks - now.UtcTicks);
        }

        private RedisKey AddCacheQualifier(string entitySet) => string.Concat(_cacheIdentifier, ".", entitySet);

        private static string HashKey(string key)
        {
            //Looking up large Keys in Redis can be expensive (comparing Large Strings), so if keys are large, hash them, otherwise if keys are short just use as-is
            if (key.Length <= 128)
            {
                return key;
            }

            using (var sha = new SHA1CryptoServiceProvider())
            {
                key = Convert.ToBase64String(sha.ComputeHash(Encoding.UTF8.GetBytes(key)));
                return key;
            }
        }

        public void InvalidateSets(IEnumerable<string> entitySets)
        {
            // ReSharper disable once PossibleMultipleEnumeration - the guard clause should not enumerate, its just checking the reference is not null
            entitySets.GuardAgainstNull(nameof(entitySets));

            EnsureDatabase();

            var itemsToInvalidate = new HashSet<string>();

            lock (_lock)
            {
                try 
                {
                    // ReSharper disable once PossibleMultipleEnumeration - the guard clause should not enumerate, its just checking the reference is not null
                    foreach (var entitySet in entitySets) {
                        var entitySetKey = AddCacheQualifier(entitySet);
                        var keys = _database.SetMembers(entitySetKey).Select(v => v.ToString());
                        itemsToInvalidate.UnionWith(keys);
                        _database.KeyDelete(entitySetKey, CommandFlags.FireAndForget);
                    }
                } 
                catch (Exception e) 
                {
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
            key.GuardAgainstNullOrEmpty(nameof(key));

            EnsureDatabase();

            key = HashKey(key);

            // todo change locking to apply to the entry key
            lock (_lock) 
            {
                try 
                {
                    var entry = _database.Get<CacheEntry>(key);

                    if (entry == null)
                    {
                        return;
                    }

                    _database.KeyDelete(key, CommandFlags.FireAndForget);

                    foreach (var set in entry.EntitySets) {
                        _database.SetRemove(AddCacheQualifier(set), key, CommandFlags.FireAndForget);
                    }
                } 
                catch (Exception e) 
                {
                    OnCachingFailed(e);
                }
            }
        }
        // ReSharper disable once BuiltInTypeReferenceStyle
        public Int64 Count
        {
            get
            {
                EnsureDatabase();
                
                var count = _database.Multiplexer.GetEndPoints()
                    .Sum(endpoint => _database.Multiplexer.GetServer(endpoint).Keys(pattern: "*").LongCount());
                return count;
                
            }
        }
        public void Purge()
        {
            EnsureDatabase();
            lock (_lock)
            {
                foreach (var endPoint in _database.Multiplexer.GetEndPoints())
                {
                    _database.Multiplexer.GetServer(endPoint).FlushDatabase();
                }
            }

        }
    }
}
