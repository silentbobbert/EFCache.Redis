using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace EFCache.Redis
{
    // ReSharper disable once ClassWithVirtualMembersNeverInherited.Global
    public class RedisCache : IRedisCache
    {
        private const string DefaultCacheIdentifier = "__EFCache.Redis_EntitySetKey_";
        private const string DefaultStatsIdentifier = "__EFCache.Redis_Stats_";
        private const string DefaultQueryHashIdentifier = "__EFCache.Redis_Stats_.Queries";
        private const string HitsIdentifier = "hits";
        private const string MissesIdentifier = "misses";
        private const string InvalidationsIdentifier = "invalidations";

        //Note- modifying these objects will alter locking scheme
        private readonly object _lock = new object();//used to put instance level lock; only one thread will execute code block per instance

        private IDatabase _database;//lock don't work on this because it is being reassigned each time new connection requested; though _redis.GetDatabase() is thread safe and should be used to let mutiplexor manage connection for best performance. Considering these let's avoid putting lock on it
        private readonly ConnectionMultiplexer _redis;
        private readonly string _cacheIdentifier;
        private readonly string _statsIdentifier;

        public event EventHandler<RedisCacheException> CachingFailed;

        public RedisCache(string config) : this(ConfigurationOptions.Parse(config))
        {
        }

        // ReSharper disable once MemberCanBePrivate.Global
        public RedisCache(ConfigurationOptions options)
        {
            _redis = ConnectionMultiplexer.Connect(options);
            _cacheIdentifier = DefaultCacheIdentifier;
            _statsIdentifier = DefaultStatsIdentifier;
        }

        public RedisCache(ConnectionMultiplexer connection, string cacheIdentifier)
        {
            _redis = connection;
            _cacheIdentifier = cacheIdentifier;
            _statsIdentifier = DefaultStatsIdentifier;
        }

        public RedisCache(ConnectionMultiplexer connection)
        {
            _redis = connection;
            _cacheIdentifier = DefaultCacheIdentifier;
            _statsIdentifier = DefaultStatsIdentifier;
        }

        public RedisCache(string config, string cacheIdentifier)
        {
            _redis = ConnectionMultiplexer.Connect(config);
            _cacheIdentifier = cacheIdentifier;
            _statsIdentifier = DefaultStatsIdentifier;
        }

        public RedisCache(ConfigurationOptions options, string cacheIdentifier)
        {
            _redis = ConnectionMultiplexer.Connect(options);
            _cacheIdentifier = cacheIdentifier;
            _statsIdentifier = DefaultStatsIdentifier;
        }

        public bool ShouldCollectStatistics { get; set; } = false;

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
            key.GuardAgainstNullOrEmpty(nameof(key));
            _database = _redis.GetDatabase(); //connect only if arguments are valid to optimize resources

            var hashedKey = HashKey(key);
            var now = DateTimeOffset.Now;//local variables are thread safe should be out of sync lock

            try
            {
                value = _database.Get<CacheEntry>(hashedKey);
            }
            catch (Exception e)
            {
                value = null;
                OnCachingFailed(e);
            }

            if (ShouldCollectStatistics)
            {
                lock (_lock)
                {
                    try
                    {
                        _database.HashSet(DefaultQueryHashIdentifier,
                            new[] { new HashEntry(hashedKey, key) }, CommandFlags.FireAndForget);
                        _database.HashIncrement(AddStatsQualifier(hashedKey), value == null ? MissesIdentifier : HitsIdentifier, 1, CommandFlags.FireAndForget);
                    }
                    catch (Exception e)
                    {
                        OnCachingFailed(e);
                    }
                }
            }

            if (value == null) return false;

            var entry = (CacheEntry)value;

            if (EntryExpired(entry, now))
            {
                InvalidateItem(hashedKey);
                value = null;
            }
            else
            {
                entry.LastAccess = now;
                value = entry.Value;
                // Update the entry in Redis to save the new LastAccess value.
                lock (_lock)
                {
                    try
                    {
                        _database.Set(hashedKey, entry);
                    }
                    catch (Exception e)
                    {
                        // Eventhough an error has occured, we will return true, because the retrieval of the entity was a success
                        OnCachingFailed(e);
                    }
                }
                return true;
            }

            return false;
        }

        private static bool EntryExpired(CacheEntry entry, DateTimeOffset now) => entry.AbsoluteExpiration < now || (now - entry.LastAccess) > entry.SlidingExpiration;

        public void PutItem(string key, object value, IEnumerable<string> dependentEntitySets, TimeSpan slidingExpiration, DateTimeOffset absoluteExpiration)
        {
            key.GuardAgainstNullOrEmpty(nameof(key));
            // ReSharper disable once PossibleMultipleEnumeration - the guard clause should not enumerate, its just checking the reference is not null
            dependentEntitySets.GuardAgainstNull(nameof(dependentEntitySets));

            _database = _redis.GetDatabase();

            key = HashKey(key);
            // ReSharper disable once PossibleMultipleEnumeration - the guard clause should not enumerate, its just checking the reference is not null
            var entitySets = dependentEntitySets.ToArray();

            lock (_lock)
            {
                try
                {
                    foreach (var entitySet in entitySets)
                    {
                        _database.SetAdd(AddCacheQualifier(entitySet), key, CommandFlags.FireAndForget);
                    }

                    _database.Set(key, new CacheEntry(value, entitySets, slidingExpiration, absoluteExpiration));
                }
                catch (Exception e)
                {
                    OnCachingFailed(e);
                }

                if (ShouldCollectStatistics)
                {
                }
            }
        }

        private RedisKey AddCacheQualifier(string entitySet) => string.Concat(_cacheIdentifier, ".", entitySet);

        private RedisKey AddStatsQualifier(string query) => string.Concat(_statsIdentifier, ".", query);

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
            // ReSharper disable once PossibleMultipleEnumeration - the guard clause should not enumerate, its just checking the reference is not null
            entitySets.GuardAgainstNull(nameof(entitySets));

            _database = _redis.GetDatabase();

            var itemsToInvalidate = new HashSet<string>();

            lock (_lock)
            {
                try
                {
                    // ReSharper disable once PossibleMultipleEnumeration - the guard clause should not enumerate, its just checking the reference is not null
                    foreach (var entitySet in entitySets)
                    {
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

            _database = _redis.GetDatabase();

            var hashedKey = HashKey(key);

            lock (_lock)
            {
                try
                {
                    var entry = _database.Get<CacheEntry>(hashedKey);

                    if (entry == null) return;

                    _database.KeyDelete(hashedKey, CommandFlags.FireAndForget);

                    foreach (var set in entry.EntitySets)
                    {
                        _database.SetRemove(AddCacheQualifier(set), hashedKey, CommandFlags.FireAndForget);
                    }
                }
                catch (Exception e)
                {
                    OnCachingFailed(e);
                }
            }

            if (!ShouldCollectStatistics) return;
            lock (_lock)
            {
                try
                {
                    _database.HashIncrement(AddStatsQualifier(hashedKey), InvalidationsIdentifier, 1, CommandFlags.FireAndForget);
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
            lock (_lock)
            {
                foreach (var endPoint in _database.Multiplexer.GetEndPoints())
                {
                    _database.Multiplexer.GetServer(endPoint).FlushDatabase();
                }
            }
        }

        public IEnumerable<QueryStatistics> GetStatistics()
        {
            _database = _redis.GetDatabase();
            List<StatResult> allStats;
            lock (_lock)
            {
                var queries = _database.HashGetAll(DefaultQueryHashIdentifier);
                allStats = queries.Select(q =>
                    new StatResult
                    {
                        Query = q,
                        Stats = _database.HashGetAll(AddStatsQualifier(q.Name.ToString()))
                    }).ToList();
            }
            return allStats.Select(r =>
                {
                    var stats = r.Stats;
                    var hits = stats.SingleOrDefault(s => s.Name == HitsIdentifier);
                    var misses = stats.SingleOrDefault(s => s.Name == MissesIdentifier);
                    var invalidations = stats.SingleOrDefault(s => s.Name == InvalidationsIdentifier);
                    var matches = Regex.Matches(r.Query.Value.ToString(), @"(_p__linq__\d+=(?:(?!_p__linq).)*)+");
                    var queryParams = new List<string>();
                    if (matches.Count > 0)
                    {
                        var match = matches[0];
                        if (match.Groups.Count > 1)
                        {
                            var captures = match.Groups[1].Captures;
                            for (var i = 0; i < captures.Count; i++)
                            {
                                queryParams.Add(captures[i].Value);
                            }
                        }
                    }

                    return new QueryStatistics
                    {
                        Query = r.Query.Value.ToString(),
                        QueryHash = r.Query.Name.ToString(),
                        QueryParams = queryParams,
                        Hits = Convert.ToInt64(hits == default(HashEntry) ? 0 : hits.Value),
                        Misses = Convert.ToInt64(misses == default(HashEntry) ? 0 : misses.Value),
                        Invalidations = Convert.ToInt64(invalidations == default(HashEntry) ? 0 : invalidations.Value)
                    };
                })
                .OrderBy(o => o.Query);
        }
    }

    internal class StatResult
    {
        public HashEntry Query { get; set; }
        public HashEntry[] Stats { get; set; }
    }
}