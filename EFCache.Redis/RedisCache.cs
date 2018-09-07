using RedLockNet.SERedis;
using RedLockNet.SERedis.Configuration;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace EFCache.Redis
{
	public class RedisCache : IRedisCache, IDisposable
	{
		#region Private Vars
		private const string DefaultCacheIdentifier = "__EFCache.Redis_EntitySetKey_";
		private const string DefaultStatsIdentifier = "__EFCache.Redis_Stats_";
		private const string DefaultQueryHashIdentifier = "__EFCache.Redis_Stats_.Queries";
		private const string HitsIdentifier = "hits";
		private const string MissesIdentifier = "misses";
		private const string InvalidationsIdentifier = "invalidations";
		private const string LockResource = "lock-resource";

		private const int RetryLimit = 3;
		private static readonly int[] RetryBackoffs = { 250, 500, 1000 };

		private static ConfigurationOptions _configurationOptions;
		private static string _redisConfig;
		private static ConnectionMultiplexer _redis;

		private static readonly Lazy<ConnectionMultiplexer> LazyRedis = ConnectAndLoadScripts();

		private static readonly Lazy<RedLockFactory> LazyRedLockFactory =
			new Lazy<RedLockFactory>(() => RedLockFactory.Create(new List<RedLockMultiplexer> { new RedLockMultiplexer(Connection) }));

		private readonly string _cacheIdentifier;
		private readonly string _statsIdentifier;

		private const string InvalidateSetsScript =
@"local queryKeys = {}
for _, entitySetKey in ipairs(KEYS) do
	 local keys = redis.call('smembers', entitySetKey)
	 for _, queryKey in ipairs(keys) do
		table.insert(queryKeys, queryKey)
		redis.call('del', queryKey)
		redis.call('srem', entitySetKey, queryKey)
	 end
end
return queryKeys";

		private const string InvalidateSingleKeyScript =
@"local queryKey = KEYS[1]
for i = 2,table.getn(KEYS) do
	 redis.call('srem', KEYS[i], queryKey)
end
redis.call('del', queryKey)";

		private const string PutItemScript =
@"local rsKey = KEYS[1]
for i = 2,table.getn(KEYS) do
	 redis.call('sadd', KEYS[i], rsKey)
end
redis.call('set', rsKey, ARGV[1])";

		private static byte[] _invalidateSetsScriptSha1;
		private static byte[] _invalidateSingleyKeyScriptSha1;
		private static byte[] _putItemScriptSha1;

		#endregion

		#region Static Initialization

		private static ConnectionMultiplexer Connection => LazyRedis.Value;

		private static Lazy<ConnectionMultiplexer> ConnectAndLoadScripts()
		{
			return _redis != null
				? new Lazy<ConnectionMultiplexer>(() => LoadScripts(_redis))
				: new Lazy<ConnectionMultiplexer>(() =>
					LoadScripts(_redisConfig != null
						? ConnectionMultiplexer.Connect(_redisConfig)
						: ConnectionMultiplexer.Connect(_configurationOptions)));
		}

		private static ConnectionMultiplexer LoadScripts(ConnectionMultiplexer r)
		{
			var endpoints = r.GetEndPoints();
			foreach (var endpoint in endpoints)
			{
				var server = r.GetServer(endpoint);
				_invalidateSetsScriptSha1 = server.ScriptLoad(InvalidateSetsScript);
				_invalidateSingleyKeyScriptSha1 = server.ScriptLoad(InvalidateSingleKeyScript);
				_putItemScriptSha1 = server.ScriptLoad(PutItemScript);
			}
			return r;
		}

		#endregion

		#region Constructors

		public RedisCache(string config) : this(ConfigurationOptions.Parse(config))
		{
		}

		public RedisCache(ConfigurationOptions options)
		{
			_configurationOptions = options;
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
			_redisConfig = config;
			_cacheIdentifier = cacheIdentifier;
			_statsIdentifier = DefaultStatsIdentifier;
		}

		public RedisCache(ConfigurationOptions options, string cacheIdentifier)
		{
			_configurationOptions = options;
			_cacheIdentifier = cacheIdentifier;
			_statsIdentifier = DefaultStatsIdentifier;
		}

		#endregion

		#region ICache

		public event EventHandler<RedisCacheException> CachingFailed;

		public bool ShouldCollectStatistics { get; set; } = false;

		public bool GetItem(string key, out object value)
		{
			key.GuardAgainstNullOrEmpty(nameof(key));

			var database = Connection.GetDatabase();
			var hashedKey = HashKey(key);
			var now = DateTimeOffset.Now;

			try
			{
				value = database.ObjectGet<CacheEntry>(hashedKey);
			}
			catch (Exception e)
			{
				value = null;
				OnCachingFailed(e);
			}

			if (ShouldCollectStatistics)
			{
				try
				{
					database.HashSet(DefaultQueryHashIdentifier,
						new[] { new HashEntry(hashedKey, key) }, CommandFlags.FireAndForget);
					database.HashIncrement(AddStatsQualifier(hashedKey), value == null ? MissesIdentifier : HitsIdentifier, 1,
						CommandFlags.FireAndForget);
				}
				catch (Exception e)
				{
					OnCachingFailed(e);
				}
			}

			if (value == null) return false;

			var entry = (CacheEntry)value;

			if (EntryExpired(entry, now))
			{
				var redLock = Lock(null, new List<string>{hashedKey});
				InvalidateItem(hashedKey);
				ReleaseLock(redLock);
				value = null;
			}
			else
			{
				value = entry.Value;
				
				// If not expiring keys, no need to make another Redis call
				if (entry.AbsoluteExpiration == DateTimeOffset.MaxValue && entry.SlidingExpiration == TimeSpan.MaxValue) return true;

				// Update the entry in Redis to save the new LastAccess value.
				entry.LastAccess = now;
				try
				{
					var redLock = Lock(null, new List<string>{hashedKey});
					database.ObjectSet(hashedKey, entry);
					ReleaseLock(redLock);
				}
				catch (Exception e)
				{
					// Eventhough an error has occured, we will return true, because the retrieval of the entity was a success
					OnCachingFailed(e);
				}
				return true;
			}

			return false;
		}

		public void PutItem(string key, object value, IEnumerable<string> dependentEntitySets, TimeSpan slidingExpiration,
			DateTimeOffset absoluteExpiration)
		{
			key.GuardAgainstNullOrEmpty(nameof(key));
			var entitySets = dependentEntitySets as string[] ?? dependentEntitySets.ToArray();
			entitySets.GuardAgainstNull(nameof(dependentEntitySets));

			var database = Connection.GetDatabase();
			var hashedKey = HashKey(key);

			var keys = new List<RedisKey> {hashedKey};
			keys.AddRange(entitySets.Select(AddCacheQualifier));

			try
			{
				var serializedValue = StackExchangeRedisExtensions.Serialize(new CacheEntry(value, entitySets.ToArray(), slidingExpiration,
					absoluteExpiration));
				database.ScriptEvaluate(_putItemScriptSha1, keys.ToArray(), new RedisValue[]{serializedValue});
			}
			catch (Exception e)
			{
				OnCachingFailed(e);
			}
		}

		public void InvalidateSets(IEnumerable<string> entitySets)
		{
			// ReSharper disable PossibleMultipleEnumeration
			entitySets.GuardAgainstNull(nameof(entitySets));

			var database = Connection.GetDatabase();
			var setKeys = entitySets.Select(AddCacheQualifier);

			//TODO: Remove this!!
			var random = new Random();
			var queryKeys = new System.Collections.Generic.HashSet<string>();
			PerformWithRetryBackoff(() =>
			{
				//TODO: Remove this!!
				//if (new[] { 0 }.Contains(random.Next() % 3)) throw new Exception("Chaos monkey");

				var result = database.ScriptEvaluate(_invalidateSetsScriptSha1, setKeys.ToArray());
				foreach (var queryKey in (string[])result)
				{
					queryKeys.Add(queryKey);
				}
			});
			// ReSharper restore PossibleMultipleEnumeration

			if (!ShouldCollectStatistics) return;

			foreach (var setKey in setKeys)
			{
				database.HashIncrement(AddStatsQualifier(HashKey(setKey)), InvalidationsIdentifier, 1, CommandFlags.FireAndForget);
			}

			foreach (var hashedKey in queryKeys)
			{
				database.HashIncrement(AddStatsQualifier(hashedKey), InvalidationsIdentifier, 1, CommandFlags.FireAndForget);
			}
		}

		public void InvalidateItem(string key)
		{
			key.GuardAgainstNullOrEmpty(nameof(key));

			var database = Connection.GetDatabase();
			var hashedKey = HashKey(key);

			var entry = database.ObjectGet<CacheEntry>(key);
			if (entry == null) return;
			var keys = new List<RedisKey> {key};
			keys.AddRange(entry.EntitySets.Select(AddCacheQualifier));

			//TODO: Remove this!!
			var random = new Random();

			PerformWithRetryBackoff(() =>
			{
				//TODO: Remove this!!
				// (new[] { 0 }.Contains(random.Next() % 3)) throw new Exception("Chaos monkey");

				database.ScriptEvaluate(_invalidateSingleyKeyScriptSha1, keys.ToArray());
			});

			// Because we don't want to include stats tracking in the, we fire this off regardless of whether the transaction
			// succeeded. The stats may not be 100% accurate but that's the tradeoff for simplifying the primary logic
			if (!ShouldCollectStatistics) return;

			database.HashIncrement(AddStatsQualifier(hashedKey), InvalidationsIdentifier, 1, CommandFlags.FireAndForget);
		}

		#endregion

		#region ILockableCache

		public object Lock(IEnumerable<string> entitySets, IEnumerable<string> keys)
		{
			// TODO: build a mechanism that uses the entitySets and keys params to create multiple locks so we don't have to lock the whole DB on write

			// Lazy eval the database in case the connection hasn't been instantiated yet
			Connection.GetDatabase();

			var expiry = TimeSpan.FromSeconds(30);
			var wait = TimeSpan.FromSeconds(10);
			var retry = TimeSpan.FromSeconds(1);

			var redLock = LazyRedLockFactory.Value.CreateLock(LockResource, expiry, wait, retry);
			return redLock.IsAcquired ? redLock : null;
		}

		public void ReleaseLock(object @lock)
		{
			var redLock = (RedLock)@lock;
			redLock.Dispose();
		}

		#endregion

		#region Statistics

		public Int64 Count
		{
			get
			{
				var database = Connection.GetDatabase();
				var count = database.Multiplexer.GetEndPoints()
					.Sum(endpoint => database.Multiplexer.GetServer(endpoint).Keys(pattern: "*").LongCount());
				return count;
			}
		}

		public IEnumerable<QueryStatistics> GetStatistics()
		{
			var database = Connection.GetDatabase();
			var queries = database.HashGetAll(DefaultQueryHashIdentifier);
			var allStats = queries.Select(q =>
				new StatResult
				{
					Query = q,
					Stats = database.HashGetAll(AddStatsQualifier(q.Name.ToString()))
				}).ToList();
			return allStats.Select(r =>
				{
					var stats = r.Stats;
					var hits = stats.SingleOrDefault(s => s.Name == HitsIdentifier);
					var misses = stats.SingleOrDefault(s => s.Name == MissesIdentifier);
					var invalidations = stats.SingleOrDefault(s => s.Name == InvalidationsIdentifier);
					var matches = Regex.Matches(r.Query.Value.ToString(), @"(_p__linq__\d+=(?:(?!_p__linq).)*|_EntityKeyValue\d+=(?:(?!_EntityKeyValue).)*)+");
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

		#endregion

		#region Error Handling

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

		/// <summary>
		/// This provides a retry mechanism and throws without handling if retries are not successful.
		/// This should only be used by the invalidation process. Gets and sets should be quick and
		/// there are no data integrity issues if they fail.
		/// An unsuccessful invalidation will cause the cache to become incoherent and should be avoided
		/// at all costs.
		/// </summary>
		/// <param name="action"></param>
		private static void PerformWithRetryBackoff(Action action)
		{
			var exceptionList = new List<Exception>();
			// Attempt the action, plus up to RetryLimit additional attempts
			for (var i = 0; i <= RetryLimit; i++)
			{
				try
				{
					action();
					return;
				}
				catch (Exception e)
				{
					exceptionList.Add(e);
					if (i < RetryLimit - 1)
						Thread.Sleep(RetryBackoffs[i]);
				}
			}

			throw new AggregateException("Retry exception", exceptionList);
		}

		#endregion

		#region Utility
		
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
		private static bool EntryExpired(CacheEntry entry, DateTimeOffset now) =>
			entry.AbsoluteExpiration < now || (now - entry.LastAccess) > entry.SlidingExpiration;

		#endregion

		public void Purge()
		{
			var database = Connection.GetDatabase();
			foreach (var endPoint in database.Multiplexer.GetEndPoints())
			{
				database.Multiplexer.GetServer(endPoint).FlushDatabase();
			}
		}

		public void Dispose()
		{
			LazyRedLockFactory.Value.Dispose();
		}
	}

	internal class StatResult
	{
		public HashEntry Query { get; set; }
		public HashEntry[] Stats { get; set; }
	}
}