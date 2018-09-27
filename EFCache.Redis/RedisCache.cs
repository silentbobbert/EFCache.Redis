using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using RedLockNet;
using RedLockNet.SERedis;
using RedLockNet.SERedis.Configuration;
using StackExchange.Redis;

namespace EFCache.Redis
{
	/// <inheritdoc cref="IRedisCache" />
	/// <inheritdoc cref="IDisposable"/>
	/// <summary>
	/// This class implement ICache via IRedisCache. Provides for (potentially) atomic management of sql query result caching
	/// via a mechanism of
	/// a) Redis sets (one per entity set) containing references to keys for queries referencing
	/// that entity set
	/// b) Strings keyed on the hash of the sql query storing the result set
	/// This class makes use of the RedLock algorithm https://redis.io/topics/distlock to offer a
	/// mechanism for two phase-style commit via locks and lua scripts for writes to ensure
	/// changes are atomic and can be serialized per entity set.
	/// </summary>
	public class RedisCache : IRedisCache, IDisposable
	{
		#region Private Vars
		// Prefixes for cache keys
		private const string DefaultCacheIdentifier = "__EFCache.Redis_EntitySetKey_";
		private readonly string _cacheIdentifier;

		private const string DefaultStatsIdentifier = "__EFCache.Redis_Stats_";
		private readonly string _statsIdentifier;
		private const string DefaultQueryHashIdentifier = "__EFCache.Redis_Stats_.Queries";

		// String constants for stats set keys
		private const string HitsIdentifier = "hits";
		private const string MissesIdentifier = "misses";
		private const string InvalidationsIdentifier = "invalidations";

		private const int RetryLimit = 3;
		private static readonly int[] RetryBackoffs = { 250, 500, 1000 };

		private static ConfigurationOptions _configurationOptions;
		private static string _redisConfig;
		private static ConnectionMultiplexer _redis;

		// Lazy instantiate the connection multiplexer per https://docs.microsoft.com/en-us/azure/redis-cache/cache-dotnet-how-to-use-azure-redis-cache
		private static readonly Lazy<ConnectionMultiplexer> LazyRedis = ConnectAndLoadScripts();

		// Lazy instantiate the RedLock factory AFTER the connection multiplexer is lazy instantiated
		private static readonly Lazy<RedLockFactory> LazyRedLockFactory =
			new Lazy<RedLockFactory>(() => RedLockFactory.Create(new List<RedLockMultiplexer> { new RedLockMultiplexer(Connection) }));

		#endregion

		#region Lua Scripting
		// Because the query entries and the sets referencing the query entries must be updated
		// atomically, Lua scripting is used to ensure the atomicity of these operations.

		// Script to iterate over all the entity sets and delete string entries and set members
		// referenced by each of the entity sets. Returns the query keys so stats about those
		// keys can be updated.
		// Accepts a set of keys referencing the entity sets to invalidate
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
		private static byte[] _invalidateSetsScriptSha1;

		// Script to delete a single query entry and to remove it from any entity sets in which
		// it is contained.
		// Accepts the query key as the first key arg and the entity set keys as the remaining key args
		private const string InvalidateSingleKeyScript =
@"local queryKey = KEYS[1]
for i = 2,table.getn(KEYS) do
	 redis.call('srem', KEYS[i], queryKey)
end
redis.call('del', queryKey)";
		private static byte[] _invalidateSingleyKeyScriptSha1;

		// Script to create a new query/result set entry. Add the string entry itself and add
		// entries to any result sets used by the query
		// Accepts the query key as the first key arg and the entity set keys as the remaining key args
		private const string PutItemScript =
@"local queryKey = KEYS[1]
for i = 2,table.getn(KEYS) do
	 redis.call('sadd', KEYS[i], queryKey)
end
redis.call('set', queryKey, ARGV[1])";
		private static byte[] _putItemScriptSha1;

		#endregion

		#region Static Initialization

		/// <summary>
		/// Get the connection from the Lazy initializer
		/// </summary>
		private static ConnectionMultiplexer Connection => LazyRedis.Value;

		/// <summary>
		/// Depending on how the instance was constructed, create a new (or use the existing)
		/// connection multiplexer. Load the Lua scripts and save the hashes for calling before
		/// returing the lazy initialized connection multiplexer
		/// </summary>
		/// <returns>A lazy initialized connection multiplexer that will load the Lua scripts on first use</returns>
		private static Lazy<ConnectionMultiplexer> ConnectAndLoadScripts()
		{
			return _redis != null
				? new Lazy<ConnectionMultiplexer>(() => LoadScripts(_redis))
				: new Lazy<ConnectionMultiplexer>(() =>
					LoadScripts(_redisConfig != null
						? ConnectionMultiplexer.Connect(_redisConfig)
						: ConnectionMultiplexer.Connect(_configurationOptions)));
		}

		/// <summary>
		/// Load the Lua scripts into every instance of the Redis server in the cluster
		/// </summary>
		/// <param name="redis">The instance of the connection muliplexer</param>
		/// <returns>The provided instance of the connection multiplexer</returns>
		private static ConnectionMultiplexer LoadScripts(ConnectionMultiplexer redis)
		{
			var endpoints = redis.GetEndPoints();
			foreach (var endpoint in endpoints)
			{
				var server = redis.GetServer(endpoint);
				_invalidateSetsScriptSha1 = server.ScriptLoad(InvalidateSetsScript);
				_invalidateSingleyKeyScriptSha1 = server.ScriptLoad(InvalidateSingleKeyScript);
				_putItemScriptSha1 = server.ScriptLoad(PutItemScript);
			}
			return redis;
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

		/// <summary>
		/// Handler that can be supplied in the event of caching exceptions.
		/// If no handler is provided, the exception is allowed to be thrown.
		/// Otherwise, the handler is responsible for rethrowing if applicable
		/// </summary>
		public event EventHandler<RedisCacheException> CachingFailed;

		/// <summary>
		/// If true, send hit/miss/invalidation stats per sql query to store in
		/// Redis itself
		/// </summary>
		public bool ShouldCollectStatistics { get; set; } = false;

		/// <summary>
		/// Given a query, hash the query and try to locate results of the
		/// query as a string (byte array) value in Redis at the hashed key.
		/// If the caching policy uses expiration, update the LastAccess timestamp
		/// to the current time.
		/// </summary>
		/// <param name="key">The database query itself</param>
		/// <param name="value">The results of the sql query to be returned</param>
		/// <returns>A boolean indicating whether the entry was found at the provided key</returns>
		public bool GetItem(string key, out object value)
		{
			key.GuardAgainstNullOrEmpty(nameof(key));

			var database = Connection.GetDatabase();
			var hashedKey = HashKey(key);

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
					// Add the sql query to the set of all queries tracked, hash of sql as key, sql itself as value
					database.HashSet(DefaultQueryHashIdentifier,
						new[] { new HashEntry(hashedKey, key) }, CommandFlags.FireAndForget);
					// Increment the stat entry for the given sql query according to whether it was found or not
					database.HashIncrement(AddStatsQualifier(hashedKey), value == null ? MissesIdentifier : HitsIdentifier, 1,
						CommandFlags.FireAndForget);
				}
				catch (Exception e)
				{
					OnCachingFailed(e);
				}
			}
			if (value == null) return false;

			// If we are not using expiry (as indicated by values == MaxValue), return
			var entry = (CacheEntry)value;
			var now = DateTimeOffset.Now;
			if (entry.AbsoluteExpiration == DateTimeOffset.MaxValue && entry.SlidingExpiration == TimeSpan.MaxValue)
			{
				value = entry.Value;
				return true;
			}
			
			// TODO: Handle locking relative to expiry
			if (EntryExpired(entry, now))
			{
				InvalidateItem(hashedKey);
				value = null;
				return false;
			}

			entry.LastAccess = now;
			value = entry;
			try
			{
				database.ObjectSet(hashedKey, entry);
			}
			catch (Exception e)
			{
				// Even though an error has occured, we will return true, because the retrieval of the entity was a success
				OnCachingFailed(e);
			}
			return true;
		}

		/// <summary>
		/// Add sql result set as string entry in Redis. Store result set at hash of sql query and add the hash of sql query
		/// to the set of entity sets involved in the query. Use a Lua script to ensure this operation happens atomically
		/// </summary>
		/// <param name="key">The text of the SQL query</param>
		/// <param name="value">The results of the query (byte array)</param>
		/// <param name="dependentEntitySets">The entity sets (tables) used by the query</param>
		/// <param name="slidingExpiration">The relative time for expiration</param>
		/// <param name="absoluteExpiration">The absolute expiration time</param>
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
				var serializedValue = StackExchangeRedisExtensions.Serialize(new CacheEntry(value, entitySets, slidingExpiration,
					absoluteExpiration));
				database.ScriptEvaluate(_putItemScriptSha1, keys.ToArray(), new RedisValue[]{serializedValue});
			}
			catch (Exception e)
			{
				OnCachingFailed(e);
			}
		}

		/// <summary>
		/// Since a sql database write is about to happen, remove all cached sql query results referencing the provided
		/// entity sets (tables). Then remove the actual entity set values.
		/// Use a Lua script to ensure that this operation happens atomically.
		/// </summary>
		/// <param name="entitySets">The entity sets used by the sql insert, update, delete being issued</param>
		public void InvalidateSets(IEnumerable<string> entitySets)
		{
			// ReSharper disable PossibleMultipleEnumeration
			entitySets.GuardAgainstNull(nameof(entitySets));

			var database = Connection.GetDatabase();
			var setKeys = entitySets.Select(AddCacheQualifier);

			var queryKeys = new HashSet<string>();
			PerformWithRetryBackoff(() =>
			{
				var result = database.ScriptEvaluate(_invalidateSetsScriptSha1, setKeys.ToArray());
				// Result is the set of keys for the queries that were removed. Used below for stats
				foreach (var queryKey in (string[])result)
				{
					queryKeys.Add(queryKey);
				}

				return null;
			});
			// ReSharper restore PossibleMultipleEnumeration

			if (!ShouldCollectStatistics) return;

			// Update the stats for the entity sets to indicate the whole set was invalidated
			foreach (var setKey in setKeys)
			{
				database.HashIncrement(AddStatsQualifier(HashKey(setKey)), InvalidationsIdentifier, 1, CommandFlags.FireAndForget);
			}

			// Update the stats for the queries removed from Redis indicating they were invalidated
			foreach (var hashedKey in queryKeys)
			{
				database.HashIncrement(AddStatsQualifier(hashedKey), InvalidationsIdentifier, 1, CommandFlags.FireAndForget);
			}
		}

		/// <summary>
		/// Remove a single key from Redis and remove it from any entity sets that are referencing it.
		/// Use a Lua script to ensure that this operation happens atomically.
		/// </summary>
		/// <param name="key"></param>
		public void InvalidateItem(string key)
		{
			key.GuardAgainstNullOrEmpty(nameof(key));

			var database = Connection.GetDatabase();
			var hashedKey = HashKey(key);

			var entry = database.ObjectGet<CacheEntry>(hashedKey);
			if (entry == null) return;
			var keys = new List<RedisKey> {hashedKey};
			keys.AddRange(entry.EntitySets.Select(AddCacheQualifier));

			PerformWithRetryBackoff(() =>
			{
				database.ScriptEvaluate(_invalidateSingleyKeyScriptSha1, keys.ToArray());
				return null;
			});

			// Because we don't want to include stats tracking in the lua script, we fire this off regardless of whether the invalidation
			// succeeded. The stats may not be 100% accurate but that's the tradeoff for simplifying the primary logic
			if (!ShouldCollectStatistics) return;

			database.HashIncrement(AddStatsQualifier(hashedKey), InvalidationsIdentifier, 1, CommandFlags.FireAndForget);
		}

		#endregion

		#region ILockableCache

		/// <summary>
		/// Use the RedLock algorithm to create a mutex lock on all of the affected entity sets with
		/// generally sane default expiry, wait, and retry values. If locks are not acquired on all
		/// entity sets, return null indicating lock was not possible. Deadlocks are actually possible
		/// here. With reasonable wait and retry values, deadlocks should not result in failure.
		///
		/// If consistency is a requirement in your application, use this method. These locks should
		/// be acquired prior to any database insert, update, or delete. Once the
		/// db operation is successful, the locks should be released.
		/// </summary>
		/// <param name="entitySets">Entity sets to acquire locks for</param>
		/// <returns>A set of IRedLock objects</returns>
		public List<ILockedEntitySet> Lock(IEnumerable<string> entitySets)
		{
			// Lazy eval the database in case the connection hasn't been instantiated yet
			Connection.GetDatabase();

			var expiry = TimeSpan.FromSeconds(30);
			var wait = TimeSpan.FromSeconds(10);
			var retry = TimeSpan.FromSeconds(1);

			var result = (List<ILockedEntitySet>)PerformWithRetryBackoff(() =>
			{
				var lockedEntitySets = new List<ILockedEntitySet>();
				var sets = entitySets as string[] ?? entitySets.ToArray();
				lockedEntitySets.AddRange(sets.Select(entitySet => new LockedEntitySet
				{
					EntitySet = entitySet,
					Lock = LazyRedLockFactory.Value.CreateLock(entitySet, expiry, wait, retry)
				}));
				

				if (lockedEntitySets.All(les => ((IRedLock)les.Lock).IsAcquired))
					return lockedEntitySets;
				foreach (var redLock in lockedEntitySets.Select(les => les.Lock).Cast<IRedLock>())
				{
					redLock.Dispose();
				}
				throw new Exception($"Could not acquire lock for {string.Join(", ", sets)}");
			});
			return result;
		}


		/// <summary>
		/// Given a set of IRedLock objects, release them
		/// </summary>
		/// <param name="locks">A set of IRedLock objects</param>
		public void ReleaseLock(IEnumerable<ILockedEntitySet> locks)
		{
			var cachedEntitySetLocks = locks.Select(les =>les.Lock).ToList();
			foreach (var cachedEntitySetLock in cachedEntitySetLocks)
			{
				cachedEntitySetLock.Dispose();
			}
		}

		#endregion

		#region Statistics

		/// <summary>
		/// Return the number of keys in the entire Redis store. This will include
		/// EntitySet sets and, potentially, statistics keys
		/// </summary>
		public long Count
		{
			get
			{
				var database = Connection.GetDatabase();
				var count = database.Multiplexer.GetEndPoints()
					.Sum(endpoint => database.Multiplexer.GetServer(endpoint).Keys(pattern: "*").LongCount());
				return count;
			}
		}

		/// <summary>
		/// This method returns a list of statistics about each query in Redis,
		/// including the sql query and its parameters, hits, misses, and invalidations.
		/// This method uses HashScan to get results per page.
		/// TODO: Accept params for sorting and paging
		/// </summary>
		/// <returns>A list of query stats</returns>
		public IEnumerable<QueryStatistics> GetStatistics()
		{
			var database = Connection.GetDatabase();
			var queryEntries = database.HashScan(DefaultQueryHashIdentifier);
			var allStats = new List<StatResult>();
			foreach (var queryEntry in queryEntries)
			{
				var statsEntries = database.HashScan(AddStatsQualifier(queryEntry.Name.ToString()));
				var hashEntries = new List<HashEntry>();
				foreach (var statsEntry in statsEntries)
				{
					hashEntries.Add(statsEntry);
				}
				allStats.Add(new StatResult
				{
					Query = queryEntry,
					Stats = hashEntries.ToArray()
				});
			}
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

		/// <summary>
		/// This method is called when any of the caching operation encounters an exception.
		/// If a <see cref="CachingFailed"/> handler has been provided, pass the exception
		/// to the handler for processing/rethrowing, etc. Otherwise just throw.
		/// </summary>
		/// <param name="e">The exception encountered in one of the caching operations</param>
		/// <param name="memberName">The method in which the exception occurred</param>
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
		/// This method provides a retry mechanism and throws without handling if retries are not successful.
		/// </summary>
		/// <param name="action"></param>
		private static object PerformWithRetryBackoff(Func<object> action)
		{
			var exceptionList = new List<Exception>();
			// Attempt the action, plus up to RetryLimit additional attempts
			for (var i = 0; i <= RetryLimit; i++)
			{
				try
				{
					return action();
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

		/// <summary>
		/// Return a hash of the SQL query for use as a Redis key. In the unlikely event the query is
		/// less than 128 bytes, just use it. Even though that's kind of weird.
		/// </summary>
		/// <param name="key">The SQL query</param>
		/// <returns>A string - normally the SHA1 hash of the SQL query</returns>
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