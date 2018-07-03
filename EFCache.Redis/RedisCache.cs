using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using StackExchange.Redis;

namespace EFCache.Redis
{
	public class RedisCache : IRedisCache
	{
		private const string DefaultCacheIdentifier = "__EFCache.Redis_EntitySetKey_";
		private const string DefaultStatsIdentifier = "__EFCache.Redis_Stats_";
		private const string DefaultQueryHashIdentifier = "__EFCache.Redis_Stats_.Queries";
		private const string HitsIdentifier = "hits";
		private const string MissesIdentifier = "misses";
		private const string InvalidationsIdentifier = "invalidations";

		private static ConfigurationOptions _configurationOptions;
		private static string _redisConfig;
		private static ConnectionMultiplexer _redis;

		private static readonly Lazy<ConnectionMultiplexer> LazyRedis = _redis != null
			? new Lazy<ConnectionMultiplexer>(() => _redis)
			: new Lazy<ConnectionMultiplexer>(() => _redisConfig != null
				? ConnectionMultiplexer.Connect(_redisConfig)
				: ConnectionMultiplexer.Connect(_configurationOptions));

		private readonly string _cacheIdentifier;
		private readonly string _statsIdentifier;

		private static ConnectionMultiplexer Connection => LazyRedis.Value;

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
				InvalidateItem(hashedKey);
				value = null;
			}
			else
			{
				entry.LastAccess = now;
				value = entry.Value;
				// Update the entry in Redis to save the new LastAccess value.
				try
				{
					database.ObjectSet(hashedKey, entry);
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
			// ReSharper disable once PossibleMultipleEnumeration - the guard clause should not enumerate, its just checking the reference is not null
			dependentEntitySets.GuardAgainstNull(nameof(dependentEntitySets));

			var database = Connection.GetDatabase();
			var hashedKey = HashKey(key);
			// ReSharper disable once PossibleMultipleEnumeration - the guard clause should not enumerate, its just checking the reference is not null
			var entitySets = dependentEntitySets.ToArray();

			try
			{
				var transaction = database.CreateTransaction();
				foreach (var entitySet in entitySets)
				{
					transaction.SetAddAsync(AddCacheQualifier(entitySet), hashedKey);
				}
				transaction.ObjectSetAsync(hashedKey, new CacheEntry(value, entitySets, slidingExpiration, absoluteExpiration));
				transaction.Execute();
			}
			catch (Exception e)
			{
				OnCachingFailed(e);
			}
		}

		public void InvalidateSets(IEnumerable<string> entitySets)
		{
			// ReSharper disable once PossibleMultipleEnumeration - the guard clause should not enumerate, its just checking the reference is not null
			entitySets.GuardAgainstNull(nameof(entitySets));

			var database = Connection.GetDatabase();

			try
			{
				var keysAndEntitySetsToInvalidate = new HashSet<(string, RedisKey)>();
				// ReSharper disable once PossibleMultipleEnumeration - the guard clause should not enumerate, its just checking the reference is not null
				foreach (var entitySet in entitySets)
				{
					var entitySetKey = AddCacheQualifier(entitySet);
					var keys = database.SetMembers(entitySetKey).Select(v => (v.ToString(), entitySetKey));
					keysAndEntitySetsToInvalidate.UnionWith(keys);
				}

				var transaction = database.CreateTransaction();
				foreach (var (key, entitySetKey) in keysAndEntitySetsToInvalidate)
				{
					var hashedKey = HashKey(key);
					transaction.KeyDeleteAsync(hashedKey);
					transaction.SetRemoveAsync(entitySetKey, hashedKey);
				}
				var committed = transaction.Execute();

				if (!ShouldCollectStatistics || !committed) return;
				
				foreach (var (key, _) in keysAndEntitySetsToInvalidate)
				{
					database.HashIncrement(AddStatsQualifier(HashKey(key)), InvalidationsIdentifier, 1, CommandFlags.FireAndForget);
				}
				
			}
			catch (Exception e)
			{
				OnCachingFailed(e);
			}
		}

		public void InvalidateItem(string key)
		{
			key.GuardAgainstNullOrEmpty(nameof(key));

			var database = Connection.GetDatabase();
			var hashedKey = HashKey(key);

			try
			{
				var entry = database.ObjectGet<CacheEntry>(hashedKey);
				if (entry == null) return;

				var transaction = database.CreateTransaction();
				transaction.KeyDeleteAsync(hashedKey);
				foreach (var set in entry.EntitySets)
				{
					transaction.SetRemoveAsync(AddCacheQualifier(set), hashedKey);
				}
				var committed = transaction.Execute();

				if (!ShouldCollectStatistics || !committed) return;
				
				database.HashIncrement(AddStatsQualifier(hashedKey), InvalidationsIdentifier, 1, CommandFlags.FireAndForget);
			}
			catch (Exception e)
			{
				OnCachingFailed(e);
			}
		}

		// ReSharper disable once BuiltInTypeReferenceStyle
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

		public void Purge()
		{
			var database = Connection.GetDatabase();
			foreach (var endPoint in database.Multiplexer.GetEndPoints())
			{
				database.Multiplexer.GetServer(endPoint).FlushDatabase();
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

		private static bool EntryExpired(CacheEntry entry, DateTimeOffset now) =>
			entry.AbsoluteExpiration < now || (now - entry.LastAccess) > entry.SlidingExpiration;

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
	}

	internal class StatResult
	{
		public HashEntry Query { get; set; }
		public HashEntry[] Stats { get; set; }
	}
}