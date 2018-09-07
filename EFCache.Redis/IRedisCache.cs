using System;
using System.Collections;
using System.Collections.Generic;

namespace EFCache.Redis
{
    public interface IRedisCache : ILockableCache
    {
        Int64 Count { get; }
        void Purge();
        event EventHandler<RedisCacheException> CachingFailed;
        bool ShouldCollectStatistics { get; set; }
        IEnumerable<QueryStatistics> GetStatistics();
    }
}
