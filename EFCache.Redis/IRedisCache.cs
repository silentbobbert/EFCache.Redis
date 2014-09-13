using System;

namespace EFCache.Redis
{
    public interface IRedisCache : ICache
    {
        Int64 Count { get; }
        void Purge();
    }
}
