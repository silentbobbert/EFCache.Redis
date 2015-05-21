using System;
using StackExchange.Redis;

namespace EFCache.Redis
{
    public interface IRedisCache : ICache
    {
        Int64 Count { get; }
        void Purge();
        event EventHandler<RedisConnectionException> OnConnectionError;
    }
}
