using System;

namespace EFCache.Redis
{
    public class RedisCacheException : Exception
    {
        public RedisCacheException()
        {
        }

        public RedisCacheException(string message) : base(message)
        {
        }

        public RedisCacheException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}