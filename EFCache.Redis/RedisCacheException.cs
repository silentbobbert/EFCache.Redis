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


    [Serializable]
    public class LockTimeoutException : Exception
    {
        public LockTimeoutException() { }
        public LockTimeoutException(string message) : base(message) { }
        public LockTimeoutException(string message, Exception inner) : base(message, inner) { }
        protected LockTimeoutException(
          System.Runtime.Serialization.SerializationInfo info,
          System.Runtime.Serialization.StreamingContext context) : base(info, context) { }

        public int ThreadId { get; internal set; }
    }


}