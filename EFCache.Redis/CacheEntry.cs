using System;

namespace EFCache.Redis
{
    [Serializable]
    public class CacheEntry
    {
        public CacheEntry(object value, string[] entitySets, TimeSpan slidingExpiration,
            DateTimeOffset absoluteExpiration)
        {
            Value = value;
            EntitySets = entitySets;
            SlidingExpiration = slidingExpiration;
            AbsoluteExpiration = absoluteExpiration;
            LastAccess = DateTimeOffset.Now;
        }

        public CacheEntry()
        {
        }

        public object Value { get; set; }

        public string[] EntitySets { get; set; }

        public TimeSpan SlidingExpiration { get; set; }

        public DateTimeOffset AbsoluteExpiration { get; set; }

        public DateTimeOffset LastAccess { get; set; }
    }
}
