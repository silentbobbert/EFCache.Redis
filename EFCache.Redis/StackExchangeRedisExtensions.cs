using System;
using StackExchange.Redis;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;

namespace EFCache.Redis
{
    public static class StackExchangeRedisExtensions
    {
        public static T Get<T>(this IDatabase cache, string key)
        {
            var item = cache.StringGet(key);
            return Deserialize<T>(item);
        }

        public static void Set<T>(this IDatabase cache, string key, T value, TimeSpan expiry) where T : class
        {
            cache.StringSet(key, Serialize<T>(value), expiry);
        }

        static byte[] Serialize<T>(T o) where T : class
        {
            if (o == null)
            {
                return null;
            }

            var binaryFormatter = new BinaryFormatter();

            using (var memoryStream = new MemoryStream())
            {
                binaryFormatter.Serialize(memoryStream, o);
                var objectDataAsStream = memoryStream.ToArray();
                return objectDataAsStream;
            }
        }

        static T Deserialize<T>(byte[] stream)
        {
            if (stream == null || !stream.Any())
            {
                return default(T);
            }

            var binaryFormatter = new BinaryFormatter();
            using (var memoryStream = new MemoryStream(stream))
            {
                memoryStream.Seek(0, SeekOrigin.Begin);
                var result = (T)binaryFormatter.Deserialize(memoryStream);
                return result;
            }
        }
    }
}