using StackExchange.Redis;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading.Tasks;

namespace EFCache.Redis
{
    public static class StackExchangeRedisExtensions
    {
        public static T ObjectGet<T>(this IDatabase cache, string key)
        {
            var item = cache.StringGet(key);
            return Deserialize<T>(item);
        }

        public static void ObjectSet<T>(this IDatabase cache, string key, T value) where T : class
        {
            cache.StringSet(key, Serialize(value));
        }

        public static Task<bool> ObjectSetAsync<T>(this IDatabaseAsync cache, string key, T value) where T : class
        {
            return cache.StringSetAsync(key, Serialize(value));
        }

        public static byte[] Serialize<T>(T o) where T : class
        {
            if (o == null) return null;
            var binaryFormatter = new BinaryFormatter();

            using (var memoryStream = new MemoryStream())
            {
                binaryFormatter.Serialize(memoryStream, o);
                var objectDataAsStream = memoryStream.ToArray();
                return objectDataAsStream;
            }
        }

        private static T Deserialize<T>(byte[] stream)
        {
            if (stream == null || !stream.Any()) return default(T);
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