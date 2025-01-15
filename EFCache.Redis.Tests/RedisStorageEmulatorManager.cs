using System;
using System.Diagnostics;
using System.IO;

namespace EFCache.Redis.Tests
{
    public class RedisStorageEmulatorManager : StorageEmulatorManager
    {
        private static readonly Lazy<RedisStorageEmulatorManager>  Lazy = new Lazy<RedisStorageEmulatorManager>();

        public static RedisStorageEmulatorManager Instance => Lazy.Value;

        public RedisStorageEmulatorManager() : base("redis-server", new ProcessStartInfo
        {
            FileName = Path.Combine(GetLibFolder(),"redis-server.exe"),
            RedirectStandardOutput = true,
            WorkingDirectory = GetLibFolder() + @"\",
            UseShellExecute = false
        }) {}

        private static string GetLibFolder()
        {
            var pathToLibFolder = Path.Combine(AssemblyDirectory, "lib");
            return pathToLibFolder;
        }
    }
}
