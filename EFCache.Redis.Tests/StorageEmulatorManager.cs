using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;

namespace EFCache.Redis.Tests
{
    public abstract class StorageEmulatorManager : IStorageEmulatorManager
    {
        private readonly string _processName;

        private readonly ProcessStartInfo _processStartInfo;

        protected StorageEmulatorManager(string processName, ProcessStartInfo processStartInfo)
        {
            _processStartInfo = processStartInfo;
            _processName = processName;
        }

        public Process GetProcess() => Process.GetProcessesByName(_processName).FirstOrDefault();

        public bool IsProcessStarted() => GetProcess() != null;

        public void StartProcess(bool waitForExit)
        {
            if (IsProcessStarted()) return;

            using (var process = Process.Start(_processStartInfo))
            {
                if (process != null && waitForExit) process.WaitForExit();
            }
        }

        public void StopProcess()
        {
            var process = GetProcess();
            process?.Kill();
        }
        public static string AssemblyDirectory
        {
            get
            {
                var codeBase = Assembly.GetExecutingAssembly().CodeBase;
                var uri = new UriBuilder(codeBase);
                var path = Uri.UnescapeDataString(uri.Path);
                return Path.GetDirectoryName(path);
            }
        }
    }
}
