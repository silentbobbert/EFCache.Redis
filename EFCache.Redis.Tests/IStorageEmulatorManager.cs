using System.Diagnostics;

namespace EFCache.Redis.Tests
{
    public interface IStorageEmulatorManager
    {
        Process GetProcess();
        bool IsProcessStarted();
        void StartProcess(bool waitForExit);
        void StopProcess();
    }
}