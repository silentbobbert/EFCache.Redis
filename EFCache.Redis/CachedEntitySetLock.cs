using System;
using RedLockNet;

namespace EFCache.Redis {
	public class CachedEntitySetLock : ICachedEntitySetLock
	{
		private readonly IRedLock _redlock;
		public CachedEntitySetLock(IRedLock redlock)
		{
			_redlock = redlock;
		}
		public void Unlock()
		{
			_redlock.Dispose();
		}
	}
}