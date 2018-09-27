namespace EFCache.Redis {
	public class CachedEntitySetLock : ICachedEntitySetLock
	{
		private readonly IRedlock _redlock;
		public CachedEntitySetLock(IRedlock redlock)
		{
			_redlock = redlock;
		}
		public void Unlock()
		{
			_redlock.Dispose();
		}
	}
}