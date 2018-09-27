using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EFCache.Redis
{
	/// <summary>
	/// An object that holds a mutex lock in the cache for an entity set
	/// </summary>
	public class LockedEntitySet : ILockedEntitySet
	{
		/// <summary>
		/// The locked entity set
		/// </summary>
		public string EntitySet { get; set; }

		/// <summary>
		/// The lock that can be released
		/// </summary>
		public IDisposable Lock { get; set; }

		public void Dispose()
		{
			Lock?.Dispose();
		}
	}


}
