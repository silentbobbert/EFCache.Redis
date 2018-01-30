using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using StackExchange.Redis;

namespace EFCache.Redis.Tests
{
    [TestClass]
    public class StackExchangeExtensionsTests
    {
        [TestMethod]
        public void Set_should_not_throw_when_caching_null_object()
        {
            var cache = Mock.Of<IDatabase>();
            cache.Set("key", (object)null);
        }
    }
}
