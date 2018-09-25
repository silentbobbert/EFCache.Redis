using System;
using System.Collections.Generic;

namespace EFCache.Redis {
    public class QueryStatistics
    {
        public string Query { get; set; }
        public string QueryHash { get; set; }
        public IEnumerable<string> QueryParams { get; set; }
        public long Hits { get; set; }
        public long Misses { get; set; }
        public long Invalidations { get; set; }
    }
}