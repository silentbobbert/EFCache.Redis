using System;
using System.Collections.Generic;

namespace EFCache.Redis {
    public class QueryStatistics
    {
        public string Query { get; set; }
        public string QueryHash { get; set; }
        public IEnumerable<string> QueryParams { get; set; }
        public Int64 Hits { get; set; }
        public Int64 Misses { get; set; }
        public Int64 Invalidations { get; set; }
    }
}