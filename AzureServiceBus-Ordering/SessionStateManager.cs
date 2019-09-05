using System;
using System.Collections.Generic;
using System.Text;

namespace AzureServiceBus_Ordering
{
    public class SessionStateManager
    {
        public SessionStateManager()
        {
            LastProcessedCount = 0;
            DeferredList = new Dictionary<int, long>();
        }
        public int LastProcessedCount { get; set; }
        public Dictionary<int, long> DeferredList { get; set; }
    }
}
