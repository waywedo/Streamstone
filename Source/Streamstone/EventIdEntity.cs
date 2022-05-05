using System;
using Azure;
using Azure.Data.Tables;

namespace Streamstone
{
    class EventIdEntity : ITableEntity
    {
        public const string RowKeyPrefix = "SS-UID-";

        public EventIdEntity()
        {}

        public EventIdEntity(Partition partition, RecordedEvent @event)
        {
            Event = @event;
            PartitionKey = partition.PartitionKey;
            RowKey = partition.EventIdRowKey(@event.Id);
            Version = @event.Version;
        }

        public int Version { get; set; }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset? Timestamp { get; set; }
        public ETag ETag { get; set; }

        [IgnoreProperty]
        public RecordedEvent Event
        {
            get; set;
        }
    }
}