using System;
using System.Runtime.Serialization;
using Azure;
using Azure.Data.Tables;

namespace Streamstone
{
    class StreamEntity : ITableEntity
    {
        public const string FixedRowKey = "SS-HEAD";

        public StreamEntity()
        {
            Properties = StreamProperties.None;
        }

        public StreamEntity(Partition partition, ETag etag, int version, StreamProperties properties)
        {
            Partition = partition;
            PartitionKey = partition.PartitionKey;
            RowKey = partition.StreamRowKey();
            ETag = etag;
            Version = version;
            Properties = properties;
        }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset? Timestamp { get; set; }
        public ETag ETag { get; set; }

        public int Version { get; set; }

        [IgnoreDataMember]
        public StreamProperties Properties { get; set; }

        [IgnoreDataMember]
        public Partition Partition { get; set; }

        public static StreamEntity From(TableEntity entity)
        {
            return new StreamEntity
            {
                PartitionKey = entity.PartitionKey,
                RowKey = entity.RowKey,
                ETag = entity.ETag,
                Timestamp = entity.Timestamp,
                Version = (int)entity.GetInt32("Version"),
                Properties = StreamProperties.From(entity)
            };
        }

        public EntityOperation Operation()
        {
            var isTransient = ETag.Equals(null);

            return isTransient ? Insert() : ReplaceOrMerge();

            EntityOperation.Insert Insert() => new EntityOperation.Insert(this);

            EntityOperation ReplaceOrMerge() => ReferenceEquals(Properties, StreamProperties.None)
                ? new EntityOperation.UpdateMerge(this)
                : new EntityOperation.Replace(this);
        }
    }
}