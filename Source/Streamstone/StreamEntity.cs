using Azure;
using Azure.Data.Tables;
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

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

        public int Version { get; set; }
        public StreamProperties Properties { get; set; }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset? Timestamp { get; set; }
        public ETag ETag { get; set; }

        //public override void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        //{
        //    base.ReadEntity(properties, operationContext);
        //    Properties = StreamProperties.ReadEntity(properties);
        //}

        //public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        //{
        //    var result = base.WriteEntity(operationContext);
        //    Properties.WriteTo(result);
        //    return result;
        //}

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

        [IgnoreDataMember]
        public Partition Partition
        {
            get; set;
        }
    }
}