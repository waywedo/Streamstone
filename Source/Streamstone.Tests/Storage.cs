using System;
using System.Collections.Generic;
using System.Linq;
using Azure.Data.Tables;
using ExpectedObjects;
using Streamstone.Utility;

namespace Streamstone
{
    static class Storage
    {
        const string TableName = "Streams";

        public static TableClient SetUp()
        {
            var account = TestStorageAccount();

            return account == "UseDevelopmentStorage=true"
                    ? SetUpDevelopmentStorageTable(account)
                    : SetUpAzureStorageTable(account);
        }

        static TableClient SetUpDevelopmentStorageTable(string account)
        {
            var serviceClient = new TableServiceClient(account);

            var table = serviceClient.GetTableClient(TableName);
            table.Delete();
            table.Create();

            return table;
        }

        static TableClient SetUpAzureStorageTable(string account)
        {
            var serviceClient = new TableServiceClient(account);

            var table = serviceClient.GetTableClient(TableName);
            table.Create();

            var entities = RetrieveAll(table);
            if (entities.Count == 0)
                return table;

            const int maxBatchSize = 100;
            var batches = (int)Math.Ceiling((double)entities.Count / maxBatchSize);
            foreach (var batch in Enumerable.Range(0, batches))
            {
                var operation = new List<TableTransactionAction>();
                var slice = entities.Skip(batch * maxBatchSize).Take(maxBatchSize).ToList();
                slice.ForEach(e => operation.Add(new TableTransactionAction(TableTransactionActionType.Delete, e)));
                table.SubmitTransaction(operation);
            }

            return table;
        }

        static string TestStorageAccount()
        {
            var connectionString = Environment.GetEnvironmentVariable(
                "Streamstone-Test-Storage", EnvironmentVariableTarget.User);

            return connectionString ?? "UseDevelopmentStorage=true";
        }

        public static StreamEntity InsertStreamEntity(this Partition partition, int version = 0)
        {
            var entity = new StreamEntity
            {
                PartitionKey = partition.PartitionKey,
                RowKey = Api.StreamRowKey,
                Version = version
            };

            partition.Table.AddEntity(entity);
            return entity;
        }

        public static StreamEntity UpdateStreamEntity(this Partition partition, int version = 0)
        {
            var entity = RetrieveStreamEntity(partition);
            entity.Version = version;

            partition.Table.UpdateEntity(entity, entity.ETag, TableUpdateMode.Replace);
            return entity;
        }

        public static StreamEntity RetrieveStreamEntity(this Partition partition)
        {
            return StreamEntity.From(partition.Table.GetEntity<TableEntity>(partition.PartitionKey, Api.StreamRowKey));
        }

        public static void InsertEventEntities(this Partition partition, params string[] ids)
        {
            for (int i = 0; i < ids.Length; i++)
            {
                var e = new EventEntity
                {
                    PartitionKey = partition.PartitionKey,
                    RowKey = (i + 1).FormatEventRowKey()
                };

                partition.Table.AddEntity(e);
            }
        }

        public static EventEntity[] RetrieveEventEntities(this Partition partition)
        {
            return partition.RowKeyPrefixQueryAsync<EventEntity>(Api.EventRowKeyPrefix).Result.ToArray();
        }

        public static void InsertEventIdEntities(this Partition partition, params string[] ids)
        {
            foreach (var id in ids)
            {
                var e = new EventIdEntity
                {
                    PartitionKey = partition.PartitionKey,
                    RowKey = id.FormatEventIdRowKey(),
                };

                partition.Table.AddEntity(e);
            }
        }

        public static EventIdEntity[] RetrieveEventIdEntities(this Partition partition)
        {
            return partition.RowKeyPrefixQueryAsync<EventIdEntity>(Api.EventIdRowKeyPrefix).Result.ToArray();
        }

        public static List<TableEntity> RetrieveAll(this Partition partition)
        {
            return partition.Table.Query<TableEntity>(e => e.PartitionKey == partition.PartitionKey).ToList();
        }

        static List<TableEntity> RetrieveAll(TableClient table)
        {
            return table.Query<TableEntity>().ToList();
        }

        public static PartitionContents CaptureContents(this Partition partition, Action<PartitionContents> continueWith)
        {
            return new PartitionContents(partition, continueWith);
        }

        public class PartitionContents
        {
            readonly Partition partition;
            readonly List<TableEntity> captured;

            public PartitionContents(Partition partition, Action<PartitionContents> continueWith)
            {
                this.partition = partition;

                captured = partition.RetrieveAll();
                continueWith(this);
            }

            public void AssertNothingChanged()
            {
                var current = partition.RetrieveAll();
                captured.ToExpectedObject().ShouldMatch(current);
            }
        }
    }
}