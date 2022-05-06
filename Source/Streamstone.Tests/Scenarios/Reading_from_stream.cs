﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Azure;
using Azure.Data.Tables;

using NUnit.Framework;

namespace Streamstone.Scenarios
{
    [TestFixture]
    public class Reading_from_stream
    {
        Partition partition;
        TableClient table;

        [SetUp]
        public void SetUp()
        {
            table = Storage.SetUp();
            partition = new Partition(table, "test");
        }

        [Test]
        public void When_start_version_is_less_than_1()
        {
            Assert.ThrowsAsync<ArgumentOutOfRangeException>(
                async () => await Stream.ReadAsync<TestEventEntity>(partition, 0));

            Assert.ThrowsAsync<ArgumentOutOfRangeException>(
                async () => await Stream.ReadAsync<TestEventEntity>(partition, -1));
        }

        [Test]
        public void When_stream_doesnt_exist()
        {
            Assert.ThrowsAsync<StreamNotFoundException>(async () => await Stream.ReadAsync<TestEventEntity>(partition));
        }

        [Test]
        public async Task When_stream_is_empty()
        {
            await Stream.ProvisionAsync(partition);

            var slice = await Stream.ReadAsync<TestEventEntity>(partition);

            Assert.That(slice.IsEndOfStream, Is.True);
            Assert.That(slice.Events.Length, Is.EqualTo(0));
        }

        [Test]
        public async Task When_version_is_greater_than_current_version_of_stream()
        {
            EventData[] events = { CreateEvent("e1"), CreateEvent("e2") };
            await Stream.WriteAsync(new Stream(partition), events);

            var slice = await Stream.ReadAsync<TestEventEntity>(partition, events.Length + 1);

            Assert.That(slice.IsEndOfStream, Is.True);
            Assert.That(slice.Events.Length, Is.EqualTo(0));
        }

        [Test]
        public async Task When_all_events_fit_to_single_slice()
        {
            EventData[] events = { CreateEvent("e1"), CreateEvent("e2") };
            await Stream.WriteAsync(new Stream(partition), events);

            var slice = await Stream.ReadAsync<TestEventEntity>(partition, sliceSize: 2);

            Assert.That(slice.IsEndOfStream, Is.True);
            Assert.That(slice.Events.Length, Is.EqualTo(2));
        }

        [Test]
        public async Task When_all_events_do_not_fit_single_slice()
        {
            EventData[] events = { CreateEvent("e1"), CreateEvent("e2") };
            await Stream.WriteAsync(new Stream(partition), events);

            var slice = await Stream.ReadAsync<TestRecordedEventEntity>(partition, sliceSize: 1);

            Assert.That(slice.IsEndOfStream, Is.False);
            Assert.That(slice.Events.Length, Is.EqualTo(1));
            Assert.That(slice.Events[0].Version, Is.EqualTo(1));

            slice = await Stream.ReadAsync<TestRecordedEventEntity>(partition, slice.Events.Last().Version + 1);

            Assert.That(slice.IsEndOfStream, Is.True);
            Assert.That(slice.Events.Length, Is.EqualTo(1));
            Assert.That(slice.Events[0].Version, Is.EqualTo(2));
        }

        [Test, Explicit]
        public async Task When_slice_size_is_bigger_than_azure_storage_page_limit()
        {
            const int sizeOverTheAzureLimit = 1500;
            const int numberOfWriteBatches = 50;

            var stream = await Stream.ProvisionAsync(partition);

            foreach (var batch in Enumerable.Range(1, numberOfWriteBatches))
            {
                var events = Enumerable
                    .Range(1, sizeOverTheAzureLimit / numberOfWriteBatches)
                    .Select(i => CreateEvent(batch + "e" + i))
                    .ToArray();

                var result = await Stream.WriteAsync(stream, events);
                stream = result.Stream;
            }

            var slice = await Stream.ReadAsync<TestRecordedEventEntity>(partition, sliceSize: 1500);

            Assert.That(slice.IsEndOfStream, Is.True);
            Assert.That(slice.Events.Length, Is.EqualTo(1500));
        }

        [Test]
        public async Task When_requested_result_is_EventProperties()
        {
            EventData[] events = { CreateEvent("e1"), CreateEvent("e2") };
            await Stream.WriteAsync(new Stream(partition), events);

            var slice = await Stream.ReadAsync(partition, sliceSize: 2);

            Assert.That(slice.IsEndOfStream, Is.True);
            Assert.That(slice.Events.Length, Is.EqualTo(2));

            var e = slice.Events[0];
            Assert.That(e["Type"], Is.EqualTo("StreamChanged"));
            Assert.That(e["Data"], Is.EqualTo("{}"));
        }

        [Test]
        public async Task When_requested_result_is_DynamicTableEntity()
        {
            EventData[] events = { CreateEvent("e1"), CreateEvent("e2") };
            await Stream.WriteAsync(new Stream(partition), events);

            var slice = await Stream.ReadAsync<TableEntity>(partition, sliceSize: 2);

            Assert.That(slice.IsEndOfStream, Is.True);
            Assert.That(slice.Events.Length, Is.EqualTo(2));

            var e = slice.Events[0];
            AssertSystemProperties(e);

            Assert.That(e.GetString("Id"), Is.EqualTo("e1"));
            Assert.That(e.GetString("Type"), Is.EqualTo("StreamChanged"));
            Assert.That(e.GetString("Data"), Is.EqualTo("{}"));
        }

        [Test]
        public async Task When_requested_result_is_custom_TableEntity()
        {
            EventData[] events = { CreateEvent("e1"), CreateEvent("e2") };
            await Stream.WriteAsync(new Stream(partition), events);

            var slice = await Stream.ReadAsync<CustomTableEntity>(partition, sliceSize: 2);

            Assert.That(slice.IsEndOfStream, Is.True);
            Assert.That(slice.Events.Length, Is.EqualTo(2));

            var e = slice.Events[0];
            AssertSystemProperties(e);

            Assert.That(e.Id, Is.EqualTo("e1"));
            Assert.That(e.Type, Is.EqualTo("StreamChanged"));
            Assert.That(e.Data, Is.EqualTo("{}"));
        }

        void AssertSystemProperties(ITableEntity e)
        {
            Assert.That(e.PartitionKey, Is.EqualTo(partition.Key));
            Assert.That(e.RowKey, Is.EqualTo(partition.EventVersionRowKey(1)));
            Assert.That(e.ETag, Is.Not.Null.Or.Empty);
            Assert.That(e.Timestamp, Is.Not.EqualTo(DateTimeOffset.MinValue));
        }

        class CustomTableEntity : ITableEntity
        {
            public string PartitionKey { get; set; }
            public string RowKey { get; set; }
            public DateTimeOffset? Timestamp { get; set; }
            public ETag ETag { get; set; }

            public string Id { get; set; }
            public string Type { get; set; }
            public string Data { get; set; }
        }

        static EventData CreateEvent(string id)
        {
            var properties = new Dictionary<string, object>
            {
                {"Id",   id},
                {"Type", "StreamChanged"},
                {"Data", "{}"}
            };

            return new EventData(EventId.From(id), EventProperties.From(properties));
        }
    }
}