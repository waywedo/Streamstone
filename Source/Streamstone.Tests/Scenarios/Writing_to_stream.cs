﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Azure;
using Azure.Data.Tables;
using ExpectedObjects;
using NUnit.Framework;

namespace Streamstone.Scenarios
{
    [TestFixture]
    public class Writing_to_stream
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
        [TestCase(true, false, false, Description = "Only Header changed. Concurrent update of stream properties")]
        [TestCase(true, true, false, Description = "Header + Event with the same version exists. Concurrent append")]
        [TestCase(true, false, true, Description = "Header + ID exists. Concurrent append")]
        [TestCase(true, true, true, Description = "Header + Event + ID. Concurrent append")]
        [TestCase(false, true, false, Description = "Only Event with the same version exists. Degenerate case (corruption or manual edit)")]
        [TestCase(false, true, true, Description = "Event + ID. Degenerate case (corruption or manual edit)")]
        public async Task When_write_conflict(bool streamHeaderChanged, bool eventEntityExists, bool idEntityExists)
        {
            var stream = await Stream.ProvisionAsync(partition, default);

            if (streamHeaderChanged)
                partition.UpdateStreamEntity();

            if (eventEntityExists)
                partition.InsertEventEntities("e123");

            if (idEntityExists)
                partition.InsertEventIdEntities("e123");

            var @event = CreateEvent("e123");

            partition.CaptureContents(contents =>
            {
                Assert.ThrowsAsync<ConcurrencyConflictException>(
                    async () => await Stream.WriteAsync(stream, default, @event));

                contents.AssertNothingChanged();
            });
        }

        [Test]
        public async Task When_writing_together_with_creating_stream_and_stream_already_exists()
        {
            await Stream.ProvisionAsync(partition, default);

            partition.CaptureContents(contents =>
            {
                Assert.ThrowsAsync<ConcurrencyConflictException>(
                    async () => await Stream.WriteAsync(new Stream(partition), default, CreateEvent()));

                contents.AssertNothingChanged();
            });
        }

        [Test]
        public void When_writing_duplicate_event()
        {
            var stream = new Stream(partition);

            partition.InsertEventIdEntities("e1", "e2");
            partition.CaptureContents(contents =>
            {
                var duplicate = CreateEvent("e2");

                Assert.ThrowsAsync<DuplicateEventException>(
                    async () => await Stream.WriteAsync(stream, default, CreateEvent("e3"), duplicate));

                contents.AssertNothingChanged();
            });
        }

        [Test]
        public async Task When_successfully_written_events_to_an_existing_stream()
        {
            var stream = new Stream(partition);

            EventData[] events = { CreateEvent("e1"), CreateEvent("e2") };
            var result = await Stream.WriteAsync(stream, default, events);

            AssertModifiedStream(stream, result, version: 2);
            AssertStreamEntity(version: 2);

            var storedEvents = result.Events;
            Assert.That(storedEvents.Length, Is.EqualTo(2));

            AssertRecordedEvent(1, events[0], storedEvents[0]);
            AssertRecordedEvent(2, events[1], storedEvents[1]);

            var eventEntities = partition.RetrieveEventEntities();
            Assert.That(eventEntities.Length, Is.EqualTo(2));

            AssertEventEntity(1, eventEntities[0]);
            AssertEventEntity(2, eventEntities[1]);

            var eventIdEntities = partition.RetrieveEventIdEntities();
            Assert.That(eventIdEntities.Length, Is.EqualTo(2));

            AssertEventIdEntity("e1", 1, eventIdEntities[0]);
            AssertEventIdEntity("e2", 2, eventIdEntities[1]);

            Assert.That(partition.RetrieveAll().Count,
                Is.EqualTo(eventEntities.Length + eventIdEntities.Length + 1));
        }

        [Test]
        public async Task When_writing_to_nonexisting_stream()
        {
            EventData[] events = { CreateEvent("e1"), CreateEvent("e2") };
            var result = await Stream.WriteAsync(new Stream(partition), default, events);

            AssertNewStream(result, version: 2);
            AssertStreamEntity(version: 2);

            var storedEvents = result.Events;
            Assert.That(storedEvents.Length, Is.EqualTo(2));

            AssertRecordedEvent(1, events[0], storedEvents[0]);
            AssertRecordedEvent(2, events[1], storedEvents[1]);

            var eventEntities = partition.RetrieveEventEntities();
            Assert.That(eventEntities.Length, Is.EqualTo(2));

            AssertEventEntity(1, eventEntities[0]);
            AssertEventEntity(2, eventEntities[1]);

            var eventIdEntities = partition.RetrieveEventIdEntities();
            Assert.That(eventIdEntities.Length, Is.EqualTo(2));

            AssertEventIdEntity("e1", 1, eventIdEntities[0]);
            AssertEventIdEntity("e2", 2, eventIdEntities[1]);

            Assert.That(partition.RetrieveAll().Count,
                Is.EqualTo(eventEntities.Length + eventIdEntities.Length + 1));
        }

        [Test]
        public async Task When_writing_events_without_id()
        {
            var stream = new Stream(partition);

            EventData[] events = { CreateEvent(), CreateEvent() };
            var result = await Stream.WriteAsync(stream, default, events);

            AssertModifiedStream(stream, result, version: 2);
            AssertStreamEntity(version: 2);

            var storedEvents = result.Events;
            Assert.That(storedEvents.Length, Is.EqualTo(2));

            AssertRecordedEvent(1, events[0], storedEvents[0]);
            AssertRecordedEvent(2, events[1], storedEvents[1]);

            var eventEntities = partition.RetrieveEventEntities();
            Assert.That(eventEntities.Length, Is.EqualTo(2));

            AssertEventEntity(1, eventEntities[0]);
            AssertEventEntity(2, eventEntities[1]);

            var eventIdEntities = partition.RetrieveEventIdEntities();
            Assert.That(eventIdEntities.Length, Is.EqualTo(0));

            Assert.That(partition.RetrieveAll().Count,
                Is.EqualTo(eventEntities.Length + 1));
        }

        [Test]
        public async Task When_writing_to_nonexisting_stream_along_with_stream_properties()
        {
            var properties = new
            {
                Created = DateTimeOffset.Now,
                Active = true
            };

            var stream = new Stream(partition, StreamProperties.From(properties));

            EventData[] events = { CreateEvent("e1"), CreateEvent("e2") };
            var result = await Stream.WriteAsync(stream, default, events);

            AssertNewStream(result, 2, properties);
            AssertStreamEntity(2, properties);

            var storedEvents = result.Events;
            Assert.That(storedEvents.Length, Is.EqualTo(2));

            AssertRecordedEvent(1, events[0], storedEvents[0]);
            AssertRecordedEvent(2, events[1], storedEvents[1]);

            var eventEntities = partition.RetrieveEventEntities();
            Assert.That(eventEntities.Length, Is.EqualTo(2));

            AssertEventEntity(1, eventEntities[0]);
            AssertEventEntity(2, eventEntities[1]);

            var eventIdEntities = partition.RetrieveEventIdEntities();
            Assert.That(eventIdEntities.Length, Is.EqualTo(2));

            AssertEventIdEntity("e1", 1, eventIdEntities[0]);
            AssertEventIdEntity("e2", 2, eventIdEntities[1]);

            Assert.That(partition.RetrieveAll().Count,
                Is.EqualTo(eventEntities.Length + eventIdEntities.Length + 1));
        }

        [Test]
        public async Task When_writing_over_WATS_max_batch_size_limit()
        {
            var stream = new Stream(partition);

            var events = Enumerable
                .Range(1, Api.AzureMaxBatchSize + 1)
                .Select(i => CreateEvent())
                .ToArray();

            var result = await Stream.WriteAsync(stream, default, events);

            AssertModifiedStream(stream, result, version: events.Length);
            AssertStreamEntity(version: events.Length);

            var storedEvents = result.Events;
            Assert.That(storedEvents.Length, Is.EqualTo(events.Length));

            var eventEntities = partition.RetrieveEventEntities();
            Assert.That(eventEntities.Length, Is.EqualTo(events.Length));
        }

        [Test]
        public async Task When_writing_using_expected_version()
        {
            var expectedVersion = 0;

            await Stream.WriteAsync(partition, expectedVersion, default,
                CreateEvent("e1"), CreateEvent("e2"));

            expectedVersion = 2;

            await Stream.WriteAsync(partition, expectedVersion, default,
                CreateEvent("e3"), CreateEvent("e4"));

            var eventEntities = partition.RetrieveEventEntities();
            Assert.That(eventEntities.Length, Is.EqualTo(4));
        }

        [Test]
        public async Task When_writing_using_expected_version_and_stream_was_changed()
        {
            var expectedVersion = 0;

            await Stream.WriteAsync(partition, expectedVersion, default,
                                    CreateEvent("e1"), CreateEvent("e2"));

            expectedVersion = 0;

            Assert.ThrowsAsync<ConcurrencyConflictException>(async () => await
                Stream.WriteAsync(partition, expectedVersion, default, CreateEvent("e1"), CreateEvent("e2")));
        }

        [Test]
        public async Task When_writing_to_existing_stream_null_stream_properties()
        {
            var properties = new
            {
                Property1 = "Foo",
                Property2 = 42
            };

            var stream = new Stream(partition, StreamProperties.From(properties));
            var result = await Stream.WriteAsync(stream, default, CreateEvent("e1"), CreateEvent("e2"));
            stream = result.Stream;

            AssertNewStream(result, 2, properties);
            AssertStreamEntity(2, properties);

            var restored = Stream.From(partition, stream.ETag, stream.Version, properties: null);
            result = await Stream.WriteAsync(restored, default, CreateEvent("e3"));

            AssertNewStream(result, 3, new { });
            AssertStreamEntity(3, properties);
        }

        [Test]
        public async Task When_writing_to_existing_stream_non_null_stream_properties()
        {
            var properties = new
            {
                Property1 = "Foo",
                Property2 = 42
            };

            var stream = new Stream(partition, StreamProperties.From(properties));
            var result = await Stream.WriteAsync(stream, default, CreateEvent("e1"), CreateEvent("e2"));
            stream = result.Stream;

            AssertNewStream(result, 2, properties);
            AssertStreamEntity(2, properties);

            var replaced = new
            {
                Property1 = "Bar"
            };

            var restored = Stream.From(partition, stream.ETag, stream.Version, StreamProperties.From(replaced));
            result = await Stream.WriteAsync(restored, default, CreateEvent("e3"));

            AssertNewStream(result, 3, replaced);
            AssertStreamEntity(3, replaced);
        }

        void AssertNewStream(StreamWriteResult actual, long version, object properties = null)
        {
            var newStream = actual.Stream;
            var newStreamEntity = partition.RetrieveStreamEntity();

            var expectedStream = CreateStream(version, newStreamEntity.ETag, properties);
            expectedStream.ToExpectedObject().ShouldEqual(newStream);
        }

        void AssertModifiedStream(Stream previous, StreamWriteResult actual, long version)
        {
            var actualStream = actual.Stream;
            var actualStreamEntity = partition.RetrieveStreamEntity();

            Assert.That(actualStream.ETag, Is.Not.EqualTo(previous.ETag));

            var expectedStream = CreateStream(version, actualStreamEntity.ETag);
            expectedStream.ToExpectedObject().ShouldEqual(actualStream);
        }

        Stream CreateStream(long version, ETag etag, object properties = null)
        {
            var props = properties != null
                ? StreamProperties.From(properties)
                : StreamProperties.None;

            return new Stream(partition, etag, version, props);
        }

        void AssertStreamEntity(long version = 0, object properties = null)
        {
            var newStreamEntity = partition.RetrieveStreamEntity();

            var expectedEntity = new
            {
                RowKey = Api.StreamRowKey,
                Properties = properties != null
                    ? StreamProperties.From(properties)
                    : StreamProperties.None,
                Version = version,
            };

            expectedEntity.ToExpectedObject().ShouldMatch(newStreamEntity);
        }

        void AssertRecordedEvent(long version, EventData source, RecordedEvent actual)
        {
            var expected = source.Record(partition, version);
            expected.ToExpectedObject().ShouldMatch(actual);
        }

        static void AssertEventEntity(long version, EventEntity actual)
        {
            var expected = new
            {
                RowKey = version.FormatEventRowKey(),
                Properties = EventProperties.From(new Dictionary<string, object>
                {
                    {"Type", "StreamChanged"},
                    {"Data", "{}"}
                }),
                Version = version
            };

            expected.ToExpectedObject().ShouldMatch(actual);
            Assert.That(actual.ETag.ToString, Is.Not.Empty);
        }

        static void AssertEventIdEntity(string id, long version, EventIdEntity actual)
        {
            var expected = new
            {
                RowKey = id.FormatEventIdRowKey(),
                Version = version,
            };

            expected.ToExpectedObject().ShouldMatch(actual);
        }

        static EventData CreateEvent(string id = null)
        {
            var properties = new Dictionary<string, object>
            {
                {"Type", "StreamChanged"},
                {"Data", "{}"}
            };

            var eventId = id != null
                ? EventId.From(id)
                : EventId.None;

            return new EventData(eventId, EventProperties.From(properties));
        }

        class TestEntity : ITableEntity
        {
            public string PartitionKey { get; set; }
            public string RowKey { get; set; }
            public DateTimeOffset? Timestamp { get; set; }
            public ETag ETag { get; set; }
        }
    }
}