﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Azure.Data.Tables;
using ExpectedObjects;
using NUnit.Framework;

namespace Streamstone.Scenarios
{
    [TestFixture]
    public class Setting_stream_properties
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
        public async Task When_property_map_is_empty()
        {
            var properties = new Dictionary<string, object>();

            var previous = await Stream.ProvisionAsync(partition);
            var current = await Stream.SetPropertiesAsync(previous, StreamProperties.From(properties));

            Assert.That(current.ETag, Is.Not.EqualTo(previous.ETag));
            StreamProperties.From(properties).ToExpectedObject().ShouldEqual(current.Properties);
        }

        [Test]
        public async Task When_concurrency_conflict()
        {
            var stream = await Stream.ProvisionAsync(partition);
            partition.UpdateStreamEntity();

            Assert.ThrowsAsync<ConcurrencyConflictException>(
                async () => await Stream.SetPropertiesAsync(stream, StreamProperties.None));
        }

        [Test]
        public async Task When_set_successfully()
        {
            var properties = new Dictionary<string, object>
            {
                {"P1", 42},
                {"P2", "42"}
            };

            var stream = await Stream.ProvisionAsync(partition, StreamProperties.From(properties));

            var newProperties = new Dictionary<string, object>
            {
                {"P1", 56},
                {"P2", "56"}
            };

            var newStream = await Stream.SetPropertiesAsync(stream, StreamProperties.From(newProperties));
            StreamProperties.From(newProperties).ToExpectedObject().ShouldEqual(newStream.Properties);

            var storedEntity = partition.RetrieveStreamEntity();
            var storedProperties = storedEntity.Properties;

            StreamProperties.From(newProperties).ToExpectedObject().ShouldEqual(storedProperties);
        }

        [Test]
        public void When_trying_to_set_properties_on_transient_stream()
        {
            var stream = new Stream(partition);

            partition.CaptureContents(contents =>
            {
                Assert.ThrowsAsync<ArgumentException>(
                    async () => await Stream.SetPropertiesAsync(stream, StreamProperties.None));

                contents.AssertNothingChanged();
            });
        }
    }
}