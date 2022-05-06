﻿using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using NUnit.Framework;

namespace Streamstone.Scenarios
{
    [TestFixture]
    public class Custom_stream_properties
    {
        [Test]
        public void When_passing_property_with_reserved_name()
        {
            var reserved = ReservedStreamProperties()
                .ToDictionary(p => p, _ => (object)42);

            var properties = StreamProperties.From(reserved);

            Assert.That(properties.Count, Is.EqualTo(0),
                "Should skip all properties with reserved names, such as RowKey, Id, etc");
        }

        static IEnumerable<string> ReservedStreamProperties()
        {
            return typeof(StreamEntity)
                    .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                    .Where(p => !p.GetCustomAttributes<IgnoreDataMemberAttribute>(true).Any() && p.Name != "Properties")
                    .Select(p => p.Name);
        }
    }
}