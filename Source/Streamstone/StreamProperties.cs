﻿using System;
using System.Collections.Generic;
using System.Linq;
using Azure.Data.Tables;

namespace Streamstone
{
    /// <summary>
    /// Represents the collection of named stream properties (metadata)
    /// </summary>
    public sealed class StreamProperties : PropertyMap
    {
        /// <summary>
        /// An empty collection of stream properties
        /// </summary>
        public static readonly StreamProperties None = new();

        StreamProperties()
        {}

        StreamProperties(IDictionary<string, object> properties)
            : base(properties)
        { }

        public override void CopyFrom(TableEntity entity)
        {
            Clear();
            foreach (var property in Build(entity))
            {
                Add(property.Key, property.Value);
            }
        }

        internal static StreamProperties ReadEntity(IDictionary<string, object> properties)
        {
            Requires.NotNull(properties, nameof(properties));
            return Build(properties);
        }

        /// <summary>
        /// Creates new instance of <see cref="StreamProperties"/> class using given dictionary of entity properties
        /// </summary>
        /// <param name="properties">The properties.</param>
        /// <returns>New instance of <see cref="StreamProperties"/> class</returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="properties"/> is <c>null</c>
        /// </exception>
        public static StreamProperties From(IDictionary<string, object> properties)
        {
            return Build(properties);
        }

        /// <summary>
        /// Creates new instance of <see cref="StreamProperties"/> class using public properties of a given object.
        /// All public properties should be of WATS compatible type..
        /// </summary>
        /// <param name="obj">The properties.</param>
        /// <returns>New instance of <see cref="StreamProperties"/> class</returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="obj"/> is <c>null</c>
        /// </exception>
        /// <exception cref="NotSupportedException">
        ///     If <paramref name="obj"/> has properties of WATS incompatible type
        /// </exception>
        public static StreamProperties From(object obj)
        {
            Requires.NotNull(obj, nameof(obj));
            return Build(ToDictionary(obj));
        }

        /// <summary>
        /// Creates new instance of <see cref="StreamProperties"/> class using public properties of a given table entity.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns>New instance of <see cref="StreamProperties"/> class</returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="entity"/> is <c>null</c>
        /// </exception>
        public static StreamProperties From(TableEntity entity)
        {
            Requires.NotNull(entity, nameof(entity));
            return Build(entity);
        }

        static StreamProperties Build(IDictionary<string, object> properties)
        {
            var filtered = properties
                .Where(x => !IsReserved(x.Key))
                .ToDictionary(p => p.Key, p => p.Value);

            return filtered.Count != 0 ? new StreamProperties(filtered) : None;
        }

        static bool IsReserved(string propertyName)
        {
            switch (propertyName)
            {
                case "PartitionKey":
                case "RowKey":
                case "ETag":
                case "odata.etag":
                case "Timestamp":
                case "Version":
                    return true;
                default:
                    return false;
            }
        }
    }
}