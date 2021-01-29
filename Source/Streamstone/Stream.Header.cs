﻿using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamstone
{
    /// <summary>
    /// Represents an event stream. Instances of this class enapsulate stream header information such as version, etag,  metadata, etc;
    /// while static methods are used to manipulate stream.
    /// </summary>
    public sealed partial class Stream
    {
        /// <summary>
        /// Restores a <see cref="Stream"/> instance from particular etag, version and optional properties.
        /// If properties is <c>null</c> the stream header will be merged, otherwise replaced.
        /// </summary>
        /// <param name="partition">
        /// The partition in which a stream resides.
        /// </param>
        /// <param name="etag">
        /// The latest etag
        /// </param>
        /// <param name="version">
        /// The version of the stream corresponding to <paramref name="etag"/>
        /// </param>
        /// <param name="properties">
        /// The additional properties for this stream.
        /// </param>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="partition"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="etag"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ArgumentException">
        ///     If <paramref name="etag"/> resolves to an empty <c>string</c>
        /// </exception>
        /// <exception cref="ArgumentException">
        ///     If <paramref name="version"/> is less than <c>0</c>
        /// </exception>
        public static Stream From(Partition partition, string etag, int version, StreamProperties properties = null)
        {
            Requires.NotNull(partition, nameof(partition));
            Requires.NotNullOrEmpty(etag, nameof(etag));
            Requires.GreaterThanOrEqualToZero(version, nameof(version));

            return new Stream(partition, etag, version, properties ?? StreamProperties.None);
        }

        /// <summary>
        /// The additional properties (metadata) of this stream
        /// </summary>
        public readonly StreamProperties Properties;

        /// <summary>
        /// The partition in which this stream resides.
        /// </summary>
        public readonly Partition Partition;
        
        /// <summary>
        /// The latest etag
        /// </summary>
        public readonly string ETag;
        
        /// <summary>
        /// The version of the stream. Sequential, monotonically increasing, no gaps.
        /// </summary>
        public readonly int Version;

        /// <summary>
        /// Constructs a new <see cref="Stream"/> instance which doesn't have any additional properties.
        /// </summary>
        /// <param name="partition">
        /// The partition in which this stream will reside. 
        /// </param>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="partition"/> is <c>null</c>
        /// </exception>
        public Stream(Partition partition) 
            : this(partition, StreamProperties.None)
        {}

        /// <summary>
        /// Constructs a new <see cref="Stream"/> instance with the given additional properties.
        /// </summary>
        /// <param name="partition">
        /// The partition in which this stream will reside. 
        /// </param>
        /// <param name="properties">
        /// The additional properties for this stream.
        /// </param>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="partition"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="properties"/> is <c>null</c>
        /// </exception>
        public Stream(Partition partition, StreamProperties properties)
        {
            Requires.NotNull(partition, nameof(partition));
            Requires.NotNull(properties, nameof(properties));

            Partition = partition;
            Properties = properties;
        }

        internal Stream(Partition partition, string etag, int version, StreamProperties properties)
        {
            Partition = partition;
            ETag = etag;
            Version = version;
            Properties = properties;
        }

        /// <summary>
        /// Gets a value indicating whether this stream header represents a transient stream.
        /// </summary>
        /// <value>
        /// <c>true</c> if this stream header was newed; otherwise, <c>false</c>.
        /// </value>
        public bool IsTransient => ETag == null;

        /// <summary>
        /// Gets a value indicating whether this stream header represents a persistent stream.
        /// </summary>
        /// <value>
        /// <c>true</c> if this stream header has been obtained from storage; otherwise, <c>false</c>.
        /// </value>
        public bool IsPersistent => !IsTransient;

        static Stream From(Partition partition, StreamEntity entity) => 
            new Stream(partition, entity.ETag, entity.Version, entity.Properties);

        StreamEntity Entity() => 
            new StreamEntity(Partition, ETag, Version, Properties);

        IEnumerable<RecordedEvent> Record(EventData[] events)
        {
            var lastVersion = events[0].Version ?? Version + 1;
            if (lastVersion <= Version)
                throw new InvalidOperationException(
                    $"The specified version '{lastVersion}' of first event " +
                    $"should be greater than stream version '{Version}'");

            RecordedEvent last = null;
            for (var i = 0; i < events.Length; i++)
            {
                var @event = events[i];

                var recorded = @event.Record(Partition, @event.Version ?? lastVersion);
                lastVersion = recorded.Version;
                lastVersion++;
                
                if (last != null && recorded.Version <= last.Version)
                    throw new InvalidOperationException(
                        $"The specified version '{recorded.Version}' of event [{i}] " +
                        $"should be greater than version '{last.Version}' of preceding event");

                last = recorded;
                yield return recorded;
            }
        }
    }
}
