﻿using System;
using System.Linq;
using System.Threading.Tasks;

using Streamstone;

namespace Example.Scenarios
{
    public class S05_Read_from_stream : Scenario
    {
        public override async Task RunAsync()
        {
            await Prepare();
            await ReadSlice();
            await ReadAll();
        }

        async Task Prepare()
        {
            var events = Enumerable
                .Range(1, 10)
                .Select(Event)
                .ToArray();

            var existent = await Stream.TryOpenAsync(Partition, default);
            var stream = existent.Found ? existent.Stream : new Stream(Partition);
            await Stream.WriteAsync(stream, default, events);
        }

        async Task ReadSlice()
        {
            Console.WriteLine("Reading single slice from specified start version and using specified slice size");

            var slice = await Stream.ReadAsync<EventEntity>(Partition, default, startVersion: 2, sliceSize: 2);
            foreach (var @event in slice.Events)
                Console.WriteLine("{0}: {1}-{2}", @event.Version, @event.Type, @event.Data);

            Console.WriteLine();
        }

        async Task ReadAll()
        {
            Console.WriteLine("Reading all events in a stream");
            Console.WriteLine("If slice size is > than WATS limit, continuation token will be managed automatically");

            StreamSlice<EventEntity> slice;
            var nextSliceStart = 1L;

            do
            {
                slice = await Stream.ReadAsync<EventEntity>(Partition, default, nextSliceStart, sliceSize: 1);

                foreach (var @event in slice.Events)
                    Console.WriteLine("{0}:{1} {2}-{3}", @event.Id, @event.Version, @event.Type, @event.Data);

                nextSliceStart = slice.HasEvents
                    ? slice.Events.Last().Version + 1
                    : -1;
            }
            while (!slice.IsEndOfStream);
        }

        static EventData Event(int id)
        {
            var properties = new
            {
                Id = id,
                Type = "<type>",
                Data = "{some}"
            };

            return new EventData(EventId.From(id.ToString()), EventProperties.From(properties));
        }

        class EventEntity
        {
            public int Id { get; set; }
            public string Type { get; set; }
            public string Data { get; set; }
            public long Version { get; set; }
        }
    }
}
