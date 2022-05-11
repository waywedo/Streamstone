﻿using System;
using System.Threading.Tasks;

using Streamstone;

namespace Example.Scenarios
{
    public class S03_Try_open_stream_for_writing : Scenario
    {
        public override async Task RunAsync()
        {
            await TryOpenNonExistentStream();
            await TryOpenExistentStream();
        }

        async Task TryOpenNonExistentStream()
        {
            var existent = await Stream.TryOpenAsync(Partition, default);

            Console.WriteLine("Trying to open non-existent stream. Found: {0}, Stream: {1}",
                              existent.Found, existent.Stream == null ? "<null>" : "?!?");
        }

        async Task TryOpenExistentStream()
        {
            await Stream.ProvisionAsync(Partition, default);

            var existent = await Stream.TryOpenAsync(Partition, default);

            Console.WriteLine("Trying to open existent stream. Found: {0}, Stream: {1}\r\nEtag - {2}, Version - {3}",
                               existent.Found, existent.Stream, existent.Stream.ETag, existent.Stream.Version);
        }
    }
}
