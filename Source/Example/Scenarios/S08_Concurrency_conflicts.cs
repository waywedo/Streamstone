using System;
using System.Threading.Tasks;

using Streamstone;

namespace Example.Scenarios
{
    public class S08_Concurrency_conflicts : Scenario
    {
        public override async Task RunAsync()
        {
            await SimultaneousProvisioning();
            await SimultaneousWriting();
            await SimultaneousSettingOfStreamMetadata();
            await SequentiallyWritingToStreamIgnoringReturnedStreamHeader();
        }

        async Task SimultaneousProvisioning()
        {
            await Stream.ProvisionAsync(Partition, default);

            try
            {
                await Stream.ProvisionAsync(Partition, default);
            }
            catch (ConcurrencyConflictException)
            {
                Console.WriteLine("Simultaneously provisioning stream in a same partition will lead to ConcurrencyConflictException");
            }
        }

        async Task SimultaneousWriting()
        {
            var a = await Stream.OpenAsync(Partition, default);
            var b = await Stream.OpenAsync(Partition, default);

            await Stream.WriteAsync(a, default, new EventData(EventId.From("123")));

            try
            {
                await Stream.WriteAsync(b, default, new EventData(EventId.From("456")));
            }
            catch (ConcurrencyConflictException)
            {
                Console.WriteLine("Simultaneously writing to the same version of stream will lead to ConcurrencyConflictException");
            }
        }

        async Task SimultaneousSettingOfStreamMetadata()
        {
            var a = await Stream.OpenAsync(Partition, default);
            var b = await Stream.OpenAsync(Partition, default);

            await Stream.SetPropertiesAsync(a, StreamProperties.From(new { A = 42 }), default);

            try
            {
                await Stream.SetPropertiesAsync(b, StreamProperties.From(new { A = 56 }), default);
            }
            catch (ConcurrencyConflictException)
            {
                Console.WriteLine("Simultaneously setting metadata using the same version of stream will lead to ConcurrencyConflictException");
            }
        }

        async Task SequentiallyWritingToStreamIgnoringReturnedStreamHeader()
        {
            var stream = await Stream.OpenAsync(Partition, default);

            var result = await Stream.WriteAsync(stream, default, new EventData(EventId.From("AAA")));

            // a new stream header is returned after each write, it contains new Etag
            // and it should be used for subsequent operations
            // stream = result.Stream;

            try
            {
                await Stream.WriteAsync(stream, default, new EventData(EventId.From("BBB")));
            }
            catch (ConcurrencyConflictException)
            {
                Console.WriteLine("Ignoring new stream (header) returned after each Write() operation will lead to ConcurrencyConflictException on subsequent write operation");
            }
        }
    }
}
