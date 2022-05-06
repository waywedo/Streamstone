using System.Threading.Tasks;
using Azure.Data.Tables;
using Streamstone;

namespace Example.Scenarios
{
    public class S11_Sharding_streams : Scenario
    {
        readonly string[] pool =
        {
            "UseDevelopmentStorage=true",
            "UseDevelopmentStorage=true" // pretend this is some other account
        };

        public override async Task RunAsync()
        {
            var partition1 = Resolve("shard-test-1");
            var partition2 = Resolve("shard-test-2");

            await Stream.ProvisionAsync(partition1);
            await Stream.ProvisionAsync(partition2);
        }

        Partition Resolve(string stream)
        {
            var account = pool[Shard.Resolve(stream, pool.Length)];
            var serviceClient = new TableServiceClient(account);
            var table = serviceClient.GetTableClient(Table.Name);
            return new Partition(table, $"{Partition.Key}_{stream}");
        }
    }
}
