using System.Threading.Tasks;
using Azure.Data.Tables;
using NUnit.Framework;

namespace Streamstone.Scenarios
{
    [TestFixture]
    public class Checking_stream_exists
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
        public async Task When_stream_does_exists()
        {
            await Stream.ProvisionAsync(partition, default);
            Assert.That(await Stream.ExistsAsync(partition, default), Is.True);
        }

        [Test]
        public async Task When_stream_does_not_exist()
        {
            Assert.That(await Stream.ExistsAsync(partition, default), Is.False);
        }
    }
}