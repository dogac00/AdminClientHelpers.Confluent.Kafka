using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace AdminClient.Extensions
{
    public static class AdminClientExtensions
    {
        public static async Task CreateDefaultTopicAsync(this IAdminClient client, string topic)
        {
            await client.CreateTopicsAsync(new TopicSpecification[]
            {
                new TopicSpecification
                {
                    Name = topic,
                    NumPartitions = 10
                }
            });
        }
    }
}