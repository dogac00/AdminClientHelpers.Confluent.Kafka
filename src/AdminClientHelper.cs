using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace AdminClientHelpers.Confluent.Kafka
{
    public static class AdminClientHelper
    {
        private static IAdminClient CreateAdminClient(string bootstrapServers)
        {
            return new AdminClientBuilder(new Dictionary<string, string>()
            {
                {"bootstrap.servers", bootstrapServers}
            }).Build();
        }
        
        public static async Task CreateDefaultTopicAsync(string bootstrapServers,
            string topic)
        {
            await CreateTopicAsync(bootstrapServers, topic, 10);
        }

        public static async Task CreateTopicAsync(string bootstrapServers,
            string topic,
            int partitions)
        {
            using var adminClient = CreateAdminClient(bootstrapServers);

            await adminClient.CreateTopicAsync(topic, partitions);
        }

        public static async Task DeleteTopicIfExistsAsync(string bootstrapServers,
            string topic)
        {
            await DeleteTopicsIfExistsAsync(bootstrapServers, new[] {topic});
        }
        
        public static async Task DeleteTopicsIfExistsAsync(string bootstrapServers,
            IEnumerable<string> topics)
        {
            using var adminClient = CreateAdminClient(bootstrapServers);
            
            foreach (var topic in topics)
            {
                var exists = TopicExists(bootstrapServers, topic);

                if (!exists)
                {
                    continue;
                }

                await adminClient.DeleteTopicAsync(topic);
            }
        }

        public static int GetPartitionCount(string bootstrapServers,
            string topic)
        {
            using var adminClient = CreateAdminClient(bootstrapServers);

            return adminClient.GetPartitionCount(topic);
        }
        
        public static async Task DeleteTopicAsync(string bootstrapServers,
            string topic)
        {
            await DeleteTopicsAsync(bootstrapServers, new[] {topic});
        }

        public static async Task DeleteTopicsAsync(string bootstrapServers,
            IEnumerable<string> topics)
        {
            using var adminClient = CreateAdminClient(bootstrapServers);

            await adminClient.DeleteTopicsAsync(topics);
        }

        public static List<string> ListTopics(string bootstrapServers)
        {
            using var adminClient = CreateAdminClient(bootstrapServers);

            return adminClient.ListTopics();
        }

        public static bool TopicExists(string bootstrapServers,
            string topic)
        {
            using var adminClient = CreateAdminClient(bootstrapServers);

            return adminClient.TopicExists(topic);
        }
    }
}