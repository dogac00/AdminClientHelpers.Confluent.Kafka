using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace AdminClient.Extensions
{
    public static class AdminClientHelper
    {
        public static async Task CreateDefaultTopicAsync(string bootstrapServers,
            string topic)
        {
            using var adminClient = new AdminClientBuilder(new Dictionary<string, string>()
            {
                {"bootstrap.servers", bootstrapServers}
            }).Build();

            await adminClient.CreateDefaultTopicAsync(topic);
        }
        
        public static async Task DeleteTopicAsync(string bootstrapServers,
            string topic)
        {
            using var adminClient = new AdminClientBuilder(new Dictionary<string, string>()
            {
                {"bootstrap.servers", bootstrapServers}
            }).Build();

            await adminClient.DeleteTopicAsync(topic);
        }
        
        public static async Task DeleteTopicsAsync(string bootstrapServers,
            IEnumerable<string> topics)
        {
            using var adminClient = new AdminClientBuilder(new Dictionary<string, string>()
            {
                {"bootstrap.servers", bootstrapServers}
            }).Build();

            await adminClient.DeleteTopicsAsync(topics);
        }
        
        public static List<string> ListTopics(string bootstrapServers)
        {
            using var adminClient = new AdminClientBuilder(new Dictionary<string, string>()
            {
                {"bootstrap.servers", bootstrapServers}
            }).Build();

            return adminClient.ListTopics();
        }
        
        public static bool TopicExists(string bootstrapServers, 
            string topic)
        {
            using var adminClient = new AdminClientBuilder(new Dictionary<string, string>()
            {
                {"bootstrap.servers", bootstrapServers}
            }).Build();

            return adminClient.TopicExists(topic);
        }
    }
}