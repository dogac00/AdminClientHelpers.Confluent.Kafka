using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace AdminClientHelpers.Confluent.Kafka
{
    public static class AdminClientExtensions
    {
        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);
        
        public static async Task CreateTopicAsync(this IAdminClient client, string topic, int partitions)
        {
            await client.CreateTopicsAsync(new[]
            {
                new TopicSpecification
                {
                    Name = topic,
                    NumPartitions = partitions
                }
            }, new CreateTopicsOptions
            {
                OperationTimeout = Timeout,
                RequestTimeout = Timeout
            });
        }

        public static async Task CreateDefaultTopicAsync(this IAdminClient client, string topic)
        {
            await CreateTopicAsync(client, topic, 10);
        }
        
        public static async Task DeleteTopicAsync(this IAdminClient client, string topic)
        {
            await client.DeleteTopicsAsync(new[] {topic}, new DeleteTopicsOptions
            {
                OperationTimeout = Timeout,
                RequestTimeout = Timeout
            });
        }
        
        public static List<string> ListTopics(this IAdminClient client)
        {
            return client.GetMetadata(Timeout)
                .Topics
                .Select(t => t.Topic)
                .ToList();
        }
        
        public static bool TopicExists(this IAdminClient client, string topic)
        {
            return ListTopics(client)
                .Contains(topic);
        }
        
        public static TopicMetadata? GetTopicMetadata(this IAdminClient client, string topic)
        {
            return client.GetMetadata(Timeout)
                .Topics
                .FirstOrDefault(t => t.Topic == topic);
        }
        
        public static int GetPartitionCount(this IAdminClient client, string topic)
        {
            var topicMetadata = GetTopicMetadata(client, topic);

            if (topicMetadata == null)
            {
                throw new Exception("Unknown topic.");
            }

            return topicMetadata.Partitions.Count;
        }
    }
}