using System;
using System.Collections.Generic;
using System.Security.Principal;
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
        
        public static async Task DeleteTopicAsync(this IAdminClient client, string topic)
        {
            await client.DeleteTopicsAsync(new[] {topic});
        }
        
        public static List<string> ListTopics(this IAdminClient client)
        {
            var metadata = client.GetMetadata(TimeSpan.FromSeconds(3));

            var metadataTopics = metadata.Topics;
            var topicList = new List<string>();
            
            foreach (var metadataTopic in metadataTopics)
            {
                var name = metadataTopic.Topic;
                
                topicList.Add(name);
            }

            return topicList;
        }
        
        public static bool TopicExists(this IAdminClient client, string topic)
        {
            var topics = ListTopics(client);

            foreach (var topicName in topics)
            {
                if (topicName == topic)
                {
                    return true;
                }
            }

            return false;
        }
    }
}