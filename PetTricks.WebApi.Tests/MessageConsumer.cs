using System;
using System.Collections.Generic;
using System.Threading;
using Confluent.Kafka;

namespace PetTricks.WebApi.Tests;

internal static class MessageConsumer
{
    public static IEnumerable<ConsumeResult<TKey, TValue>> ConsumeMessages<TKey, TValue>(string topicName, CancellationToken cancellationToken)
    {
        using var consumer = new ConsumerBuilder<TKey, TValue>(new ConsumerConfig
        {
            BootstrapServers = ContainerFixture.BootstrapServers,
            GroupId = "test",
            AutoOffsetReset = AutoOffsetReset.Earliest,
        }).SetKeyDeserializer(new JsonDeserializer<TKey>()).SetValueDeserializer(new JsonDeserializer<TValue>()).Build();
        try
        {
            consumer.Subscribe(topicName);
            while (!cancellationToken.IsCancellationRequested)
            {
                var consumeResult = consumer.Consume(cancellationToken);
                yield return consumeResult;
            }
        }
        finally
        {
            consumer.Close();
        }
    }
}