using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using Xunit;

namespace PetTricks.WebApi.Tests;

public class ContainerFixture : IAsyncLifetime
{
    private static int _runningTests;

    private static readonly Lazy<KafkaTestcontainer> KafkaContainer = new(() =>
        new TestcontainersBuilder<KafkaTestcontainer>()
            .WithKafka(new KafkaTestcontainerConfiguration())
            .Build());

    public string NewMoviesTopic { get; } = $"new-movies-{Guid.NewGuid()}";
    public string LatestMediaPositionsTopic { get; } = $"latest-media-positions-{Guid.NewGuid()}";
    
    public static string BootstrapServers => KafkaContainer.Value.BootstrapServers;

    public async Task InitializeAsync()
    {
        Interlocked.Increment(ref _runningTests);
        await KafkaContainer.Value.StartAsync(Timeout());
        await CreateTopic(
            new TopicSpecification { Name = NewMoviesTopic, NumPartitions = 1, },
            new TopicSpecification { Name = LatestMediaPositionsTopic, NumPartitions = 1, });
    }

    public Task DisposeAsync() =>
        Interlocked.Decrement(ref _runningTests) == 0
            ? KafkaContainer.Value.StopAsync(Timeout())
            : Task.CompletedTask;

    private static CancellationToken Timeout() => new CancellationTokenSource(TimeSpan.FromSeconds(60)).Token;
    
    private static async Task CreateTopic(params TopicSpecification[] topicSpecifications)
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = BootstrapServers,
        }).Build();
        await Task.WhenAll(
            topicSpecifications.Select(topicSpecification =>
                CreateTopic(topicSpecification, 3, adminClient)));
    }
    
    private static async Task CreateTopic(TopicSpecification topicSpecification, int attemptsRemaining, IAdminClient adminClient)
    {
        try
        {
            await adminClient.CreateTopicsAsync(new[] { topicSpecification });
        }
        catch (CreateTopicsException e) when (e.Error.Reason.Contains("already exists"))
        {
        }
        catch (KafkaException e) when (e.Error.Code == ErrorCode.Local_TimedOut)
        {
            if (attemptsRemaining == 0) throw;
            await CreateTopic(topicSpecification, attemptsRemaining - 1, adminClient);
        }
    }
}