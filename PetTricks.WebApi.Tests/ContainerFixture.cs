using System;
using System.Threading;
using System.Threading.Tasks;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using Xunit;

namespace PetTricks.WebApi.Tests;

public class ContainerFixture : IAsyncLifetime
{
    private static readonly Lazy<KafkaTestcontainer> KafkaContainer = new(() =>
        new TestcontainersBuilder<KafkaTestcontainer>()
            .WithKafka(new KafkaTestcontainerConfiguration())
            .Build());

    private static int runningTests;

    public static string BootstrapServers => KafkaContainer.Value.BootstrapServers;

    public Task InitializeAsync()
    {
        Interlocked.Increment(ref runningTests);
        return KafkaContainer.Value.StartAsync(Timeout());
    }

    public Task DisposeAsync() =>
        Interlocked.Decrement(ref runningTests) == 0
            ? KafkaContainer.Value.StopAsync(Timeout())
            : Task.CompletedTask;

    private static CancellationToken Timeout() => new CancellationTokenSource(TimeSpan.FromSeconds(60)).Token;
}