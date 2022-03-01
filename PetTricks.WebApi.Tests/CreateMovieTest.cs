using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace PetTricks.WebApi.Tests;

public class CreateMovieTest : IClassFixture<ContainerFixture>
{
    private readonly CancellationToken _totalTestTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(180)).Token;

    private CancellationToken IndividualOperationTimeout => CancellationTokenSource.CreateLinkedTokenSource(
        _totalTestTimeout,
        new CancellationTokenSource(TimeSpan.FromSeconds(30)).Token
    ).Token;

    private readonly TopicSpecification _topicSpecification = new()
    {
        Name = "new-movies",
        NumPartitions = 1,
    };

    private readonly WebApplicationFactory<Program> _webApplicationFactory = new WebApplicationFactory<Program>()
        .WithWebHostBuilder(builder =>
        {
            builder.ConfigureAppConfiguration((_, configurationBuilder) =>
            {
                configurationBuilder.AddInMemoryCollection(new Dictionary<string, string>()
                {
                    [$"{nameof(KafkaConfig)}:{nameof(KafkaConfig.BootstrapServers)}"] =
                        ContainerFixture.BootstrapServers
                });
            });
        });

    public CreateMovieTest(ContainerFixture _)
    {
    }

    [Fact]
    public async Task ShouldPublishANewMovieToTheTopic()
    {
        await CreateTopic(_topicSpecification);
        var newMovie = new Media(Title: "One Flew Over the Cuckoo's Nest", Genres: new[] { "Drama" }, TimeSpan.FromMinutes(133));

        using var client = _webApplicationFactory.CreateClient();

        var response = await client.PutAsJsonAsync("/movie", newMovie, cancellationToken: IndividualOperationTimeout);
        response.EnsureSuccessStatusCode();
        var key = await response.Content.ReadFromJsonAsync<Guid>(cancellationToken: IndividualOperationTimeout);

        var topicMessages = ConsumeMessages(IndividualOperationTimeout).Select(m => (m.Message.Key, m.Message.Value));
        Assert.Contains((key, newMovie), topicMessages);
    }

    private static async Task CreateTopic(TopicSpecification topicSpecification, int attemptsRemaining = 3)
    {
        try
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = ContainerFixture.BootstrapServers,
            }).Build();
            await adminClient.CreateTopicsAsync(new[] { topicSpecification });
        }
        catch (CreateTopicsException e) when (e.Error.Reason.Contains("already exists"))
        {
        }
        catch (KafkaException e) when (e.Error.Code == ErrorCode.Local_TimedOut)
        {
            if (attemptsRemaining == 0) throw;
            await CreateTopic(topicSpecification, attemptsRemaining - 1);
        }
    }

    private IEnumerable<ConsumeResult<Guid, Media>> ConsumeMessages(CancellationToken cancellationToken)
    {
        using var consumer = new ConsumerBuilder<Guid, Media>(new ConsumerConfig
        {
            BootstrapServers = ContainerFixture.BootstrapServers,
            GroupId = "test",
            AutoOffsetReset = AutoOffsetReset.Earliest,
        }).SetKeyDeserializer(new JsonDeserializer<Guid>()).SetValueDeserializer(new JsonDeserializer<Media>()).Build();
        try
        {
            consumer.Subscribe(_topicSpecification.Name);
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


internal class JsonDeserializer<T> : IDeserializer<T>
{
    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) =>
        isNull
            ? throw new Exception($"Attempted to deserialize null {typeof(T).Name} instance")
            : JsonSerializer.Deserialize<T>(data) ?? throw new Exception($"Attempted to deserialize null {typeof(T).Name} instance");
}