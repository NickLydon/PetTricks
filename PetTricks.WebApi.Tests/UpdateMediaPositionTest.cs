using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace PetTricks.WebApi.Tests;

public class UpdateMediaPositionTest : IClassFixture<ContainerFixture>
{
    private readonly CancellationToken _totalTestTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(180)).Token;

    private CancellationToken IndividualOperationTimeout => CancellationTokenSource.CreateLinkedTokenSource(
        _totalTestTimeout,
        new CancellationTokenSource(TimeSpan.FromSeconds(30)).Token
    ).Token;

    private readonly WebApplicationFactory<Program> _webApplicationFactory;
    private readonly string _latestMediaPositionsTopic;

    public UpdateMediaPositionTest(ContainerFixture containerFixture)
    {
        _latestMediaPositionsTopic = containerFixture.LatestMediaPositionsTopic;
        _webApplicationFactory = new WebApplicationFactory<Program>()
            .WithWebHostBuilder(builder =>
            {
                builder.ConfigureAppConfiguration((_, configurationBuilder) =>
                {
                    configurationBuilder.AddInMemoryCollection(new Dictionary<string, string>()
                    {
                        [$"{nameof(KafkaConfig)}:{nameof(KafkaConfig.BootstrapServers)}"] = ContainerFixture.BootstrapServers,
                        [$"{nameof(KafkaConfig)}:{nameof(KafkaConfig.LatestMediaPositionsTopic)}"] = _latestMediaPositionsTopic,
                    });
                });
            });
    }

    [Fact]
    public async Task ShouldPublishANewMovieToTheTopic()
    {
        var userId = Guid.NewGuid();
        var positionUpdate = new LatestMediaPosition(Guid.NewGuid(), TimeSpan.Parse("00:43:11"));
        
        using var client = _webApplicationFactory.CreateClient();

        var response = await client.PutAsJsonAsync($"/latestmediaposition/{userId}", positionUpdate, cancellationToken: IndividualOperationTimeout);
        response.EnsureSuccessStatusCode();

        var topicMessages = MessageConsumer
            .ConsumeMessages<Guid, LatestMediaPosition>(_latestMediaPositionsTopic, IndividualOperationTimeout)
            .Select(m => (m.Message.Key, m.Message.Value));
        Assert.Contains((userId, positionUpdate), topicMessages);
    }
}