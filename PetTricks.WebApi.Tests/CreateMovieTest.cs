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

public class CreateMovieTest : IClassFixture<ContainerFixture>
{
    private readonly CancellationToken _totalTestTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(180)).Token;

    private CancellationToken IndividualOperationTimeout => CancellationTokenSource.CreateLinkedTokenSource(
        _totalTestTimeout,
        new CancellationTokenSource(TimeSpan.FromSeconds(30)).Token
    ).Token;

    private readonly WebApplicationFactory<Program> _webApplicationFactory;
    private readonly string _newMoviesTopic;

    public CreateMovieTest(ContainerFixture containerFixture)
    {
        _newMoviesTopic = containerFixture.NewMoviesTopic;
        _webApplicationFactory = new WebApplicationFactory<Program>()
            .WithWebHostBuilder(builder =>
            {
                builder.ConfigureAppConfiguration((_, configurationBuilder) =>
                {
                    configurationBuilder.AddInMemoryCollection(new Dictionary<string, string>()
                    {
                        [$"{nameof(KafkaConfig)}:{nameof(KafkaConfig.BootstrapServers)}"] = ContainerFixture.BootstrapServers,
                        [$"{nameof(KafkaConfig)}:{nameof(KafkaConfig.NewMoviesTopic)}"] = _newMoviesTopic,
                    });
                });
            });
    }

    [Fact]
    public async Task ShouldPublishANewMovieToTheTopic()
    {
        var newMovie = new Media(Title: "One Flew Over the Cuckoo's Nest", Genres: new[] { "Drama" }, TimeSpan.FromMinutes(133));

        using var client = _webApplicationFactory.CreateClient();

        var response = await client.PutAsJsonAsync("/movie", newMovie, cancellationToken: IndividualOperationTimeout);
        response.EnsureSuccessStatusCode();
        var key = await response.Content.ReadFromJsonAsync<Guid>(cancellationToken: IndividualOperationTimeout);

        var topicMessages = MessageConsumer
            .ConsumeMessages<Guid, Media>(_newMoviesTopic, IndividualOperationTimeout)
            .Select(m => (m.Message.Key, m.Message.Value));
        Assert.Contains((key, newMovie), topicMessages);
    }
}