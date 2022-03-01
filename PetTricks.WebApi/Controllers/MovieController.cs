using System.Text.Json;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace PetTricks.WebApi.Controllers;

[ApiController]
[Route("[controller]")]
public class MovieController : ControllerBase
{
    private readonly IOptions<KafkaConfig> _options;

    public MovieController(IOptions<KafkaConfig> options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    [HttpPut(Name = "CreateMovie")]
    public async Task<Guid> Put([FromBody] Media media, CancellationToken cancellationToken = default)
    {
        using var producer = new ProducerBuilder<Guid, Media>(new ProducerConfig
        {
            BootstrapServers = _options.Value.BootstrapServers,
        }).SetKeySerializer(new XJsonSerializer<Guid>()).SetValueSerializer(new XJsonSerializer<Media>()).Build();
        var key = Guid.NewGuid();
        await producer.ProduceAsync("new-movies", new Message<Guid, Media>
        {
            Key = key,
            Value = media
        }, cancellationToken: cancellationToken);
        return key;
    }
}

internal class XJsonSerializer<T> : ISerializer<T>
{
    public byte[] Serialize(T data, SerializationContext context) => JsonSerializer.SerializeToUtf8Bytes(data);
}