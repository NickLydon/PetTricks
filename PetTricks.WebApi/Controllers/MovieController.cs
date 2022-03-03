using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace PetTricks.WebApi.Controllers;

[ApiController]
[Route("[controller]")]
public class MovieController : ControllerBase
{
    private readonly IOptions<KafkaConfig> _options;
    private readonly KafkaDependentProducer<Guid, Media> _producer;

    public MovieController(IOptions<KafkaConfig> options, KafkaDependentProducer<Guid, Media> producer)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _producer = producer ?? throw new ArgumentNullException(nameof(producer));
    }

    [HttpPut(Name = "CreateMovie")]
    public async Task<Guid> Put([FromBody] Media media, CancellationToken cancellationToken = default)
    {
        var key = Guid.NewGuid();
        await _producer.ProduceAsync(_options.Value.NewMoviesTopic, new Message<Guid, Media>
        {
            Key = key,
            Value = media
        }, cancellationToken: cancellationToken);
        return key;
    }
}