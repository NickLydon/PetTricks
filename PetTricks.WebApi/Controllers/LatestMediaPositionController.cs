using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace PetTricks.WebApi.Controllers;

[ApiController]
[Route("[controller]")]
public class LatestMediaPositionController : ControllerBase
{
    private readonly IOptions<KafkaConfig> _options;

    public LatestMediaPositionController(IOptions<KafkaConfig> options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }
    
    [HttpPut("{userId:guid}/")]
    public async Task Put([FromRoute] Guid userId, [FromBody] LatestMediaPosition latestMediaPosition, CancellationToken cancellationToken = default)
    {
        using var producer = new ProducerBuilder<Guid, LatestMediaPosition>(new ProducerConfig
        {
            BootstrapServers = _options.Value.BootstrapServers,
        }).SetKeySerializer(new XJsonSerializer<Guid>()).SetValueSerializer(new XJsonSerializer<LatestMediaPosition>()).Build();
        await producer.ProduceAsync(_options.Value.LatestMediaPositionsTopic, new Message<Guid, LatestMediaPosition>
        {
            Key = userId,
            Value = latestMediaPosition
        }, cancellationToken: cancellationToken);
    }
}