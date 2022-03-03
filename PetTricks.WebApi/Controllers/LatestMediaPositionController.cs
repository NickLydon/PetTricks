using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace PetTricks.WebApi.Controllers;

[ApiController]
[Route("[controller]")]
public class LatestMediaPositionController : ControllerBase
{
    private readonly IOptions<KafkaConfig> _options;
    private readonly KafkaDependentProducer<Guid, LatestMediaPosition> _producer;

    public LatestMediaPositionController(IOptions<KafkaConfig> options, KafkaDependentProducer<Guid, LatestMediaPosition> producer)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _producer = producer ?? throw new ArgumentNullException(nameof(producer));
    }
    
    [HttpPut("{userId:guid}/")]
    public async Task Put([FromRoute] Guid userId, [FromBody] LatestMediaPosition latestMediaPosition, CancellationToken cancellationToken = default)
    {
        await _producer.ProduceAsync(_options.Value.LatestMediaPositionsTopic, new Message<Guid, LatestMediaPosition>
        {
            Key = userId,
            Value = latestMediaPosition
        }, cancellationToken: cancellationToken);
    }
}