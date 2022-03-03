using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace PetTricks.WebApi.Controllers;

[ApiController]
[Route("[controller]")]
public class UserController : ControllerBase
{
    private readonly IOptions<KafkaConfig> _options;
    private readonly KafkaDependentProducer<Guid, User> _producer;

    public UserController(IOptions<KafkaConfig> options, KafkaDependentProducer<Guid, User> producer)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _producer = producer ?? throw new ArgumentNullException(nameof(producer));
    }
    
    [HttpPut("{userId:guid}/")]
    public async Task Put([FromRoute] Guid userId, [FromBody] User user, CancellationToken cancellationToken = default)
    {
        await _producer.ProduceAsync(_options.Value.UsersTopic, new Message<Guid, User>
        {
            Key = userId,
            Value = user
        }, cancellationToken: cancellationToken);
    }
}