using System.Text.Json;
using Confluent.Kafka;

namespace PetTricks.WebApi;

internal class XJsonSerializer<T> : ISerializer<T>
{
    public byte[] Serialize(T data, SerializationContext context) => JsonSerializer.SerializeToUtf8Bytes(data);
}