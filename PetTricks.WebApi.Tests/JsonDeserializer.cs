using System;
using System.Text.Json;
using Confluent.Kafka;

namespace PetTricks.WebApi.Tests;

internal class JsonDeserializer<T> : IDeserializer<T>
{
    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) =>
        isNull
            ? throw new Exception($"Attempted to deserialize null {typeof(T).Name} instance")
            : JsonSerializer.Deserialize<T>(data) ?? throw new Exception($"Attempted to deserialize null {typeof(T).Name} instance");
}