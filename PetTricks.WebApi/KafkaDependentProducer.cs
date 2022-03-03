// Copyright 2020 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.
//
// Modified by Nicholas Lydon

using Confluent.Kafka;

namespace PetTricks.WebApi;

/// <summary>
///     Leverages the injected KafkaClientHandle instance to allow
///     Confluent.Kafka.Message{K,V}s to be produced to Kafka.
/// </summary>
public class KafkaDependentProducer<TK,TV>
{
    private readonly IProducer<TK, TV> _kafkaHandle;

    public KafkaDependentProducer(KafkaClientHandle handle)
    {
        _kafkaHandle = new DependentProducerBuilder<TK, TV>(handle.Handle)
            .SetKeySerializer(new XJsonSerializer<TK>())
            .SetValueSerializer(new XJsonSerializer<TV>())
            .Build();
    }

    /// <summary>
    ///     Asychronously produce a message and expose delivery information
    ///     via the returned Task. Use this method of producing if you would
    ///     like to await the result before flow of execution continues.
    /// </summary>
    public Task ProduceAsync(string topic, Message<TK, TV> message, CancellationToken cancellationToken = default)
        => _kafkaHandle.ProduceAsync(topic, message, cancellationToken);

    /// <summary>
    ///     Asynchronously produce a message and expose delivery information
    ///     via the provided callback function. Use this method of producing
    ///     if you would like flow of execution to continue immediately, and
    ///     handle delivery information out-of-band.
    /// </summary>
    public void Produce(string topic, Message<TK, TV> message, Action<DeliveryReport<TK, TV>>? deliveryHandler = null)
        => _kafkaHandle.Produce(topic, message, deliveryHandler);

    public void Flush(TimeSpan timeout)
        => _kafkaHandle.Flush(timeout);
}