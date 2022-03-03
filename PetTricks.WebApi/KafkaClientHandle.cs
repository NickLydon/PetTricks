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
using Microsoft.Extensions.Options;

namespace PetTricks.WebApi;

public sealed class KafkaClientHandle : IDisposable
{
    private readonly IProducer<byte[], byte[]> _kafkaProducer;

    public KafkaClientHandle(IOptions<KafkaConfig> kafkaConfig)
    {
        if (kafkaConfig == null) throw new ArgumentNullException(nameof(kafkaConfig));
        _kafkaProducer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig
        {
            BootstrapServers = kafkaConfig.Value.BootstrapServers
        }).Build();
    }

    public Handle Handle => _kafkaProducer.Handle;

    public void Dispose()
    {
        _kafkaProducer.Flush();
        _kafkaProducer.Dispose();
    }
}