using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace WebKafka
{
    public class KafkaClientHandle : IDisposable
    {
        IProducer<byte[], byte[]> kafkaProducer;

        public KafkaClientHandle(IConfiguration config)
        {
            string brokerList = "localhost:9092";
            string topicName = "weblog";
            string ClientId = "KPItest";

            var producerconfig = new ProducerConfig
            {
                BootstrapServers = brokerList,
                ClientId = ClientId,
            };

            this.kafkaProducer = new ProducerBuilder<byte[], byte[]>(producerconfig).Build();
        }

        public Handle Handle { get => this.kafkaProducer.Handle; }

        public void Dispose()
        {
            // Block until all outstanding produce requests have completed (with or
            // without error).
            kafkaProducer.Flush();
            kafkaProducer.Dispose();
        }
    }
}
