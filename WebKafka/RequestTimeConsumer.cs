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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace WebKafka
{

    public class RequestTimeConsumer : BackgroundService
    {
        private readonly string brokerList = "localhost:9092";
        private readonly List<string> topics = new List<string> { "weblog" };
        private readonly IConsumer<Ignore, string> kafkaConsumer;

        public RequestTimeConsumer(IConfiguration config)
        {
            
            var consumerConfig = new ConsumerConfig
            {
                GroupId = new Guid().ToString(),
                BootstrapServers = brokerList,
                EnableAutoCommit = true, // để mặc định vì ở dưới đã set EnableAutoOffsetStore = false
                EnableAutoOffsetStore = false//việc này giúp chỉ StoreOffset khi xong nhưng nếu có lỗi sẽ xử lý lại mess dẫn đến có thể bị trùng lặp transaction cần lưu ý khi xử lý với các giao dịch không thể re-try ví dụ như đã gọi API trừ tiền trường hợp này có thể check choắc như nào là tùy
            };
            this.kafkaConsumer = new ConsumerBuilder<Ignore, string>(consumerConfig)
               // Note: All handlers are called on the main .Consume thread.
               .SetErrorHandler((_, e) => { Console.WriteLine($"Error: {e.Reason}"); })
               .SetStatisticsHandler((_, json) => { Console.WriteLine($"Statistics: {json}"); })
               .SetPartitionsAssignedHandler((c, partitions) =>
               {
                   //sự kiện AssignedHandler Partitions cho thằng consumer này


                   // Since a cooperative assignor (CooperativeSticky) has been configured, the
                   // partition assignment is incremental (adds partitions to any existing assignment).


                   //Console.WriteLine(
                   //   "Partitions incrementally assigned: [" +
                   //   string.Join(',', partitions.Select(p => p.Partition.Value)) +
                   //   "], all: [" +
                   //   string.Join(',', c.Assignment.Concat(partitions).Select(p => p.Partition.Value)) +
                   //   "]");



                   // Possibly manually specify start offsets by returning a list of topic/partition/offsets
                   // to assign to, e.g.:
                   // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));

               })
               .SetPartitionsRevokedHandler((c, partitions) =>
               {
                   // Since a cooperative assignor (CooperativeSticky) has been configured, the revoked
                   // assignment is incremental (may remove only some partitions of the current assignment).

                   //var remaining = c.Assignment.Where(atp => partitions.Where(rtp => rtp.TopicPartition == atp).Count() == 0);
                   //Console.WriteLine(
                   //    "Partitions incrementally revoked: [" +
                   //    string.Join(',', partitions.Select(p => p.Partition.Value)) +
                   //    "], remaining: [" +
                   //    string.Join(',', remaining.Select(p => p.Partition.Value)) +
                   //    "]");
               })
               .SetPartitionsLostHandler((c, partitions) =>
               {
                   // The lost partitions handler is called when the consumer detects that it has lost ownership
                   // of its assignment (fallen out of the group).


                   // Console.WriteLine($"Partitions were lost: [{string.Join(", ", partitions)}]");
               })
               .Build();
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            new Thread(() => StartConsumerLoop(stoppingToken)).Start();

            return Task.CompletedTask;
        }

        private void StartConsumerLoop(CancellationToken cancellationToken)
        {
            kafkaConsumer.Subscribe(this.topics);

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = this.kafkaConsumer.Consume(cancellationToken);
                 
                    if (consumeResult.IsPartitionEOF)
                    {
                        Console.WriteLine(
                            $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                        continue;
                    }
                    Debug.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");
                    

                    this.kafkaConsumer.StoreOffset(consumeResult);
                   
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (ConsumeException e)
                {
                    // Consumer errors should generally be ignored (or logged) unless fatal.
                    Console.WriteLine($"Consume error: {e.Error.Reason}");

                    if (e.Error.IsFatal)
                    {
                        // https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#fatal-consumer-errors
                        break;
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Unexpected error: {e}");
                    break;
                }
            }
        }

        public override void Dispose()
        {
            this.kafkaConsumer.Close(); // Commit offsets and leave the group cleanly.
            this.kafkaConsumer.Dispose();

            base.Dispose();
        }
    }
}
