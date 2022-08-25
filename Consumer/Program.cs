using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;


/// <summary>
///     Demonstrates use of the Consumer client.
/// </summary>
namespace Confluent.Kafka.Consumer
{
    public class Program
    {
        /// <summary>
        ///     In this example
        ///         - offsets are manually committed.
        ///         - no extra thread is created for the Poll (Consume) loop.
        /// </summary>
        public static void Run_Consume(string brokerList, List<string> topics, CancellationToken cancellationToken)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = brokerList,
                GroupId = "csharp-consumer",
                EnableAutoCommit = false,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true,
                // A good introduction to the CooperativeSticky assignor and incremental rebalancing:
                // https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb/
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
            };


            const int commitPeriod = 5;

            // Note: If a key or value deserializer is not set (as is the case below), the 
            // deserializer corresponding to the appropriate type from Confluent.Kafka.Deserializers
            // will be used automatically (where available). The default deserializer for string
            // is UTF8. The default deserializer for Ignore returns null for all input data
            // (including non-null data).
            using (var consumer = new ConsumerBuilder<Ignore, string>(config)
                // Note: All handlers are called on the main .Consume thread.
                .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                .SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}"))
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    // Since a cooperative assignor (CooperativeSticky) has been configured, the
                    // partition assignment is incremental (adds partitions to any existing assignment).
                    Console.WriteLine(
                        "Partitions incrementally assigned: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "], all: [" +
                        string.Join(',', c.Assignment.Concat(partitions).Select(p => p.Partition.Value)) +
                        "]");

                    // Possibly manually specify start offsets by returning a list of topic/partition/offsets
                    // to assign to, e.g.:
                    // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    // Since a cooperative assignor (CooperativeSticky) has been configured, the revoked
                    // assignment is incremental (may remove only some partitions of the current assignment).
                    var remaining = c.Assignment.Where(atp => partitions.Where(rtp => rtp.TopicPartition == atp).Count() == 0);
                    Console.WriteLine(
                        "Partitions incrementally revoked: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "], remaining: [" +
                        string.Join(',', remaining.Select(p => p.Partition.Value)) +
                        "]");
                })
                .SetPartitionsLostHandler((c, partitions) =>
                {
                    // The lost partitions handler is called when the consumer detects that it has lost ownership
                    // of its assignment (fallen out of the group).
                    Console.WriteLine($"Partitions were lost: [{string.Join(", ", partitions)}]");
                })
                .Build())
            {
                consumer.Subscribe(topics);

                try
                {
                    while (true)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume(cancellationToken);

                            if (consumeResult.IsPartitionEOF)
                            {
                                Console.WriteLine(
                                    $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                                continue;
                            }

                            Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");

                            if (consumeResult.Offset % commitPeriod == 0)
                            {
                                // The Commit method sends a "commit offsets" request to the Kafka
                                // cluster and synchronously waits for the response. This is very
                                // slow compared to the rate at which the consumer is capable of
                                // consuming messages. A high performance application will typically
                                // commit offsets relatively infrequently and be designed handle
                                // duplicate messages in the event of failure.
                                try
                                {
                                    consumer.Commit(consumeResult);
                                }
                                catch (KafkaException e)
                                {
                                    Console.WriteLine($"Commit error: {e.Error.Reason}");
                                }
                            }
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Consume error: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Closing consumer.");
                    consumer.Close();
                }
            }
        }

        /// <summary>
        ///     In this example
        ///         - consumer group functionality (i.e. .Subscribe + offset commits) is not used.
        ///         - the consumer is manually assigned to a partition and always starts consumption
        ///           from a specific offset (0).
        /// </summary>
        public static void Run_ManualAssign(string brokerList, List<string> topics, CancellationToken cancellationToken)
        {
            var config = new ConsumerConfig
            {
                // the group.id property must be specified when creating a consumer, even 
                // if you do not intend to use any consumer group functionality.
                GroupId = new Guid().ToString(),
                BootstrapServers = brokerList,
                // partition offsets can be committed to a group even by consumers not
                // subscribed to the group. in this example, auto commit is disabled
                // to prevent this from occurring.
                EnableAutoCommit = true
            };

            using (var consumer =
                new ConsumerBuilder<Ignore, string>(config)
                    .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                    .Build())
            {
                consumer.Assign(topics.Select(topic => new TopicPartitionOffset(topic, 0, Offset.Beginning)).ToList());

                try
                {
                    while (true)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume(cancellationToken);
                            // Note: End of partition notification has not been enabled, so
                            // it is guaranteed that the ConsumeResult instance corresponds
                            // to a Message, and not a PartitionEOF event.
                            Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: ${consumeResult.Message.Value}");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Consume error: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Closing consumer.");
                    consumer.Close();
                }
            }
        }

        public static void Run_Consume_khanhdeptrai(string brokerList, List<string> topics, CancellationToken cancellationToken)
        {
            var config = new ConsumerConfig
            {
                GroupId = new Guid().ToString(),
                BootstrapServers = brokerList,
                EnableAutoCommit = true, // để mặc định vì ở dưới đã set EnableAutoOffsetStore = false
                EnableAutoOffsetStore = false//việc này giúp chỉ StoreOffset khi xong nhưng nếu có lỗi sẽ xử lý lại mess dẫn đến có thể bị trùng lặp transaction cần lưu ý khi xử lý với các giao dịch không thể re-try ví dụ như đã gọi API trừ tiền trường hợp này có thể check choắc như nào là tùy
            };
            using (var consumer = new ConsumerBuilder<Ignore, string>(config)
               // Note: All handlers are called on the main .Consume thread.
               .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
               .SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}"))
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
               .Build())
            {
                consumer.Subscribe(topics);

                try
                {
                    while (true)
                    {

                        var consumeResult = consumer.Consume(cancellationToken);

                        if (consumeResult.IsPartitionEOF)
                        {
                            Console.WriteLine(
                                $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                            continue;
                        }

                        Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");

                        consumer.StoreOffset(consumeResult);
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Closing consumer.");
                    consumer.Close();
                }
            }
        }

        private static void PrintUsage()
            => Console.WriteLine("Usage: .. <subscribe|manual> <broker,broker,..> <topic> [topic..]");

        public static void Main(string[] args)
        {
            var mode = "khanhdeptrai";
            var brokerList = "localhost:9092";
            var topics = new List<string> { "weblog" };

            Console.WriteLine($"Started consumer, Ctrl-C to stop consuming");

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            switch (mode)
            {
                case "subscribe":
                    Run_Consume(brokerList, topics, cts.Token);
                    break;
                case "manual":
                    Run_ManualAssign(brokerList, topics, cts.Token);
                    break;
                case "khanhdeptrai":
                    Run_Consume_khanhdeptrai(brokerList, topics, cts.Token);
                    break;
                default:
                    PrintUsage();
                    break;
            }
        }
    }
}
