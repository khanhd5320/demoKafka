using Confluent.Kafka;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;


namespace Confluent.Kafka.Producer
{
    public class Program
    {
        public static async Task Main(string[] args)
        {

            string brokerList = "localhost:9092";
            string topicName = "weblog";
            string ClientId = "KPItest";

            var config = new ProducerConfig { BootstrapServers = brokerList,
                ClientId = ClientId,
            };

            using (var producer = new ProducerBuilder<string, string>(config).Build())
            {
                Console.WriteLine("\n-----------------------------------------------------------------------");
                Console.WriteLine($"Producer {producer.Name} producing on topic {topicName}.");
                var cancelled = false;
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // prevent the process from terminating.
                    cancelled = true;
                };

                while (!cancelled)
                {
                    Console.Write("> ");

                    string text;
                    try
                    {
                        text = Console.ReadLine();
                    }
                    catch (IOException)
                    {
                        // IO exception is thrown when ConsoleCancelEventArgs.Cancel == true.
                        break;
                    }
                    if (text == null)
                    {
                        // Console returned null before 
                        // the CancelKeyPress was treated
                        break;
                    }

                    string key = null;
                    string val = text;

                    // split line if both key and value specified.
                    int index = text.IndexOf(" ");
                    if (index != -1)
                    {
                        key = text.Substring(0, index);
                        val = text.Substring(index + 1);
                    }

                    try
                    {
                        // Note: Awaiting the asynchronous produce request below prevents flow of execution
                        // from proceeding until the acknowledgement from the broker is received (at the 
                        // expense of low throughput).

                        var t = producer.ProduceAsync(topicName, new Message<string, string> { Key = key, Value = text });
                        t.ContinueWith(task =>
                        {
                            if (task.IsFaulted)
                            {
                                throw task.Exception;
                            }
                            else
                            {
                                Console.WriteLine($"Wrote to offset: {task.Result.Offset}");
                            }
                        });
                    }
                    catch (ProduceException<string, string> e)
                    {
                        Console.WriteLine($"failed to deliver message: {e.Message} [{e.Error.Code}]");
                    }
                }

                // Since we are producing synchronously, at this point there will be no messages
                // in-flight and no delivery reports waiting to be acknowledged, so there is no
                // need to call producer.Flush before disposing the producer.
            }
        }
    }
}
