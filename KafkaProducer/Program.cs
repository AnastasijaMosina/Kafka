using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaProducer_1
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
            };

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                Console.WriteLine("Kafka Producer started.");
                Console.WriteLine("Type a message and press Enter to send to 'quickstart-events' topic.");
                Console.WriteLine("Type 'exit' to quit.");

                string input;
                while ((input = Console.ReadLine()) != null)
                {
                    if (input.Equals("exit", StringComparison.OrdinalIgnoreCase))
                        break;

                    try
                    {
                        var result = await producer.ProduceAsync(
                            "quickstart-events",
                            new Message<Null, string> { Value = input });

                        Console.WriteLine(
                            $"Delivered '{input}' to: {result.TopicPartitionOffset}");
                    }
                    catch (ProduceException<Null, string> e)
                    {
                        Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                    }
                }
            }
        }
    }
}
