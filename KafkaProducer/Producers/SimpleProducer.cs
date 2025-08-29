using Confluent.Kafka;
using Microsoft.Extensions.Configuration;

namespace KafkaProducer.Producers;

public static class SimpleProducer
{
    public static async Task RunAsync(IConfiguration config)
    {
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = config["Kafka:ServerUri"]
        };

        using (var producer = new ProducerBuilder<Null, string>(producerConfig).Build())
        {
            Console.WriteLine("Kafka Producer started.");
            Console.WriteLine("Type a message and press Enter to send to 'simple-events' topic.");
            Console.WriteLine("Type 'exit' to quit.");

            string input;
            while ((input = Console.ReadLine()) != null)
            {
                if (input.Equals("exit", StringComparison.OrdinalIgnoreCase))
                    break;

                try
                {
                    var result = await producer.ProduceAsync(
                        "simple-events",
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
