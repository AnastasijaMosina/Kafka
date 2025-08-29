using Confluent.Kafka;
using KafkaProducer.Interfaces;
using Newtonsoft.Json;
using System;
using System.Threading.Tasks;

namespace KafkaProducer.Producers
{
    public class SchemaBasedProducer
    {
        private readonly IProducer<string, string> _producer;
        private readonly Random _random = new Random();
        private readonly IRandomGenerator _randomGenerator;

        public SchemaBasedProducer(IRandomGenerator randomGenerator)
        {
            _randomGenerator = randomGenerator;
        }

        public SchemaBasedProducer()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                MessageTimeoutMs = 30000,
                Acks = Acks.All,
                MessageSendMaxRetries = 3,
                EnableIdempotence = true
            };

            _producer = new ProducerBuilder<string, string>(config).Build();
        }

        public async Task ProduceOrderEvents()
        {
            Console.WriteLine("Schema-based producer started. Generating order events...");
            Console.WriteLine("Press any key to stop producing...");

            var producing = true;
            var keyTask = Task.Run(() => { Console.ReadKey(); producing = false; });

            while (producing)
            {
                var orderEvent = _randomGenerator.GenerateRandomOrder();
                var json = JsonConvert.SerializeObject(orderEvent);

                try
                {
                    var result = await _producer.ProduceAsync(
                        "order-events",
                        new Message<string, string>
                        {
                            Key = orderEvent.OrderId.ToString(),
                            Value = json,
                            Headers = new Headers
                            {
                                {"content-type", System.Text.Encoding.UTF8.GetBytes("application/json")},
                                {"schema-version", System.Text.Encoding.UTF8.GetBytes("1.0")}
                            }
                        });

                    Console.WriteLine($"Produced order {orderEvent.OrderId} to partition {result.Partition} at offset {result.Offset}");
                }
                catch (ProduceException<string, string> e)
                {
                    Console.WriteLine($"Failed to produce: {e.Error.Reason}");
                }

                await Task.Delay(2000);
            }

            _producer.Dispose();
        }

    }
}
