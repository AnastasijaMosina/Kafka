using Confluent.Kafka;
using KafkaConnector.Consumers;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaConnector
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Choose consumer type:");
            Console.WriteLine("1. Basic Consumer");
            Console.WriteLine("2. Advanced Consumer");
            Console.WriteLine("3. Order Processing Consumer");

            var choice = Console.ReadLine();

            switch (choice)
            {
                case "1":
                    RunBasicConsumer();
                    break;
                case "2":
                    await RunAdvancedConsumer();
                    break;
                case "3":
                    await RunOrderProcessor();
                    break;
                default:
                    Console.WriteLine("Invalid choice");
                    break;
            }
        }

        static void RunBasicConsumer()
        {
            var config = new Confluent.Kafka.ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "test-consumer-group",
                AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest
            };

            using (var consumer = new Confluent.Kafka.ConsumerBuilder<Ignore, string>(config).Build())
            {
                consumer.Subscribe("simple-events");
                Console.WriteLine("Running basic consumer...");
                Console.WriteLine("Listening to 'quickstart-events' topic on localhost:9092");
                Console.WriteLine("Press Ctrl+C to exit");
                Console.WriteLine();

                var cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true;
                    cts.Cancel();
                };

                try
                {
                    while (!cts.Token.IsCancellationRequested)
                    {
                        try
                        {
                            var cr = consumer.Consume(cts.Token);
                            Console.WriteLine(
                                $"[{cr.Message.Timestamp.UtcDateTime:HH:mm:ss}] Consumed message: '{cr.Message.Value}' | Partition: {cr.Partition}, Offset: {cr.Offset}");
                        }
                        catch (Confluent.Kafka.ConsumeException e)
                        {
                            Console.WriteLine($"Consume error: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Basic consumer stopped.");
                }
                finally
                {
                    consumer.Close();
                }
            }
        }

        static async Task RunAdvancedConsumer()
        {
            var consumer = new AdvancedKafkaConsumer();
            await consumer.StartConsuming("simple-events");
        }

        static async Task RunOrderProcessor()
        {
            var processor = new OrderProcessingConsumer();
            await processor.StartProcessing();
        }
    }
}
