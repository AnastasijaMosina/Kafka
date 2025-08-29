using Confluent.Kafka;
using System;
using System.Threading;

namespace KafkaConnector.Consumers
{
    internal class BasicKafkaConsumer
    {
        public void Run()
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "test-consumer-group",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
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
                        catch (ConsumeException e)
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
    }
}
